from __future__ import annotations

import asyncio
import contextlib
import json
import random
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import websockets

from .errors import TessiumError
from .protocol import ENDPOINT, TERMINAL, endpoint

_BACKOFF_MIN = 0.25
_BACKOFF_MAX = 10.0
_DEFAULT_MAX_QUEUE = 10_000


@dataclass
class _Subscription:
    stream: str
    params: dict[str, Any] | None
    cursor: str | None = None
    resumable: bool = True


@dataclass
class _State:
    subs: dict[str, _Subscription] = field(default_factory=dict)
    pending: dict[int, str] = field(default_factory=dict)


class Client:
    """One socket, as many subscriptions as the plan allows.

    Events are yielded whole because ``cursor`` lives on the frame and a resume is
    made of it. The client remembers the last cursor of every subscription, so a
    dropped socket costs nothing the replay window still covers.
    """

    def __init__(
        self,
        api_key: str,
        *,
        base: str = ENDPOINT,
        reconnect: bool = True,
        max_queue: int = _DEFAULT_MAX_QUEUE,
        on_notice: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self._url = endpoint(api_key, base)
        self._reconnect = reconnect
        self._max_queue = max_queue
        self._on_notice = on_notice
        self._state = _State()
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._failure: BaseException | None = None
        self._closed = asyncio.Event()
        self._ws: Any = None
        self._next_id = 1
        self._overflowed = False
        self._task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> Client:
        self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    def subscribe(
        self,
        stream: str,
        params: Mapping[str, Any] | None = None,
        *,
        sub: str | None = None,
    ) -> str:
        """Open a subscription and return its name, which every one of its events carries."""
        name = sub or f"{stream}_{self._next_id}"
        if name in self._state.subs:
            raise ValueError(f"tessium: subscription {name!r} already exists")
        entry = _Subscription(stream=stream, params=dict(params) if params else None)
        self._state.subs[name] = entry
        if self._ws is not None:
            asyncio.create_task(self._send_subscribe(name, entry))
        self.start()
        return name

    def unsubscribe(self, sub: str) -> None:
        if self._state.subs.pop(sub, None) is None:
            return
        if self._ws is not None:
            frame = {"op": "unsubscribe", "sub": sub, "id": self._next_id}
            self._next_id += 1
            asyncio.create_task(self._send(frame))

    async def close(self) -> None:
        self._closed.set()
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        self.start()
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[dict[str, Any]]:
        getter: asyncio.Future[dict[str, Any]] | None = None
        try:
            while True:
                if not self._queue.empty():
                    yield self._queue.get_nowait()
                    continue
                if self._failure is not None:
                    raise self._failure
                if self._closed.is_set():
                    return
                getter = asyncio.ensure_future(self._queue.get())
                stop = asyncio.ensure_future(self._closed.wait())
                done, _ = await asyncio.wait({getter, stop}, return_when=asyncio.FIRST_COMPLETED)
                stop.cancel()
                if getter in done:
                    yield getter.result()
                else:
                    getter.cancel()
                getter = None
        finally:
            if getter is not None:
                getter.cancel()

    async def _send(self, frame: Mapping[str, Any]) -> None:
        ws = self._ws
        if ws is None:
            return
        with contextlib.suppress(Exception):
            await ws.send(json.dumps(dict(frame)))

    async def _send_subscribe(self, name: str, entry: _Subscription) -> None:
        frame: dict[str, Any] = {
            "op": "subscribe",
            "stream": entry.stream,
            "sub": name,
            "id": self._next_id,
        }
        self._state.pending[self._next_id] = name
        self._next_id += 1
        if entry.params:
            frame["params"] = entry.params
        if entry.cursor and entry.resumable:
            frame["cursor"] = entry.cursor
        await self._send(frame)

    def _push(self, frame: dict[str, Any]) -> None:
        if self._queue.qsize() >= self._max_queue:
            with contextlib.suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
            if not self._overflowed:
                self._overflowed = True
                self._notify(
                    {
                        "op": "notice",
                        "code": "client_queue_full",
                        "message": "events arrive faster than this program reads them; "
                        "the oldest are being dropped",
                    }
                )
        self._queue.put_nowait(frame)

    def _notify(self, frame: dict[str, Any]) -> None:
        if self._on_notice is not None:
            self._on_notice(frame)

    def _fail(self, error: BaseException) -> None:
        if self._failure is None:
            self._failure = error
        self._closed.set()

    async def _run(self) -> None:
        attempt = 0
        while not self._closed.is_set():
            try:
                async with websockets.connect(self._url) as ws:
                    self._ws = ws
                    attempt = 0
                    self._state.pending.clear()
                    for name, entry in list(self._state.subs.items()):
                        await self._send_subscribe(name, entry)
                    async for message in ws:
                        self._receive(json.loads(message))
                        if self._closed.is_set():
                            return
            except asyncio.CancelledError:
                raise
            except (OSError, websockets.WebSocketException):
                # A socket that ends is the ordinary case, not a failure: the
                # service drains its sessions on every deploy, and says so first.
                pass
            except Exception as exc:
                # Anything else is a defect in here, and a defect that retries
                # forever in silence is the worst shape it could take.
                self._fail(exc)
                return
            finally:
                self._ws = None

            if self._closed.is_set() or not self._reconnect:
                self._closed.set()
                return
            # Full jitter, because every client of a service that drains its
            # sessions would otherwise come back in the same instant.
            ceiling = min(_BACKOFF_MAX, _BACKOFF_MIN * 2**attempt)
            attempt += 1
            await asyncio.sleep(random.uniform(0, ceiling))

    def _receive(self, frame: dict[str, Any]) -> None:
        op = frame.get("op")
        if op == "event":
            entry = self._state.subs.get(frame.get("sub", ""))
            if entry is not None:
                entry.cursor = frame.get("cursor")
            self._push(frame)
            return
        if op == "ack":
            self._state.pending.pop(frame.get("id", -1), None)
            return
        if op == "notice":
            if frame.get("code") in TERMINAL:
                self._fail(TessiumError(frame))
            else:
                self._notify(frame)
            return
        if op == "error":
            name = self._state.pending.pop(frame.get("id", -1), None)
            if frame.get("code") in TERMINAL:
                self._fail(TessiumError(frame, name))
                return
            # Replay is a paid capability, and losing it is not worth losing the
            # stream: the subscription opens again live instead of being abandoned.
            if frame.get("feature") == "cursor" and name is not None:
                entry = self._state.subs.get(name)
                if entry is not None:
                    entry.resumable = False
                    entry.cursor = None
                    asyncio.create_task(self._send_subscribe(name, entry))
                    return
            self._fail(TessiumError(frame, name))


def connect(api_key: str, **options: Any) -> Client:
    return Client(api_key, **options)


async def events(
    api_key: str,
    stream: str,
    params: Mapping[str, Any] | None = None,
    **options: Any,
) -> AsyncIterator[dict[str, Any]]:
    """Yield ``event`` frames for a single subscription — the shortest thing that works.

    Reach for :func:`connect` as soon as you want a second stream: one connection
    carries all eight, while every call here spends a connection of its own.
    """
    client = Client(api_key, **options)
    client.subscribe(stream, params)
    try:
        async for frame in client:
            yield frame
    finally:
        await client.close()
