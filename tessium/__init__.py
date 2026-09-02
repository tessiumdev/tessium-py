from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping
from typing import Any

import websockets

__version__ = "0.0.1"

ENDPOINT = "wss://api.tessium.dev/stream"

STREAMS = (
    "launches",
    "migrations",
    "pool_creations",
    "token_trades",
    "token_transfers",
    "candles",
    "wallet_trades",
    "wallet_transfers",
)


def endpoint(api_key: str, base: str = ENDPOINT) -> str:
    return f"{base}?key={api_key}"


def subscribe_frame(
    stream: str,
    params: Mapping[str, Any] | None = None,
    sub_id: int = 1,
) -> dict[str, Any]:
    frame: dict[str, Any] = {"op": "subscribe", "stream": stream, "id": sub_id}
    if params:
        frame["params"] = dict(params)
    return frame


async def events(
    api_key: str,
    stream: str,
    params: Mapping[str, Any] | None = None,
    *,
    base: str = ENDPOINT,
) -> AsyncIterator[dict[str, Any]]:
    """Yield `event` frames for one subscription.

    Frames are yielded whole, not just `data`: `cursor` is what you persist to
    replay a short disconnect, and it lives on the frame.
    """
    async with websockets.connect(endpoint(api_key, base)) as ws:
        await ws.send(json.dumps(subscribe_frame(stream, params)))
        async for message in ws:
            frame = json.loads(message)
            if frame.get("op") == "event":
                yield frame
