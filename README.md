# tessium

Minimal Python client for [Tessium](https://tessium.dev) — a realtime Solana
data API. Open one WebSocket, subscribe to the streams you need, and receive
on-chain activity as structured events instead of raw RPC payloads you have to
decode yourself.

Eight streams over a single connection: `launches`, `migrations`,
`pool_creations`, `token_trades`, `token_transfers`, `candles`, `wallet_trades`,
`wallet_transfers`.

## Install

```bash
pip install tessium
```

## Watch new token launches

Every new token on pump.fun, printed as it happens:

```python
import asyncio
from tessium import events

async def main():
    async for frame in events(
        "YOUR_API_KEY",
        "launches",
        {"platforms": ["pumpfun"]},
    ):
        launch = frame["data"]
        print(launch["symbol"], launch["mint"])

asyncio.run(main())
```

An [API key](https://tessium.dev/dashboard) on the
[free plan](https://tessium.dev/pricing) needs no payment details.

## Several streams, one connection

`events()` spends a connection per stream, and a plan sells only a few. Reach for
`connect()` the moment you want a second one — every event names the subscription
it came from:

```python
import asyncio
from tessium import connect

WSOL = "So11111111111111111111111111111111111111112"

async def main():
    async with connect("YOUR_API_KEY") as tessium:
        tessium.subscribe("launches", {"platforms": ["pumpfun"]}, sub="new")
        tessium.subscribe("token_trades", {"mint": WSOL}, sub="sol")

        async for frame in tessium:
            data = frame["data"]
            if frame["sub"] == "new":
                print("launch", data["symbol"])
            else:
                print("trade", data["tradeType"], data["valueUsd"])

asyncio.run(main())
```

## Disconnects are handled for you

The socket will end — networks drop, and the service drains its sessions on every
deploy. The client reopens it with a jittered backoff and resubscribes **from the
last cursor it saw**, so the gap is filled by the replay window rather than lost.
Nothing is required of you.

Two details worth knowing:

- Replay is a paid capability. On the free plan the server refuses the cursor, and
  the client resubscribes live instead of giving up — you keep the stream, you just
  lose the events from the seconds you were away.
- Refusals raise. A wrong key, a stream the plan does not carry, `detail="full"`
  without it — each raises a `TessiumError` carrying `code`, `feature` and the
  cheapest `upgrade` that lifts it. Nothing fails quietly.

```python
from tessium import TessiumError, connect

async def main():
    tessium = connect(
        "YOUR_API_KEY",
        on_notice=lambda n: print("notice:", n["code"], n.get("message", "")),
    )
    tessium.subscribe("token_trades", {"mint": WSOL})
    try:
        async for frame in tessium:
            handle(frame)
    except TessiumError as err:
        print(err.code, err.feature, err.upgrade)
    finally:
        await tessium.close()
```

`on_notice` is where the server's own remarks arrive: `server_restart` before a
handover, `gap` when a resume fell outside the window, `dropped` when a reader is
too slow to keep up.

Pass `reconnect=False` for a single socket that ends when it ends.

## Also here

```python
from tessium import ENDPOINT, STREAMS, endpoint, subscribe_frame
```

`endpoint()` builds the URL, `subscribe_frame()` builds the subscribe frame —
useful when you drive the socket yourself.

## More

- [Documentation](https://tessium.dev/docs/) — protocol frames, cursors and
  replay, per-stream payloads, limits
- [Runnable examples](https://github.com/tessiumdev/tessium-example) — launch to
  trades, early volume filter, Telegram alerts, in Node and Python
- [Coverage](https://tessium.dev/coverage) — every launchpad, AMM and router
  Tessium reads

## License

MIT
