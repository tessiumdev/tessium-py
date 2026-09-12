from __future__ import annotations

from collections.abc import Mapping
from typing import Any

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

# Refusals no reconnection can fix. Everything else is worth another socket.
TERMINAL = frozenset({"unauthorized", "key_revoked", "account_banned", "account_deleted"})


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
