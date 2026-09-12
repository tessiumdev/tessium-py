from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class TessiumError(Exception):
    """A refusal from the service: the request named by ``sub`` did not happen."""

    def __init__(self, frame: Mapping[str, Any], sub: str | None = None) -> None:
        super().__init__(frame.get("message") or frame.get("code", "refused"))
        self.code: str = frame.get("code", "")
        self.feature: str | None = frame.get("feature")
        self.upgrade: dict[str, Any] | None = frame.get("upgrade")
        self.sub: str | None = sub or frame.get("sub")
