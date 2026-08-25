"""Shared runtime pointer for private fixed-route workflow relay handlers."""

from __future__ import annotations

import asyncio
import threading
import weakref
from typing import Any, Callable, Coroutine

_lock = threading.RLock()
_active: weakref.ReferenceType | None = None


def set_active(adapter: Any) -> None:
    global _active
    with _lock:
        _active = weakref.ref(adapter)


def clear_active(adapter: Any) -> None:
    global _active
    with _lock:
        if _active is not None and _active() is adapter:
            _active = None


def get_active() -> Any | None:
    with _lock:
        return _active() if _active is not None else None


async def call_active(factory: Callable[[Any], Coroutine[Any, Any, Any]]) -> Any:
    """Run an adapter coroutine on the gateway loop from the workflow relay."""
    adapter = get_active()
    if adapter is None or not getattr(adapter, "is_connected", False):
        raise ConnectionError("Glasses are not connected")
    gateway_loop = getattr(adapter, "gateway_loop", None)
    if gateway_loop is None or gateway_loop.is_closed():
        raise ConnectionError("G2 gateway loop is unavailable")
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None
    if current_loop is gateway_loop:
        return await factory(adapter)
    concurrent_future = asyncio.run_coroutine_threadsafe(factory(adapter), gateway_loop)
    return await asyncio.wrap_future(concurrent_future)
