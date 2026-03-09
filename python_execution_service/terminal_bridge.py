"""Terminal broadcast bridge.

Bridges the synchronous PTY thread in scai_runner to async WebSocket
clients.  Each connected WebSocket subscribes a ``asyncio.Queue`` and
receives raw PTY chunks in real-time.

This is the equivalent of bolt.new's PtySession tap pattern, adapted
for our architecture where scai_runner spawns its own PTY per command.
"""

import asyncio
from threading import Lock
from typing import Set

_subscribers: Set[asyncio.Queue[str]] = set()
_lock = Lock()
_loop: asyncio.AbstractEventLoop | None = None


def set_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Must be called once from the async context (e.g. app startup)."""
    global _loop
    _loop = loop


def subscribe() -> asyncio.Queue[str]:
    """Create a new subscriber queue."""
    q: asyncio.Queue[str] = asyncio.Queue(maxsize=4096)
    with _lock:
        _subscribers.add(q)
    return q


def unsubscribe(q: asyncio.Queue[str]) -> None:
    """Remove a subscriber queue."""
    with _lock:
        _subscribers.discard(q)


def broadcast(data: str) -> None:
    """Push a raw PTY chunk to all connected WebSocket clients.

    Thread-safe — called from the sync PTY thread in scai_runner.
    Uses ``call_soon_threadsafe`` to schedule the put on the event loop.
    """
    if not data:
        return

    with _lock:
        subs = list(_subscribers)

    for q in subs:
        if _loop is not None and _loop.is_running():
            _loop.call_soon_threadsafe(_safe_put, q, data)
        else:
            # Fallback: try direct put (may fail if loop not set)
            try:
                q.put_nowait(data)
            except asyncio.QueueFull:
                pass


def _safe_put(q: asyncio.Queue[str], data: str) -> None:
    try:
        q.put_nowait(data)
    except asyncio.QueueFull:
        # Drop oldest and retry — prevents blocking the PTY
        try:
            q.get_nowait()
        except asyncio.QueueEmpty:
            pass
        try:
            q.put_nowait(data)
        except asyncio.QueueFull:
            pass
