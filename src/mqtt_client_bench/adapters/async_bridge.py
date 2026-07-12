"""Shared helpers for asyncio-based MQTT adapters (thread + event loop bridge)."""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, List, Optional, TypeVar

T = TypeVar("T")


class AsyncioBridge:
    """Run coroutines on a dedicated asyncio loop from sync role workers."""

    def __init__(self) -> None:
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def running(self) -> bool:
        return self._loop is not None and self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.running:
            return
        ready = threading.Event()
        loop_holder: dict[str, asyncio.AbstractEventLoop] = {}

        def _run() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop_holder["loop"] = loop
            ready.set()
            loop.run_forever()
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

        self._thread = threading.Thread(target=_run, name="mqtt-bench-asyncio", daemon=True)
        self._thread.start()
        if not ready.wait(timeout=5):
            raise RuntimeError("asyncio bridge failed to start")
        self._loop = loop_holder["loop"]

    def stop(self) -> None:
        loop = self._loop
        thread = self._thread
        if loop is None or thread is None:
            return
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=10)
        self._loop = None
        self._thread = None

    def run(self, coro: Coroutine[Any, Any, T], timeout: Optional[float] = 30.0) -> T:
        if self._loop is None:
            raise RuntimeError("asyncio bridge is not running")
        if threading.current_thread() is self._thread:
            raise RuntimeError(
                "AsyncioBridge.run() called from the bridge loop thread; "
                "schedule work with create_task() instead to avoid deadlock"
            )
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=timeout)

    def on_loop_thread(self) -> bool:
        return self._thread is not None and threading.current_thread() is self._thread

    def create_task(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Future:
        if self._loop is None:
            raise RuntimeError("asyncio bridge is not running")
        return asyncio.run_coroutine_threadsafe(coro, self._loop)


def topic_matches_sub(sub: str, topic: str) -> bool:
    """Return True if MQTT filter ``sub`` matches ``topic`` (+ / # wildcards)."""
    if sub == "#":
        return True
    sub_levels = sub.split("/")
    topic_levels = topic.split("/")
    for i, level in enumerate(sub_levels):
        if level == "#":
            return i == len(sub_levels) - 1
        if i >= len(topic_levels):
            return False
        if level == "+":
            continue
        if level != topic_levels[i]:
            return False
    return len(sub_levels) == len(topic_levels)


@dataclass
class IncomingMessage:
    """Minimal message object expected by role workers (``msg.payload`` / ``msg.topic``)."""

    topic: str
    payload: Any
    qos: int = 0
    retain: bool = False


class BridgedAdapterBase:
    """Sync facade base for asyncio MQTT clients driven via ``AsyncioBridge``."""

    MQTT_ERR_SUCCESS = 0
    _NAME = "bridged"
    _NOTES = ""

    def __init__(self) -> None:
        self.on_connect: Optional[Callable[..., Any]] = None
        self.on_publish: Optional[Callable[..., Any]] = None
        self.on_message: Optional[Callable[..., Any]] = None
        self.on_subscribe: Optional[Callable[..., Any]] = None
        self._bridge = AsyncioBridge()
        self._topic_callbacks: Dict[str, Callable[..., Any]] = {}
        self._mid_lock = threading.Lock()
        self._next_mid = 1
        self._userdata: Any = None
        self._pump_task: Optional[asyncio.Future] = None
        self._stopping = False
        self._connected = False

    def _ensure_bridge(self) -> None:
        if not self._bridge.running:
            self._bridge.start()

    def loop_start(self) -> None:
        self._ensure_bridge()

    def loop_stop(self) -> None:
        self._bridge.stop()

    def _start_pump(self) -> None:
        """Schedule ``_message_pump`` on the running bridge loop (call from async connect)."""
        self._pump_task = asyncio.ensure_future(self._message_pump())

    async def _stop_pump(self) -> None:
        self._stopping = True
        pump = self._pump_task
        self._pump_task = None
        if pump is None or pump.done():
            return
        pump.cancel()
        try:
            await pump
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass

    async def _message_pump(self) -> None:
        raise NotImplementedError(f"{self._NAME}: _message_pump not implemented")

    def alloc_mid(self) -> int:
        with self._mid_lock:
            mid = self._next_mid
            self._next_mid = 1 if self._next_mid >= 65535 else self._next_mid + 1
            return mid

    def message_callback_add(self, topic: str, callback: Callable[..., Any]) -> None:
        self._topic_callbacks[topic] = callback

    def build_publish_properties(self, profile: str) -> Any:
        return None

    def _fire_on_connect(
        self,
        flags: Any = None,
        reason_code: Any = 0,
        properties: Any = None,
    ) -> None:
        cb = self.on_connect
        if cb is None:
            return
        if flags is None:
            flags = {}
        cb(self, self._userdata, flags, reason_code, properties)

    def _fire_on_publish(self, mid: int, reason_code: Any = 0, properties: Any = None) -> None:
        cb = self.on_publish
        if cb is None:
            return
        cb(self, self._userdata, mid, reason_code, properties)

    def _fire_on_subscribe(
        self,
        mid: int,
        reason_code_list: List[Any],
        properties: Any = None,
    ) -> None:
        cb = self.on_subscribe
        if cb is None:
            return
        cb(self, self._userdata, mid, reason_code_list, properties)

    def _dispatch_message(self, msg: IncomingMessage) -> None:
        matched = False
        for filt, callback in list(self._topic_callbacks.items()):
            if topic_matches_sub(filt, msg.topic):
                matched = True
                callback(self, self._userdata, msg)
        if not matched and self.on_message is not None:
            self.on_message(self, self._userdata, msg)
