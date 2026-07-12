"""Shared helpers for asyncio-based MQTT adapters (thread + event loop bridge)."""

from __future__ import annotations

import asyncio
import threading
from typing import Any, Callable, Coroutine, Optional, TypeVar

from mqtt_client_bench.adapters.base import AdapterNotImplemented

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
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=timeout)

    def create_task(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Future:
        if self._loop is None:
            raise RuntimeError("asyncio bridge is not running")
        return asyncio.run_coroutine_threadsafe(coro, self._loop)


class AsyncAdapterStub:
    """Base stub for async clients — capabilities documented, methods raise until wired."""

    MQTT_ERR_SUCCESS = 0
    _NAME = "async-stub"
    _NOTES = ""
    _UNIMPLEMENTED = ["full_adapter"]

    def __init__(self) -> None:
        self.on_connect: Optional[Callable[..., Any]] = None
        self.on_publish: Optional[Callable[..., Any]] = None
        self.on_message: Optional[Callable[..., Any]] = None
        self.on_subscribe: Optional[Callable[..., Any]] = None
        self._bridge = AsyncioBridge()

    @classmethod
    def identity(cls) -> dict:
        return {
            "client": cls._NAME,
            "adapter": cls._NAME,
            "client_module": None,
            "client_version": None,
            "status": "stub",
        }

    def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        raise AdapterNotImplemented(f"{self._NAME}: connect not implemented yet")

    def disconnect(self) -> None:
        raise AdapterNotImplemented(f"{self._NAME}: disconnect not implemented yet")

    def loop_start(self) -> None:
        self._bridge.start()

    def loop_stop(self) -> None:
        self._bridge.stop()

    def publish(self, topic: str, payload=None, qos: int = 0, retain: bool = False, properties=None):
        raise AdapterNotImplemented(f"{self._NAME}: publish not implemented yet")

    def subscribe(self, topic: str, qos: int = 0):
        raise AdapterNotImplemented(f"{self._NAME}: subscribe not implemented yet")

    def message_callback_add(self, topic: str, callback) -> None:
        raise AdapterNotImplemented(f"{self._NAME}: message_callback_add not implemented yet")

    def build_publish_properties(self, profile: str):
        return None
