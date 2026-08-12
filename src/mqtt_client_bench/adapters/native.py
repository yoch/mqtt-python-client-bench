"""Native driving of an adapter that already owns the library's coroutines.

The bridged adapters expose their library calls as `aconnect` / `apublish` /
`asubscribe` / `adisconnect`. The sync facade hands those to a bridge thread;
this wrapper awaits them on the role worker's own loop instead. Both paths go
through the *same* coroutine, so there is one place where the library is called
and no way for the two to drift into measuring different things.

What disappears in native mode is the thread boundary, measured at 18.5 us per
message. Being a fixed cost, it did not tax every client equally: it inflated a
25,000 msgs/s client's period by 46% and a 6,000 msgs/s client's by 11%. That is
a harness artefact large enough to reorder a ranking, which is the one thing a
comparative benchmark may not do.
"""

from __future__ import annotations

from typing import Any, Optional

from mqtt_client_bench.adapters.base import AdapterCapabilities, SubscribeResult


class NativeAsyncAdapter:
    """Drives an inner bridged adapter's coroutines on the caller's loop."""

    _INNER: Any = None

    def __init__(self, inner) -> None:
        self._inner = inner

    @classmethod
    def capabilities(cls) -> AdapterCapabilities:
        return cls._INNER.capabilities()

    @classmethod
    def identity(cls) -> dict:
        return cls._INNER.identity()

    @classmethod
    def create(cls, **kwargs) -> "NativeAsyncAdapter":
        return cls(cls._INNER.create(**kwargs))

    # The role worker sets these; the inner adapter is what actually fires them.
    @property
    def on_connect(self):
        return self._inner.on_connect

    @on_connect.setter
    def on_connect(self, value) -> None:
        self._inner.on_connect = value

    @property
    def on_publish(self):
        return self._inner.on_publish

    @on_publish.setter
    def on_publish(self, value) -> None:
        self._inner.on_publish = value

    @property
    def on_message(self):
        return self._inner.on_message

    @on_message.setter
    def on_message(self, value) -> None:
        self._inner.on_message = value

    async def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        await self._inner.aconnect(host, port, keepalive)

    async def disconnect(self) -> None:
        await self._inner.adisconnect()

    async def publish(
        self,
        topic: str,
        payload: Any = None,
        qos: int = 0,
        retain: bool = False,
        properties: Any = None,
    ) -> Optional[int]:
        return await self._inner.apublish(topic, payload, qos, retain, properties)

    def publish_nowait(self, topic, payload=None, qos=0, retain=False, properties=None):
        raise NotImplementedError(
            f"{type(self._inner).__name__} has no synchronous-on-loop publish; "
            "the awaited shape is the one its capabilities declare"
        )

    async def subscribe(self, topic: str, qos: int = 0) -> SubscribeResult:
        return await self._inner.asubscribe(topic, qos)

    def message_callback_add(self, topic: str, callback) -> None:
        self._inner.message_callback_add(topic, callback)

    def build_publish_properties(self, profile: str) -> Any:
        return self._inner.build_publish_properties(profile)


def native_async_for(inner_cls):
    """Bind the native wrapper to one library's adapter class."""
    bound = type(f"Native{inner_cls.__name__}", (NativeAsyncAdapter,), {"_INNER": inner_cls})
    return bound
