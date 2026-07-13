"""Mosquitto $SYS counter probe for ingress ceiling diagnostics."""

from __future__ import annotations

import threading
import time
from typing import Dict, Optional


# Topics published by Mosquitto 2.x with sys_interval > 0.
SYS_TOPICS = (
    "$SYS/broker/publish/messages/dropped",
    "$SYS/broker/publish/messages/sent",
    "$SYS/broker/publish/messages/received",
    "$SYS/broker/messages/received",
    "$SYS/broker/messages/sent",
)


def _parse_int(payload) -> Optional[int]:
    try:
        if isinstance(payload, bytes):
            text = payload.decode("utf-8", errors="replace").strip()
        else:
            text = str(payload).strip()
        # Some $SYS values are floats as strings; take the integer part.
        return int(float(text))
    except (TypeError, ValueError):
        return None


class SysCountersProbe:
    """Background MQTT subscriber that samples Mosquitto $SYS counters.

    Intentionally not cpuset-pinned: affinity would apply to the harness
    process. The probe is a single lightweight Paho connection.
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        client_id: str = "bench-sys-probe",
    ) -> None:
        self.host = host
        self.port = int(port)
        self.client_id = client_id
        self._lock = threading.Lock()
        self._values: Dict[str, int] = {}
        self._client = None
        self._started = False

    def start(self, timeout_s: float = 10.0) -> None:
        if self._started:
            return
        # Optional extra: keep harness importable without paho-mqtt installed.
        try:
            import paho.mqtt.client as mqtt  # noqa: PLC0415
        except ImportError as exc:
            raise RuntimeError("sys probe requires paho-mqtt (pip install mqtt-client-bench[paho])") from exc

        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.client_id,
            protocol=mqtt.MQTTv311,
        )
        ready = threading.Event()

        def on_connect(c, userdata, flags, reason_code, properties=None):
            rc = int(getattr(reason_code, "value", reason_code))
            if rc == 0:
                for topic in SYS_TOPICS:
                    c.subscribe(topic, qos=0)
                ready.set()

        def on_message(c, userdata, msg):
            value = _parse_int(msg.payload)
            if value is None:
                return
            with self._lock:
                self._values[msg.topic] = value

        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(self.host, self.port, keepalive=60)
        client.loop_start()
        self._client = client
        if not ready.wait(timeout=timeout_s):
            self.stop()
            raise TimeoutError("sys probe connect/subscribe timeout")
        # Wait briefly for first $SYS publish (sys_interval is 1s in our conf).
        deadline = time.time() + max(2.0, timeout_s)
        while time.time() < deadline:
            with self._lock:
                if self._values:
                    break
            time.sleep(0.1)
        self._started = True

    def snapshot(self) -> Dict[str, Optional[int]]:
        with self._lock:
            values = dict(self._values)
        return {
            "dropped": values.get("$SYS/broker/publish/messages/dropped"),
            "publish_sent": values.get("$SYS/broker/publish/messages/sent"),
            "publish_received": values.get("$SYS/broker/publish/messages/received"),
            "messages_received": values.get("$SYS/broker/messages/received"),
            "messages_sent": values.get("$SYS/broker/messages/sent"),
            "raw": values,
        }

    def stop(self) -> None:
        client = self._client
        self._client = None
        self._started = False
        if client is None:
            return
        try:
            client.loop_stop()
            client.disconnect()
        except Exception:  # noqa: BLE001
            pass


def sys_counters_delta(before: Optional[dict], after: Optional[dict]) -> dict:
    """Compute end-start deltas for the main $SYS fields."""
    if not before or not after:
        return {
            "dropped_delta": None,
            "publish_sent_delta": None,
            "publish_received_delta": None,
            "messages_received_delta": None,
            "messages_sent_delta": None,
            "before": before,
            "after": after,
        }

    def _delta(key: str) -> Optional[int]:
        a = after.get(key)
        b = before.get(key)
        if a is None or b is None:
            return None
        return int(a) - int(b)

    return {
        "dropped_delta": _delta("dropped"),
        "publish_sent_delta": _delta("publish_sent"),
        "publish_received_delta": _delta("publish_received"),
        "messages_received_delta": _delta("messages_received"),
        "messages_sent_delta": _delta("messages_sent"),
        "before": before,
        "after": after,
    }
