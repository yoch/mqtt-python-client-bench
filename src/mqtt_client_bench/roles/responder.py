"""
Responder worker for application RTT measurements.

Subscribes to request topic and republishes payload to response topic.
"""

from __future__ import annotations

import argparse
import json
import os
import threading
import time

from mqtt_client_bench.adapters.registry import adapter_identity, create_adapter
from mqtt_client_bench.control import barrier_client_session, touch, write_json


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)
    with open(args.config, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)

    client_name = cfg.get("client", "paho")
    client_path = cfg.get("client_path")
    identity = adapter_identity(client_name, client_path)

    request_topic = cfg["request_topic"]
    response_topic = cfg["response_topic"]
    qos = int(cfg.get("qos_subscribe", 1))
    protocol = cfg.get("protocol", "MQTTv311")

    state = {
        "connected": threading.Event(),
        "subscribed": threading.Event(),
        "responses": 0,
        "lock": threading.Lock(),
        "sub_mid": None,
    }

    adapter = create_adapter(
        client_name,
        client_path=client_path,
        client_id=cfg.get("client_id", f"resp-{cfg['run_id']}"),
        protocol=protocol,
        clean_session=True,
        tls_ca_certs=cfg.get("ca_certs") if cfg.get("tls") else None,
    )

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if int(getattr(reason_code, "value", reason_code)) == 0:
            state["connected"].set()

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        ok = True
        for item in reason_code_list:
            if int(getattr(item, "value", item)) >= 128:
                ok = False
        if ok:
            state["subscribed"].set()

    def on_message(client, userdata, msg):
        adapter.publish(response_topic, payload=msg.payload, qos=qos, retain=False)
        with state["lock"]:
            state["responses"] += 1

    adapter.on_connect = on_connect
    adapter.on_subscribe = on_subscribe
    adapter.on_message = on_message
    adapter.connect(cfg["host"], int(cfg["port"]), keepalive=60)
    adapter.loop_start()
    if not state["connected"].wait(30):
        write_json(cfg["result_path"], {"ok": False, "error": "ready_timeout", **identity})
        adapter.loop_stop()
        return 1
    result = adapter.subscribe(request_topic, qos=qos)
    state["sub_mid"] = result.mid
    if result.mid is None:
        state["subscribed"].set()
    if not state["subscribed"].wait(30):
        write_json(cfg["result_path"], {"ok": False, "error": "ready_timeout", **identity})
        adapter.loop_stop()
        return 1

    touch(cfg["ready_path"], {"role": "responder", "pid": os.getpid(), **identity})
    barrier = barrier_client_session(cfg["barrier_path"], timeout_s=float(cfg.get("barrier_timeout_s", 120)))
    barrier.wait("T0")
    # Mirror initiator warmup duration, then join the measure barrier.
    time.sleep(float(cfg.get("warmup_s", 1)))
    barrier.ack("WARMUP_DRAINED")
    barrier.wait("T_MEASURE")
    barrier.close()
    # Stay alive for measure+drain.
    alive = float(cfg.get("duration_s", 3)) + float(cfg.get("drain_s", 2)) + 2
    time.sleep(alive)
    with state["lock"]:
        responses = state["responses"]
    adapter.disconnect()
    adapter.loop_stop()
    write_json(cfg["result_path"], {"ok": True, "role": "responder", "responses": responses, **identity})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
