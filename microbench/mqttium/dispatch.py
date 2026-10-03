"""Per-message cost of selecting ``message_callback_add`` callbacks.

Campaign point: ``sub_filters_fixed`` (1,000 topics ``<base>/<i//10>/<i%10>``,
100 filters ``<base>/<d>/+``, so every message matches exactly one filter).
Only paho and mqttium dispatch natively, so paho is the only comparison.

Two measurements, pure CPU, no broker:

1. ns per message to select the matching callbacks, for 1 to 1,000 filters.
   A trie (paho) should stay flat; a linear scan grows with the filter count.
2. How many filter comparisons mqttium runs per message
   (``TopicMatcher._matches`` calls), which is the scan made visible.

Run with the all-clients environment:
    PYTHONPATH=src .venvs/_all/bin/python microbench/mqttium/dispatch.py
"""

from __future__ import annotations

import argparse
import gc
import time

from mqttium.dispatch.matcher import TopicMatcher
from paho.mqtt.matcher import MQTTMatcher

BASE = "bench/0123456789ab/data"
TOPICS = [f"{BASE}/{i // 10}/{i % 10}" for i in range(1000)]


def build(cls, n_filters: int):
    matcher = cls()
    for d in range(n_filters):
        matcher[f"{BASE}/{d}/+"] = d
    return matcher


def ns_per_message(matcher, rounds: int) -> float:
    iter_match = matcher.iter_match
    topics = TOPICS
    best = float("inf")
    gc.disable()
    try:
        for _ in range(5):
            t0 = time.perf_counter_ns()
            for _ in range(rounds):
                for topic in topics:
                    for _cb in iter_match(topic):
                        pass
            best = min(best, (time.perf_counter_ns() - t0) / (rounds * len(topics)))
    finally:
        gc.enable()
    return best


def comparisons_per_message(n_filters: int) -> float:
    matcher = build(TopicMatcher, n_filters)
    calls = 0
    original = TopicMatcher._matches

    def counting(filter_levels, topic_levels, is_system_topic):
        nonlocal calls
        calls += 1
        return original(filter_levels, topic_levels, is_system_topic)

    TopicMatcher._matches = staticmethod(counting)
    try:
        for topic in TOPICS:
            for _cb in matcher.iter_match(topic):
                pass
    finally:
        TopicMatcher._matches = staticmethod(original)
    return calls / len(TOPICS)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rounds", type=int, default=20)
    parser.add_argument("--filters", default="1,10,100,1000")
    args = parser.parse_args()

    print(f"{'filters':>8} {'paho ns/msg':>12} {'mqttium ns/msg':>15} {'ratio':>6} {'mqttium cmp/msg':>16}")
    for n in (int(x) for x in args.filters.split(",")):
        rounds = max(1, args.rounds * 100 // max(n, 100))
        paho_ns = ns_per_message(build(MQTTMatcher, n), rounds)
        mqttium_ns = ns_per_message(build(TopicMatcher, n), rounds)
        cmp = comparisons_per_message(n)
        print(f"{n:>8} {paho_ns:>12.0f} {mqttium_ns:>15.0f} {mqttium_ns / paho_ns:>6.1f} {cmp:>16.0f}")


if __name__ == "__main__":
    main()
