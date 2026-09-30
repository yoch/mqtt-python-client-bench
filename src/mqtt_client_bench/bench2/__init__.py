"""Benchmark core v2: fixed work, counted by a neutral peer and the broker.

Every published number is either a count that a second party confirms (the C
peer on the other side of the broker, and the broker's own ``$SYS`` counters)
or a resource the orchestrator reads from ``/proc`` outside the client's
process. The client's process runs the adapter and a handful of integer
counters, nothing else.
"""
