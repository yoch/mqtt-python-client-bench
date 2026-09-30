# Not measured yet

Questions the v2 catalogue does not answer. Each would need a design that
keeps the invariants in [CLAUDE.md](CLAUDE.md): counts confirmed by a party
that shares no code with the client, resources read from outside, identical
absolute offers, refusals instead of approximations.

## Robustness

- **Reconnect.** Time from a broker restart or a dropped connection to the
  first delivered message, and what was lost or duplicated meanwhile. Needs a
  controlled outage (container pause, or a TCP proxy that cuts the socket)
  and a C peer that numbers messages so gaps and duplicates are countable.
  `AdapterCapabilities.reconnect` already exists for refusals.
- **Session resume.** A persistent session (`clean_session=False` /
  `clean_start=False` with an expiry): disconnect, let the C source publish
  QoS 1 and 2 messages, reconnect, and count what the client receives. The
  broker's queued-message counters confirm what it held.
- **Ordering.** Per-topic order at QoS 1 and 2, including across a reconnect.
  The C peer would check a sequence number in the payload after the stamp.
- **Retained messages.** Subscribe-time delivery of many retained topics:
  time to receive them all and the memory it takes.
- **Bursts.** A fixed average rate delivered in bursts (for example 10,000
  messages every 5 s): the backlog each client builds, how long it takes to
  drain it, and its peak RSS.

## Environment variants

- **uvloop.** The asyncio clients on uvloop instead of the default loop, as a
  separate client variant (its own environment and identity), never mixed
  into the default ranking.
- **Python versions.** The same campaign on several CPython versions,
  including the free-threaded build, where the sync clients' network threads
  could run in parallel. Each version is its own campaign; the report would
  compare campaigns.
- **Real network.** Client and broker on different hosts, or loopback with
  `tc netem` delay and loss. Latency then includes the network, so the report
  would need to show it apart from the loopback results.
