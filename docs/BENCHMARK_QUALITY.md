# Benchmark quality contract

A benchmark result is not simply **valid** or **invalid**. The same run can be
trustworthy for a paired A/B comparison and untrustworthy as an absolute
baseline. `mqtt_client_bench.quality` makes that distinction explicit.

## Practical profiles

Day-to-day regression work should be short enough to use routinely. Do not pay
for publication-grade A/A replication on every code change.

- `scripts/run_pairwise_rtt_daily.sh`: the default regression probe. Smoke
  windows, external pacer, two balanced ABBA/BAAB blocks, MQTTv311 at 25% and
  50% of pair-specific `C_common`, no A/A by default.
- `scripts/run_pairwise_rtt_quick.sh`: broader practical sweep. Same mechanics,
  25% and 50% under MQTTv311 and MQTTv5, no A/A by default.
- `scripts/run_pairwise_rtt_campaign.sh` with `PROFILE=standard`: exceptional
  deep/publication validation. It keeps the strict A/A gate and larger
  replication budget.

75% and 90% of closed-loop RTT capacity are useful saturation/backpressure
probes but are not routine latency-ranking points. Run them explicitly when the
question is behavior near saturation.

Re-run A/A when the harness, pacer, runner or measurement methodology changes,
or when a borderline A/B result needs deeper qualification. It is not a tax on
every daily comparison.

## Inspect an A/A control

```bash
python -m mqtt_client_bench.quality results/compare-aa-mqttium.json
```

For machine-readable output:

```bash
python -m mqtt_client_bench.quality --json results/compare-aa-*.json
```

The one-line output reports four independent questions:

- **comparative** — is same-client A/A neutral at the benchmark's 3% published
  resolution, including complementary pair-unit stability and complete blocks?
- **stimulus** — did the external pacer deliver the prescribed token sequence,
  without more than 0.2% catch-up slots?
- **absolute** — are complete four-slot block centers stationary within 3%?
- **usable_for** — which claims the evidence supports.

## Meaning of `usable_for`

`paired_ranking_control`
: Suitable as the A/A control before an interleaved ABBA client ranking.
  Common-mode absolute drift may exist; the complementary estimator must show it
  cancels cleanly.

`paired_version_ab_control`
: Suitable as the A/A control before an interleaved before/after comparison of
  two versions of the same client. This is the preferred method for a deep
  MQTTium release-candidate qualification: compare old and new code in the same
  campaign instead of comparing two absolute baselines measured hours or days
  apart.

`absolute_baseline`
: Suitable for quoting an absolute latency baseline and comparing it with an
  independently measured future baseline. This requires block-center
  stationarity in addition to the paired controls.

`diagnostic`
: The evidence may still explain scheduler, batching, GC, pacing or other
  mechanisms, but must not be promoted into a ranking/baseline claim.

## ABBA reducer semantics

Each complete ABBA/BAAB block contains exactly two observations per arm. The
block ratio is `median(B) / median(A)`, where **median means the conventional
sample median**: for two observations it is their arithmetic mean. It must not
be implemented as nearest-rank p50, because nearest-rank p50 of two values is
just the smaller value and can silently discard the slow regime of one arm.

Per-message latency percentiles remain nearest-rank. This change only fixes the
small-sample reducer used to combine run-level observations.

Complementary ABBA+BAAB blocks are then combined multiplicatively as pair units,
which preserves the existing position-balance design.

## Why catch-up is a stimulus property

An external pacer uses an absolute calendar. A catch-up event means token `n`
was emitted only after the scheduled deadline of token `n+1` had already
passed. The mean offered rate can still be exact while the temporal workload is
compressed into a microburst.

The default catch-up budget is 0.2% of scheduled measure-window tokens. This is
an explicit workload-shape budget, not a latency-derived threshold. It matches
the benchmark's existing 0.2% matched-load admission-miss budget. Token loss,
sequence gaps and phase mismatches remain strict zero-tolerance failures.

## Why absolute stationarity is separate

ABBA is intentionally robust to common drift. Consider two A/A blocks:

```text
block 0: 1.00 1.00 1.00 1.00
block 1: 1.20 1.20 1.20 1.20
```

Every within-block A/B ratio is exactly 1.0, so the paired estimator is neutral.
That does **not** make `1.00` and `1.20` interchangeable absolute baselines.

The absolute diagnostic therefore computes the geometric center of each
complete four-slot block and compares those centers to their log-median with a
log-symmetric distance.

## Runtime anomaly telemetry

The benchmark records runtime state to explain intermittent regimes without
putting extra work in the timed hot path. In addition to CPU time, voluntary
and involuntary context switches and client `stats()` snapshots, RTT workers
record Linux process page-fault deltas (`ru_minflt`, `ru_majflt`) around the
measurement window and a compact process-layout snapshot when available.

Treat these as **correlates/diagnostics**, not validity gates. A future MQTTium
release may expose additional CPython/runtime anomaly counters through its
public `AsyncClient.stats()` surface; those should flow into the existing
library snapshot rather than requiring benchmark-specific MQTTium internals.

Normal system ASLR is the representative configuration. Disabling ASLR with
`setarch -R` is permitted only as an explicit causal experiment and such runs
must remain non-comparable/non-publishable.

## Release-candidate workflow

For a new MQTTium RC, prefer this practical order:

1. Freeze the benchmark SHA, broker, host profile and comparison design.
2. Run a short exact-source old/new interleaved comparison first (`daily` or a
   similarly targeted `version_compare`). Large effects and pathological
   runtime regimes should be visible without a multi-hour campaign.
3. Inspect stimulus quality plus runtime telemetry (`effects`, writer batching,
   process CPU/context switches/page faults/layout and MQTTium `stats()`).
4. If the result is small/borderline, the runner changed, or the harness itself
   changed, qualify A/A and increase replication deliberately.
5. Use `quick` for the broader 25/50% dual-protocol sweep when needed.
6. Reserve `PROFILE=standard` for deep/publication evidence, not routine RC
   iteration.
7. Do not promote a result to an independently comparable absolute baseline
   unless `absolute_baseline` is also present.

For exact-source MQTTium campaigns, `scripts/run_mqttium_campaign.sh` accepts an
exact `MQTTIUM_GIT_SHA` or `MQTTIUM_CLIENT_PATH` and isolates labelled results;
do not silently benchmark a moving branch tip when doing before/after work.

## Retry policy

Retries must be evidence-preserving. A block may be retried only for a
predeclared stimulus/environment failure, never because its latency, ratio or
client result is surprising. Keep every attempt in the artifact and cap retries.
A client-side overload/backpressure result by itself is not a retry reason; if a
pacer temporal failure occurred at the same time, retry because the stimulus was
invalid and retain the original attempt as evidence.

## Historical temporal-trace limitation

The bounded temporal RTT trace is diagnostic only and is not itself used for
p50/p95/p99, run validity, ABBA ratios or ranking verdicts. Before the correction
below, unsampled completions could fill a prefix instead of covering the full
window. Do not infer whole-window dynamics from those historical trace files.
Although the latency reservoir and reducer are separate, the buggy trace's
changing write cost could perturb the workload being observed.

## RTT scheduling and trace corrections (September 2026)

The native RTT initiator now yields once with `asyncio.sleep(0)` after counting
an open-loop offer as missed because its outstanding window is full. This is
not a wait for a reply and does not retry that offer. The absolute calendar,
external token consumption, timeout rules, and quality thresholds are unchanged.
The same guard covers the overdue in-loop path. Warmup must cooperate as well,
without adding its counters to the measured window. Unsaturated publication does
not gain an unconditional scheduling hop.

This prevents a buffered pacer socket and a non-suspending native publish path
from consuming tokens until the end of the phase without servicing readable
reply sockets or deferred writes. A syntactic `await` alone does not ensure
progress of other tasks. Regression tests use real ready sockets without
Mosquitto or syscall tracing and verify bounded admission, reply progress,
cancellation, and exact offer/miss accounting.

The initiator also commits a temporal-trace row only when the send path reserved
that sequence. Previously, unsampled completions filled the first 4096 rows,
usually with missing pacing metadata, instead of covering the full window.
Dropped reservations leave holes; they are not replaced by unsampled messages.
This changes trace completeness and removes a mid-window instrumentation cost
change when that premature prefix filled up.

Both fixes change the automatically computed harness fingerprint. Do not pool
old and new measurements or reinterpret old trace files as full-window samples.
No historical result, percentile estimator, or acceptance threshold is rewritten.
These correctness fixes alone do not establish that the normal RTT latency modes
have disappeared; performance qualification remains a separate experiment.

**Unchanged workload limitation:** the application-RTT send loops currently emit
the 40-byte correlation header, even when the scenario names `telemetry256`.
This patch deliberately preserves that payload to isolate scheduling and sampling.
A later workload-size correction must be qualified separately; historical RTT
results must not be described as having a 256-byte application payload.
