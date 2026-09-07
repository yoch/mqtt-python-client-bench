# Benchmark quality contract

A benchmark result is not simply **valid** or **invalid**. The same run can be
trustworthy for a paired A/B comparison and untrustworthy as an absolute
baseline. `mqtt_client_bench.quality` makes that distinction explicit.

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
  two versions of the same client. This is the preferred method for an MQTTium
  release candidate: compare old and new code in the same campaign instead of
  comparing two absolute baselines measured hours or days apart.

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

## Release-candidate workflow

For a new MQTTium RC, prefer this order:

1. Freeze one benchmark SHA, broker, host profile and absolute target rate.
2. Qualify external-pacer A/A at the relevant load point.
3. If the A/A evidence is usable for `paired_version_ab_control`, run old and
   new MQTTium versions **interleaved in one ABBA campaign**.
4. Report the relative A/B result together with stimulus quality, pair-unit
   stability and absolute block-center drift.
5. Use runtime telemetry (`effects`, writer batching, process CPU/context
   switches, temporal trace) to estimate what remains to optimize.
6. Do not promote a result to an independently comparable absolute baseline
   unless `absolute_baseline` is also present.

## Retry policy

Retries must be evidence-preserving. A block may be retried only for a
predeclared stimulus/environment failure, never because its latency, ratio or
client result is surprising. Keep every attempt in the artifact and cap retries.
A client-side overload/backpressure result by itself is not a retry reason; if a
pacer temporal failure occurred at the same time, retry because the stimulus was
invalid and retain the original attempt as evidence.

## Known diagnostic limitation

The bounded temporal RTT trace is **diagnostic only** and is not used for the
published p50/p95/p99, run validity, ABBA ratios or ranking verdict. A discovered
sampling bug can cause the retained trace to over-represent the beginning of a
run instead of covering the full measure window at the advertised stride. Until
that sampler is corrected, use the trace for qualitative inspection only and do
not infer the timing of a mid-run regime switch from its saved sequence range.

This limitation does not affect the main latency reservoir or the paired
comparison reducer, so it is intentionally not a blocker for normal benchmark
campaigns.
