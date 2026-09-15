# Direct `rtt_capacity_qos1` comparison: 8e29cfa vs c4f477 vs RC14

Same-window closed-loop ABBA on the PR #39 runner (`cursor` / `bce5e86fefabd33d`).
Workload is the matrix point: QoS1/QoS1, `telemetry256`, `outstanding=inflight=32`,
`standard` timings (3 s warmup / 12 s measure / 6 s drain), layout initiator=CPU0,
broker=CPU1, responder=CPU3 (`orch`). Fresh processes every slot.

MQTTium was not modified. The driver calls `harness.run_point` (capacity cadence).
It does **not** use `version_compare` (that command freezes `target_rate`).

| Comparison | v311 | v5 | control quality | conclusion |
| ---------- | ---: | --: | --------------- | ---------- |
| c4 / 8e29 | **0.999** | **1.039** | B/B clean (CV 0.8–1.2 %) | no regression from the two recent commits |
| c4 / RC14 | **1.125** | **1.154** | B/B clean; v311 C/B CV 0.8 % (stable +12 %); v5 noisier | no RTT debt vs RC14; c4 is faster |

**Verdict:** none of the three hypotheses that needed a library fix is supported.
The matrix #39 drop (c4 ~24 k vs 8e29cfa ~31 k) was **primarily environmental**.

---

## Q1 / Q2 / Q3

### Q1 — did `22c56d5` + `bef3457` degrade `rtt_capacity_qos1`?

**No.**

Those two commits are the only runtime delta A→B. MQTTv311 is the negative
control (neither commit touches QoS1 v311). Observed `median(B/A) = 0.999`
(range 0.984–1.012, 8/8 valid on each arm, CV 1.5–1.9 %). A >5 % v311
slowdown with a clean B/B would have been surprising; it is not there.

v5 `median(B/A) = 1.039` (range 0.983–1.053). Direction is *faster*, consistent
with the empty MQTT 5 property singleton, but the 3.9 % sit just outside the
±3 % gate and v5 CV is 1.7–2.7 %. Treat it as “not a regression”, not as a
claimed win.

### Q2 — is HEAD `c4f477` slower than RC14 in the same window?

**No. It is faster.**

v311: `median(B/C) = 1.125` (range 1.121–1.130, CV 0.8 % both arms, 8/8 valid).
All four blocks agree. The heuristic `control_quality=noisy` here only means
“|ratio−1| > 3 %”; it is not scatter. That is a real +12 % for the lean
rewrite vs RC14 on this runner, not a debt.

v5: `median(B/C) = 1.154` (range 1.053–1.225). Still B/C ≥ 0.95 on every
completed block. This comparison is noisier (CV ~8–10 %, one `host_busy_at_start:4.1`
on the last B slot) and overlapped campaign wrap-up I/O; do not over-read the
exact percentage. Direction is still c4 ≥ RC14.

Stop rule `B/C >= 0.95` is met. No profiling phase.

### Q3 — how much of the matrix #39 drop is the runner?

**Most of it.**

| Source | c4 v311 | c4 v5 | 8e29cfa v311 | 8e29cfa v5 |
| --- | ---: | ---: | ---: | ---: |
| Matrix #39 (hours earlier, interleaved with gmqtt) | 24 626 | 23 318 | 31 042 | 29 417 |
| This ABBA (same afternoon, mqttium-only) | 30.6–31.1 k | 29.9–29.9 k | 30 601 | 28 696 |

c4 in this window matches the *8e29cfa matrix*, not the 24 k c4 matrix.
Ratio 24.6 k / 31.0 k ≈ 0.79. gmqtt also collapsed in that matrix
(`broker_headroom_low` on v311). B/B here is CV < 1.2 %, so the runner *can*
be quiet; the matrix RTT points were taken in a worse window.

---

## 1. B/B control

Two independent `--target` installs of `c4f477` (`inst-B1`, `inst-B2`), same
tree SHA `69a64bd124875782c4c443b683f12d370c2cb2bf`. Order ABBA/BAAB × 4
blocks = 8 observations per arm per protocol. All 32 slots valid.

| Protocol | median B2/B1 | CV B1 | CV B2 | median msgs/s B1 / B2 |
| --- | ---: | ---: | ---: | ---: |
| v311 | 1.004 (0.991–1.014) | 0.76 % | 1.15 % | 30 956 / 31 129 |
| v5 | 1.008 (0.990–1.017) | 0.86 % | 0.85 % | 29 842 / 29 982 |

Gate `|ratio−1| > 3 %` does **not** fire. CV is far below a few-percent effect.
A/B and C/B ratios may be read as code, not as time-of-day drift *inside this
session*.

The CPU2/orch diagnostic was **not** run: B/B was already clean.

---

## 2. A ↔ B (`8e29cfa` vs `c4f477`)

| Protocol | median B/A | CV A | CV B | median A / B | valid |
| --- | ---: | ---: | ---: | ---: | ---: |
| v311 | **0.999** | 1.85 % | 1.48 % | 30 601 / 30 663 | 8 / 8 |
| v5 | **1.039** | 1.66 % | 2.67 % | 28 696 / 29 872 | 8 / 8 |

v311 block ratios: 0.984, 1.002, 0.996, 1.012 (ABBA/BAAB). Negative control holds.

v5 is allowed to be ≥ 1.00. It is, weakly. One BAAB block at 1.053 is the
high end; one ABBA at 0.983. No block shows B slower by 5 %.

---

## 3. C ↔ B (RC14 `c194597` vs `c4f477`)

| Protocol | median B/C | CV C | CV B | median C / B | valid |
| --- | ---: | ---: | ---: | ---: | ---: |
| v311 | **1.125** | 0.82 % | 0.79 % | 27 456 / 30 967 | 8 / 8 |
| v5 | **1.154** | 9.64 % | 8.28 % | 22 486 / 27 384 | 8 / 7 |

v311 is the clean one: every block 1.121–1.130. Constructor vocabularies
differ (`rc14` vs `frozen`); that is the lean rewrite, not the two later
commits. RC14 is slower here, so there is **no** RTT QoS1 debt to recover.

v5 C/B later slots show elevated `ru_nivcsw` (thousands vs ~100 on quiet
slots) and one discarded `host_busy_at_start:4.1` (loadavg 4.14 at T0,
kept, not retried). That slot coincided with wrapping PR #39 (archive of
~250 MB `application_rtt` JSON). Favourable and unfavourable valid rates are
both kept. Even the quietest complete block is still B/C = 1.053.

---

## 4. Runner role

- B/B proves the box *can* hold ~31 k v311 / ~30 k v5 with CV ≈ 1 %.
- Matrix #39 RTT for the same binary was ~21 % lower, and gmqtt died of
  `broker_headroom_low` in that same campaign.
- `scaling_governor` is unreadable (`frequency_policy: unpinned`); that is
  declared on the host profile. It does not by itself explain a 21 % hole
  that appears in one campaign and not in a later ABBA.
- Layout (responder on `orch` CPU3) was **not** the dominant noise source in
  this session (B/B clean). A CPU2 diagnostic would be `non_comparable` and
  was not needed.

---

## 5. Profiling

Not run. Trigger was `B/C < 0.95` with a clean B/B. Observed B/C is 1.12–1.15.

---

## 6. Identities (accepted only with demonstrated sources)

| Arm | requested SHA | checked out | `src/mqttium` tree | import path | ctor vocab |
| --- | --- | --- | --- | --- | --- |
| A | `8e29cfa27dcd…` | match | `3116e692dca0…` | `…/inst-A/mqttium/__init__.py` | frozen |
| B1 / B2 | `c4f477dbc13f…` | match | `69a64bd12487…` | `…/inst-B1` and `…/inst-B2` | frozen |
| C | `c194597bcf5a…` | match | `9f85f15776f5…` | `…/inst-C/mqttium/__init__.py` | rc14 |

All report `1.0.0rc14`. B1 and B2 share the tree SHA and differ only by install
directory (independent processes).

- Harness fingerprint: `e9c7baeeaf7c5ebc` on every document.
- Reference SHA `e05b5c47a56e7f4d3a0e2131361e80cb01c61553` at identity and B/B.
  Later C/B documents record `HEAD=add3475` because campaign result JSON was
  committed on the sibling branch during the last comparison. That commit
  does not touch `src/mqtt_client_bench/` (fingerprint unchanged).
- Broker: `eclipse-mosquitto:2.1.2-alpine@sha256:6f8d8a947c506f8a2290ec65cd4bd2bc7cb4d43fb5f6271f861cb013e2ef9797`.
- Python 3.12.3. Hostname `cursor`.
- Point contract: `qos_publish=1`, `qos_subscribe=1`, `payload=telemetry256`,
  `outstanding=32`, `inflight=32`, `cadence=capacity`, `network=localhost`.

---

## Limitations

- One host, `clock_unpinned`, four vCPUs. Numbers are not a published ranking.
- v5 C/B is partly contended; the +15 % should not be cited as a precise
  library delta. v311 C/B (+12 %, CV 0.8 %) is the trustworthy C/B figure.
- `control_quality=noisy` in the JSON means “|median ratio−1| > 3 % or CV>5 %”.
  For C/B v311 that flag is a real +12 % with *low* CV, not a bad control.
- No ABBA vs gmqtt here. Matrix gmqtt RTT remains `broker_headroom_low`.
- Invalid runs were not replaced. The only invalid slot is kept
  (`host_busy_at_start:4.1`).

## Files

- `identities.json`
- `control-bb-v311.json` / `control-bb-v5.json`
- `8e29-vs-c4-v311.json` / `8e29-vs-c4-v5.json`
- `rc14-vs-c4-v311.json` / `rc14-vs-c4-v5.json`
- driver: `scripts/run_mqttium_rtt_direct.py`
