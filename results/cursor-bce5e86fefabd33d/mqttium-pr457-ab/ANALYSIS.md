# mqttium PR #457 vs RC14 — same-window capacity A/B

Host `cursor` / fingerprint `bce5e86fefabd33d` (`clock_unpinned`).
Harness `e9c7baeeaf7c5ebc`. Profile `standard` (warmup 3 s, duration 12 s, drain 6 s).
Cpusets: SUT=0, broker=1, loadgen=2, orch=3. Closed-loop (`cadence=capacity`); `target_rate` not frozen.
Design: 8 even ABBA/BAAB blocks. Retry: whole-block, stimulus/environment only (`host_busy`, `barrier_failed`, `worker_hang`). Surprising ratios never retry. Context-switch counters (`ru_nvcsw`) were not exported by the role workers on this harness.

Pins (`PIN.json`):

- A = RC14 `c194597bcf5af4951fbec2b560600eef3cb84b3c`
- B = PR #457 runtime `8ff3acb5fce8f617013ec67a695c62c365d683c9`
- HEAD of #457 at start (`6f8a390`) differs only in `tests/unit/test_immediate_delivery_admission.py`; `src/` is identical to `8ff3acb5`.

No mqttium source was modified. No optimisation was attempted.

Estimator: `complementary_pair_geometric_mean` from `compare_verdict_from_block_ratios` (min effect 3 %). CI is on (ratio − 1).

Raw artefacts live next to this file. Full runs remain under `archive/cursor-bce5e86fefabd33d/mqttium-pr457-ab/`.

---

## A. Publish QoS1 MQTT 3.1.1 (`pub_qos_sweep_telemetry` variant 2)

Campaign #41 claim: final ~56 030 vs RC14 ~58 130 (−3.6 %). MQTT 5 QoS1 was at parity.

Knobs: `qos_publish=1`, `protocol=MQTTv311`, `payload=telemetry256`, `inflight=outstanding=64`, publisher-only.

### Verdict

**statistically unresolved** for a 3–4 % library effect on this runner.

The campaign’s specifically *negative* −3.6 % is **not reproduced**. Same-window A/B has the opposite sign (B faster in every block). That is enough to reject “final is 3.6 % slower than RC14”. It is **not** enough to claim that `8ff3acb5` is 3.4 % faster: the A/A control cannot resolve an effect of that size.

### Evidence — A/A RC14 vs RC14 (`aa-qos1-v311.json`)

| | A | B (same SHA) |
| --- | ---: | ---: |
| Median msgs/s | 55 696 | 55 135 |
| CV | 6.11 % | 7.97 % |
| Valid slots | 15 | 16 |
| Bottleneck | `sut_limited` | `sut_limited` |

- Qualification **not ok**: 7/8 blocks. Block 4 retry-exhausted (`host_busy_at_start:5.0` then `:4.3`).
- Median B/A **0.988**. Block ratios: 0.993, 1.116, 0.953, 1.058, 0.840, 0.993, 0.995.
- 95 % CI on (ratio−1): **−8.7 % … +5.3 %**, includes 0.
- Same-source slots span 39 574 … 57 364 msgs/s. Failures / backpressure / sync rejects: 0.
- Broker CPU median ~20 %. SUT cost median ~17.8 µs/msg.
- Statistical verdict `inconclusive`. This runner, in this A/A window, **cannot distinguish 3–4 %**.

### Evidence — A/B RC14 vs `8ff3acb5` (`ab-qos1-v311.json`)

| | A RC14 | B 8ff3acb5 |
| --- | ---: | ---: |
| Median msgs/s | 56 662 | 58 519 |
| CV | 0.60 % | 0.60 % |
| n | 16/16 | 16/16 |

- Qualification **ok**: 8/8, 32/32 `valid` `sut_limited`, 0 retries.
- B/A **1.034**. Every block B > A: 1.034, 1.039, 1.034, 1.022, 1.027, 1.042, 1.039, 1.032.
- 95 % CI on (ratio−1): **+3.0 % … +3.6 %**, excludes 0.
- Statistical verdict `improvement` (threshold 3 %).
- Broker CPU ~20 % both arms. Cost 17.64 µs/msg (A) vs 17.09 µs/msg (B). Failures / backpressure: 0.

Sign of the campaign gap is reversed. A/A showed 8–12 % same-source swings in a noisier window, so the +3.4 % is **not** a product claim.

### Attribution

None. Stop rule: no confirmed regression. No mqttium profiling.

### Minimal plausible fix

None.

### Complexity

Do not touch mqttium QoS1 publish for a 3–4 % campaign delta that this runner cannot certify and that same-window A/B does not reproduce as a slowdown.

---

## B. Subscribe hierarchy `+` (`sub_hierarchy_telemetry` variant 0)

Campaign #41 claim: final ~194 220 vs c4f477 ~199 205 / RC14 ~199 915 (−2.5 to −2.9 %), tagged `offer_limited`.

Knobs: `subscription=plus`, `protocol=MQTTv311`, `payload=telemetry256`, QoS 0 subscribe, campaign ingress offer 200k unless noted.

### Verdict

**benchmark ceiling / invalid comparison**.

Both arms sit on the 200k offer when the host is quiet. The offer probe cannot create a SUT-limited regime: 160k clips to the offer; 250k does not stably lift delivery and one repeat is `broker_fanout_limited`. Campaign −2.9 % is **not** present in the same-window A/B (−0.43 %, all 32 slots still `offer_limited`). The same ~0.25–0.43 % B undershoot appears on `#` and exact-topic controls, so it is not a `+` matcher regression.

### Evidence — offer probe on `8ff3acb5` (`probe-plus-offer.json`)

| Requested offer | Rates (msgs/s) | Median | Notes |
| ---: | --- | ---: | --- |
| 160k | 159 324, 154 075 | 156 700 | holds / `offer_limited` |
| 200k | 184 815, 191 859 | 188 337 | 92–96 % of offer |
| 250k | 195 502, 172 234 | 183 868 | no stable lift; second run `broker_fanout_limited:172234/149944` |

Raising the offer does not produce a clean higher SUT rate. Lowering it clips both runtimes to the offer, so a 3 % SUT gap cannot appear.

### Evidence — A/A RC14 vs RC14 (`aa-sub-plus.json`)

| | A | B (same SHA) |
| --- | ---: | ---: |
| Median msgs/s | 199 865 | 199 746 |
| CV | 4.09 % | 5.77 % |
| Median B/A |  | **0.999** |

- Qualification **not ok**: 7/8. Block 2 retry-exhausted (`host_busy_at_start`).
- After warm-up, later blocks sit on the offer (ratios 0.9995–1.0008).
- 30/32 active slots `offer_limited`; 2 `broker_limited` (`broker_fanout_limited`).
- Broker CPU median ~88 %. SUT cost ~5.00 µs/msg. Failures / backpressure: 0.
- Early-window noise already spans the campaign 2–3 % gap. This A/A cannot resolve a SUT difference of that size **because the SUT is not the limit**.

### Evidence — A/B plus RC14 vs `8ff3acb5` (`ab-sub-plus.json`)

| | A RC14 | B 8ff3acb5 |
| --- | ---: | ---: |
| Median msgs/s | 199 904 | 199 388 |
| CV | 0.05 % | 0.54 % |
| Offer hold | 99.95 % | 99.69 % |

- Qualification **ok**: 8/8, 32/32 `valid` **`offer_limited`**, 0 retries.
- B/A **0.996**. Blocks: 0.987, 0.997, 0.998, 0.996, 0.998, 0.996, 0.997, 0.996.
- 95 % CI on (ratio−1): **−0.68 % … −0.28 %**. Effect −0.43 % ≪ 3 % threshold → statistical `inconclusive`.
- Broker CPU median 87 % (A) / 89 % (B). Cost 5.000 vs 5.015 µs/msg. Failures / backpressure: 0.
- One B slot at 194 978 (block 0); all other B slots 198.8k–199.5k. Campaign 194.2k median is **not** this window.

Stop rule: A/B within ±1–2 % at a ceiling that cannot be made SUT-limited.

### Controls — not `+`-specific

| Comparison | Median A | Median B | B/A | Blocks | Bottleneck |
| --- | ---: | ---: | ---: | --- | --- |
| plus `ab-sub-plus.json` | 199 904 | 199 388 | 0.996 | 0.987–0.998 | 32/32 `offer_limited` |
| hash `ab-sub-hash.json` | 199 945 | 199 457 | 0.997 | 0.9965–0.9983 | 32/32 `offer_limited` |
| exact v311 `ab-sub-exact-v311.json` | 199 927 | 199 443 | 0.997 | 0.992–0.999 | 32/32 `offer_limited` |

Hash: 32/32 valid, 0 retries, CI −0.29 % … −0.22 %, statistical `inconclusive`.

Exact: 32/32 valid after one operational retry of block 6 (`barrier_failed` / `WARMUP_DRAINED`; subscriber `TimeoutError` on `T_MEASURE`). Retry succeeded. Same ~0.30 % ceiling undershoot.

The ~0.25–0.43 % B shortfall is a **global receive-path / offer-hold** difference at the 200k ceiling, not a `+` matcher cost.

### Attribution

None. No `+` matcher regression is confirmed. Profiling the decode → route → `+` matcher → callback path would not reproduce a SUT-limited delta, because none was measured.

### Minimal plausible fix

None. A new cache, index, or routing structure for 0.3 % at an offer ceiling is rejected by the project policy even if someone later confirmed it.

### Complexity

Do not touch mqttium subscribe routing before merge of #457 on the basis of campaign #41 `+` numbers. Those numbers compare two offer-limited medians taken hours apart.

---

## What this does *not* say

- It does not say `8ff3acb5` is 3.4 % faster at QoS1 v311. A/A cannot resolve that.
- It does not say the two runtimes are identical on subscribe. B holds ~99.7 % of a 200k offer vs A at 99.95 %. That is not a 2.9 % SUT regression and is not matcher-specific.
- `c4f477` was not a third A/B arm. Campaign #41 already has it; adding it here would have diluted the A vs B window.

## Driver

`scripts/run_mqttium_capacity_ab.py` + `scripts/run_mqttium_pr457_suspects.sh`.
Log: `logs/mqttium-pr457-ab.log` (`PR457_AB_START` 07:42:38Z → `PR457_AB_DONE` 09:30:31Z).
