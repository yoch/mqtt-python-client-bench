"""Deterministic tests for ExternalRatePacer and stimulus telemetry."""

from __future__ import annotations

import inspect
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from mqtt_client_bench.harness import validate_run
from mqtt_client_bench.pacing import (
    DEFAULT_PACE_SAMPLE_LIMIT,
    DEFAULT_PACER_RECEIVE_GRACE_NS,
    DEFAULT_PACER_SPIN_NS,
    DEFAULT_PACER_STARTUP_GUARD_NS,
    ExternalRatePacer,
    FakeClock,
    PACER_STIMULUS_INVALID,
    PACE_SAMPLE_COLUMNS,
    PaceRecorder,
    PaceToken,
    PacerClient,
    PacerPhaseError,
    STIMULUS_DUPLICATE,
    STIMULUS_EMITTED_VS_SCHEDULED,
    STIMULUS_INTERNAL_GAP,
    STIMULUS_OUT_OF_WINDOW,
    STIMULUS_PREFIX_LOSS,
    STIMULUS_RECEIVED_VS_EMITTED,
    STIMULUS_SEND_FAILURE,
    STIMULUS_STALE_PHASE,
    STIMULUS_SUFFIX_LOSS,
    absolute_start_ns_from_start_command,
    choose_absolute_start_ns,
    interval_ns_for_rate,
    load_phase_stats,
    pack_token,
    pacer_stimulus_reasons,
    receive_window_open,
    resolve_pacer_mode,
    stimulus_invalid_reasons,
    tokens_expected_in_window,
    unpack_token,
    wait_for_phase_complete,
)
from mqtt_client_bench.pairwise import (
    AA_CONTROL_MAX_ABS_EFFECT_PCT,
    AA_CONTROL_MAX_PAIR_UNIT_ABS_PCT,
)


class PacerModeTests(unittest.TestCase):
    def test_capacity_ignores_external(self):
        self.assertEqual(
            resolve_pacer_mode({"pacer_mode": "external", "cadence": "capacity"}, None),
            "in_loop",
        )
        self.assertEqual(
            resolve_pacer_mode({"pacer_mode": "external", "cadence": "capacity"}, 5000.0),
            "in_loop",
        )

    def test_burst_and_completion_gated_stay_in_loop(self):
        for cadence in ("burst", "microburst", "batch64"):
            self.assertEqual(
                resolve_pacer_mode({"pacer_mode": "external", "cadence": cadence}, 1000.0),
                "in_loop",
                cadence,
            )

    def test_open_loop_accepts_external(self):
        self.assertEqual(
            resolve_pacer_mode({"pacer_mode": "external", "cadence": "loaded75"}, 5000.0),
            "external",
        )

    def test_default_is_in_loop_control(self):
        self.assertEqual(resolve_pacer_mode({"cadence": "loaded75"}, 5000.0), "in_loop")

    def test_aa_gate_margins_unchanged(self):
        self.assertEqual(AA_CONTROL_MAX_ABS_EFFECT_PCT, 3.0)
        self.assertEqual(AA_CONTROL_MAX_PAIR_UNIT_ABS_PCT, 3.0)


class AbsoluteScheduleTests(unittest.TestCase):
    def test_late_iteration_does_not_shift_later_deadlines(self):
        clock = FakeClock(0)
        sent = []

        def send(token: PaceToken) -> bool:
            sent.append(token)
            return True

        pacer = ExternalRatePacer(
            interval_ns=1_000_000,
            start_ns=0,
            spin_ns=50_000,
            clock=clock,
            send_fn=send,
        )
        pacer.emit_one()
        self.assertEqual(pacer.deadline(0), 0)
        clock.advance(5_000_000)
        pacer.emit_one()
        self.assertEqual(pacer.deadline(1), 1_000_000)
        self.assertEqual(pacer.deadline(2), 2_000_000)
        self.assertEqual(sent[1].scheduled_deadline_ns, 1_000_000)
        self.assertGreater(sent[1].pacer_emission_ns, sent[1].scheduled_deadline_ns)
        self.assertEqual(pacer.deadline(2), 0 + 2 * 1_000_000)

    def test_sleep_then_spin_not_full_interval_spin(self):
        clock = FakeClock(0)
        pacer = ExternalRatePacer(
            interval_ns=1_000_000,
            start_ns=0,
            spin_ns=50_000,
            clock=clock,
            send_fn=lambda token: True,
        )
        pacer.emit_one()
        pacer.emit_one()
        self.assertEqual(clock.sleeps_ns, [950_000])
        self.assertEqual(DEFAULT_PACER_SPIN_NS, 50_000)


class NoSutFeedbackTests(unittest.TestCase):
    def test_slow_receiver_does_not_change_calendar(self):
        clock = FakeClock(0)
        deadlines = []

        def send(token: PaceToken) -> bool:
            deadlines.append(pacer.deadline(token.sequence + 1))
            return True

        pacer = ExternalRatePacer(
            interval_ns=1000,
            start_ns=10_000,
            spin_ns=100,
            clock=clock,
            send_fn=send,
        )
        pacer.emit_one()
        pacer.emit_one()
        self.assertEqual(deadlines[0], 10_000 + 1000)
        self.assertEqual(pacer.deadline(0), 10_000)
        self.assertEqual(pacer.deadline(1), 11_000)
        self.assertEqual(pacer.deadline(2), 12_000)

    def test_ipc_drop_counts_and_invalidates(self):
        clock = FakeClock(0)
        results = [True, False, True]

        def send(token: PaceToken) -> bool:
            del token
            return results.pop(0)

        pacer = ExternalRatePacer(
            interval_ns=1000,
            start_ns=0,
            spin_ns=0,
            clock=clock,
            send_fn=send,
        )
        pacer.emit_until(3000)
        self.assertEqual(pacer.recorder.token_send_failures, 1)
        self.assertEqual(pacer.recorder.tokens_emitted, 2)
        self.assertEqual(pacer.recorder.tokens_scheduled, 3)
        self.assertFalse(pacer.recorder.stimulus_valid())
        self.assertEqual(
            stimulus_invalid_reasons(pacer.recorder.summary()),
            [STIMULUS_SEND_FAILURE],
        )


class SequenceTests(unittest.TestCase):
    def test_strictly_increasing_and_gaps(self):
        rec = PaceRecorder(mode="external", target_rate=1000.0, target_interval_ns=1_000_000)
        rec.note_gap(0)
        rec.note_gap(1)
        rec.note_gap(3)
        self.assertEqual(rec.sequence_gaps, 1)
        rec.note_gap(3)
        self.assertGreaterEqual(rec.sequence_gaps, 2)

    def test_pack_roundtrip_and_magic(self):
        raw = pack_token(7, 100, 120)
        token = unpack_token(raw)
        self.assertIsNotNone(token)
        self.assertEqual(token.sequence, 7)
        self.assertEqual(token.scheduled_deadline_ns, 100)
        self.assertEqual(token.pacer_emission_ns, 120)
        self.assertIsNone(unpack_token(b"xxxx"))
        self.assertIsNone(unpack_token(b""))


class TelemetryTests(unittest.TestCase):
    def test_lateness_intervals_catch_up_and_burst(self):
        rec = PaceRecorder(mode="in_loop", target_rate=5000.0, target_interval_ns=200_000)
        rec.record_emission(0, 0, 0, sent=True)
        rec.record_emission(1, 200_000, 200_000, sent=True)
        rec.record_emission(2, 400_000, 650_000, sent=True)
        rec.record_emission(3, 600_000, 660_000, sent=True)
        self.assertEqual(rec.catch_up_events, 1)
        self.assertGreaterEqual(rec.microburst_emissions, 1)
        summary = rec.summary(duration_s=1.0)
        self.assertEqual(summary["pacer_lateness"]["max"], 250_000)
        self.assertIsNotNone(summary["emission_intervals"]["p50"])
        self.assertIn("catch_up", summary)
        self.assertIn("microburst", summary)
        self.assertEqual(summary["mode"], "in_loop")

    def test_receiver_delays(self):
        rec = PaceRecorder(mode="external", target_rate=1000.0, target_interval_ns=1_000_000)
        token = PaceToken(0, 1000, 1100)
        rec.record_receiver(token, 1300, None)
        rec.note_receiver_to_publish(1300, 1400)
        summary = rec.summary()
        self.assertEqual(summary["emission_to_receiver_delay"]["p50"], 200)
        self.assertEqual(summary["receiver_to_publish_delay"]["p50"], 100)
        self.assertEqual(summary["tokens_received"], 1)


class ValidatePacerStimulusTests(unittest.TestCase):
    def test_external_gap_invalidates_run(self):
        point = {
            "cadence": "loaded75",
            "target_rate": 1000.0,
            "pacer_mode": "external",
            "topology": "application_rtt",
            "duration_s": 3.0,
            "shared_load_fraction": 0.25,
        }
        worker = {
            "role": "rtt_initiator",
            "ok": True,
            "duration_s": 3.0,
            "offered": 3000,
            "sent_in_window": 3000,
            "completed_in_window": 3000,
            "missed_due_to_backpressure": 0,
            "pacing": {
                "mode": "external",
                "token_send_failures": 0,
                "sequence_gaps": 2,
                "tokens_emitted": 3000,
                "tokens_received": 2998,
                "stimulus_valid": False,
            },
        }
        out = validate_run(point, [worker], None, [])
        self.assertEqual(out["status"], "inconclusive")
        self.assertTrue(
            any(reason.startswith(PACER_STIMULUS_INVALID) for reason in out["reasons"])
        )

    def test_in_loop_gap_is_not_the_external_gate(self):
        point = {
            "cadence": "loaded75",
            "target_rate": 1000.0,
            "pacer_mode": "in_loop",
            "topology": "application_rtt",
            "duration_s": 3.0,
        }
        worker = {
            "role": "rtt_initiator",
            "ok": True,
            "duration_s": 3.0,
            "offered": 3000,
            "sent_in_window": 3000,
            "completed_in_window": 3000,
            "missed_due_to_backpressure": 0,
            "pacing": {"mode": "in_loop", "sequence_gaps": 2, "token_send_failures": 0},
        }
        self.assertEqual(pacer_stimulus_reasons(point, [worker]), [])

    def test_capacity_worker_without_pacing_stays_valid(self):
        point = {"cadence": "capacity", "topology": "application_rtt", "pacer_mode": "external"}
        worker = {"role": "rtt_initiator", "ok": True, "completed_in_window": 100, "sent_in_window": 100}
        out = validate_run(point, [worker], None, [])
        self.assertNotIn(PACER_STIMULUS_INVALID, out["reasons"])


class InLoopSourceTests(unittest.TestCase):
    def test_native_open_loop_still_uses_asyncio_sleep(self):
        src = (ROOT / "src/mqtt_client_bench/roles/rtt_initiator.py").read_text()
        send = src.split("async def _send_loop_async")[1].split("if __name__")[0]
        self.assertIn("await asyncio.sleep(min(0.001, next_send - now))", send)
        self.assertIn('pacer_mode == "external"', send)
        self.assertNotIn("process_runtime_snapshot", send)

    def test_causal_script_counterbalances_and_refuses_standard(self):
        src = (ROOT / "scripts/run_pacer_causal_aa.sh").read_text()
        self.assertIn("--pacer-mode", src)
        self.assertIn("run_cell mqttium 0 in_loop external", src)
        self.assertIn("run_cell gmqtt 0 external in_loop", src)
        self.assertIn("run_cell mqttium 4 in_loop external", src)
        self.assertIn("run_cell gmqtt 4 external in_loop", src)
        self.assertIn("load-profile-dir", src)
        self.assertIn("PROFILE=standard", src)
        self.assertIn("NO OFFICIAL RANKING", src)
        self.assertIn("temporal_trace", src)
        self.assertIn("pacer-causal requires scaling_governor=performance", src)
        self.assertIn("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor", src)


class TokenCompletenessTests(unittest.TestCase):
    def _recorder(self, expected=4):
        rec = PaceRecorder(mode="external", target_rate=1000.0, target_interval_ns=1_000_000)
        rec.set_window(0, expected * 1_000_000, 1_000_000)
        self.assertEqual(rec.tokens_expected_in_measure_window, expected)
        return rec

    def _feed(self, rec, sequences, *, emitted=None):
        for seq in sequences:
            rec.record_receiver(PaceToken(seq, seq * 1_000_000, seq * 1_000_000 + 1), seq * 1_000_000 + 2)
        n = rec.tokens_expected_in_measure_window or 0
        rec.tokens_scheduled = n if emitted is None else int(emitted)
        rec.tokens_emitted = n if emitted is None else int(emitted)
        rec.token_send_failures = 0

    def test_internal_gap_fails(self):
        rec = self._recorder()
        self._feed(rec, [0, 1, 3])
        reasons = rec.completeness_reasons()
        self.assertIn(STIMULUS_INTERNAL_GAP, reasons)
        self.assertFalse(rec.stimulus_valid())

    def test_suffix_loss_fails_without_following_sequence(self):
        rec = self._recorder()
        self._feed(rec, [0, 1, 2])
        reasons = rec.completeness_reasons()
        self.assertIn(STIMULUS_SUFFIX_LOSS, reasons)
        self.assertIn(STIMULUS_RECEIVED_VS_EMITTED, reasons)
        self.assertFalse(rec.stimulus_valid())

    def test_prefix_loss_fails(self):
        rec = self._recorder()
        self._feed(rec, [1, 2, 3])
        reasons = rec.completeness_reasons()
        self.assertIn(STIMULUS_PREFIX_LOSS, reasons)
        self.assertNotIn(STIMULUS_SUFFIX_LOSS, reasons)
        self.assertFalse(rec.stimulus_valid())

    def test_duplicate_fails(self):
        rec = self._recorder()
        self._feed(rec, [0, 1, 1, 2, 3])
        reasons = rec.completeness_reasons()
        self.assertIn(STIMULUS_DUPLICATE, reasons)
        self.assertFalse(rec.stimulus_valid())

    def test_exact_complete_sequence_passes(self):
        rec = self._recorder()
        self._feed(rec, [0, 1, 2, 3])
        self.assertEqual(rec.completeness_reasons(), [])
        self.assertTrue(rec.stimulus_valid())
        summary = rec.summary()
        self.assertEqual(summary["stimulus_invalid_reasons"], [])
        self.assertTrue(summary["stimulus_valid"])

    def test_emitted_vs_scheduled_accounting_fails(self):
        rec = self._recorder()
        self._feed(rec, [0, 1, 2, 3], emitted=4)
        rec.tokens_scheduled = 5
        rec.token_send_failures = 0
        self.assertIn(STIMULUS_EMITTED_VS_SCHEDULED, rec.completeness_reasons())

    def test_persists_exact_reason(self):
        rec = self._recorder()
        self._feed(rec, [0, 1, 2])
        summary = rec.summary()
        self.assertIn(STIMULUS_SUFFIX_LOSS, summary["stimulus_invalid_reasons"])
        self.assertTrue(
            all(item.startswith(PACER_STIMULUS_INVALID) for item in summary["stimulus_invalid_reasons"])
        )


class PaceRecorderMemoryTests(unittest.TestCase):
    def test_memory_is_capped_after_many_tokens(self):
        rec = PaceRecorder(
            mode="in_loop",
            target_rate=10_000.0,
            target_interval_ns=100_000,
            max_samples=DEFAULT_PACE_SAMPLE_LIMIT,
        )
        baseline = rec.memory_bytes()
        self.assertEqual(baseline, DEFAULT_PACE_SAMPLE_LIMIT * len(PACE_SAMPLE_COLUMNS) * 8)
        for i in range(100_000):
            token = PaceToken(i, i * 100_000, i * 100_000)
            rec.record_receiver(token, i * 100_000, None)
            rec.note_receiver_to_publish(i * 100_000, i * 100_000 + 50)
        self.assertEqual(rec.memory_bytes(), baseline)
        self.assertLessEqual(rec.sample_count(), DEFAULT_PACE_SAMPLE_LIMIT)
        self.assertEqual(rec.tokens_received, 100_000)
        self.assertEqual(rec.catch_up_events, 0)


class AbsoluteStartTests(unittest.TestCase):
    def test_startup_guard_is_explicit_milliseconds(self):
        self.assertEqual(DEFAULT_PACER_STARTUP_GUARD_NS, 5_000_000)
        self.assertEqual(choose_absolute_start_ns(10, 5_000_000), 5_000_010)

    def test_start_command_requires_absolute_start(self):
        with self.assertRaises(ValueError):
            absolute_start_ns_from_start_command({"interval_ns": 1000})
        self.assertEqual(absolute_start_ns_from_start_command({"absolute_start_ns": 42}), 42)

    def test_no_emission_before_absolute_start(self):
        clock = FakeClock(0)
        sent = []

        def send(token: PaceToken) -> bool:
            sent.append((clock.now_ns, token))
            return True

        pacer = ExternalRatePacer(
            interval_ns=1_000,
            start_ns=10_000,
            spin_ns=0,
            clock=clock,
            send_fn=send,
        )
        pacer.emit_one()
        self.assertGreaterEqual(sent[0][0], 10_000)
        self.assertGreaterEqual(sent[0][1].pacer_emission_ns, 10_000)
        self.assertEqual(sent[0][1].scheduled_deadline_ns, 10_000)
        self.assertGreaterEqual(clock.now_ns, 10_000)

    def test_rate_pacer_uses_command_start_not_now(self):
        src = (ROOT / "src/mqtt_client_bench/roles/rate_pacer.py").read_text()
        self.assertIn("absolute_start_ns_from_start_command", src)
        self.assertNotIn("start_ns = clock.monotonic_ns()", src)
        initiator = (ROOT / "src/mqtt_client_bench/roles/rtt_initiator.py").read_text()
        self.assertIn("choose_absolute_start_ns", initiator)
        self.assertIn("DEFAULT_PACER_STARTUP_GUARD_NS", initiator)

    def test_window_token_count_matches_emit_until(self):
        self.assertEqual(tokens_expected_in_window(0, 10_000, 1_000), 10)
        clock = FakeClock(0)
        pacer = ExternalRatePacer(
            interval_ns=1_000,
            start_ns=0,
            spin_ns=0,
            clock=clock,
            send_fn=lambda token: True,
        )
        rec = pacer.emit_until(10_000)
        self.assertEqual(rec.tokens_scheduled, 10)
        self.assertEqual(rec.tokens_expected_in_measure_window, 10)


class InLoopRecorderCostTests(unittest.TestCase):
    def test_in_loop_instrumentation_stays_under_harness_budget(self):
        rec = PaceRecorder(mode="in_loop", target_rate=5000.0, target_interval_ns=200_000)
        n = 20_000
        t0 = time.perf_counter_ns()
        for i in range(n):
            ns = i * 200_000
            rec.record_receiver(PaceToken(i, ns, ns), ns, None)
            rec.note_receiver_to_publish(ns, ns + 10)
        rec_ns = (time.perf_counter_ns() - t0) / n
        t0 = time.perf_counter_ns()
        acc = 0
        for i in range(n):
            acc += i
        empty_ns = (time.perf_counter_ns() - t0) / n
        del acc
        # Shared CI runners jitter past 2 µs. 10 µs is still << a 200 µs
        # slot and fails if the recorder grows lists or does extra syscalls.
        self.assertLess(rec_ns, 10_000)
        self.assertLess(rec_ns - empty_ns, 10_000)
        self.assertEqual(DEFAULT_PACER_RECEIVE_GRACE_NS, DEFAULT_PACER_STARTUP_GUARD_NS)
        self.assertEqual(DEFAULT_PACER_RECEIVE_GRACE_NS, 5_000_000)


class PhaseProtocolTests(unittest.TestCase):
    def _write_stats(self, path, phase_id, tokens_emitted):
        Path(path).write_text(
            json.dumps(
                {
                    "phase_id": phase_id,
                    "tokens_emitted": tokens_emitted,
                    "tokens_scheduled": tokens_emitted,
                    "tokens_received": tokens_emitted,
                }
            ),
            encoding="utf-8",
        )

    def test_warmup_stats_are_not_accepted_as_measure(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "pacer.stats.json")
            self._write_stats(path, "warmup-1-1", 3844)
            with self.assertRaises(PacerPhaseError) as ctx:
                load_phase_stats(path, "measure-2-2")
            self.assertEqual(ctx.exception.reason, "stale_phase")
            self.assertEqual(ctx.exception.stimulus_reason(), STIMULUS_STALE_PHASE)

    def test_read_before_measure_complete_does_not_return_warmup_stats(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "pacer.stats.json")
            self._write_stats(path, "warmup-1-1", 3844)
            loaded = {"n": 0}

            def load_stats():
                loaded["n"] += 1
                return load_phase_stats(path, "measure-2-2")

            with self.assertRaises(PacerPhaseError) as ctx:
                wait_for_phase_complete(
                    phase_id="measure-2-2",
                    readline=lambda: None,
                    poll=lambda: None,
                    load_stats=load_stats,
                    timeout_s=1.0,
                    clock=lambda: 0.0,
                    wait_readable=lambda remaining: False,
                )
            self.assertEqual(ctx.exception.reason, "phase_timeout")
            self.assertEqual(loaded["n"], 0)
            with self.assertRaises(PacerPhaseError) as stale:
                load_phase_stats(path, "measure-2-2")
            self.assertEqual(stale.exception.reason, "stale_phase")

    def test_phase_complete_ack_mismatch_fails(self):
        with self.assertRaises(PacerPhaseError) as ctx:
            wait_for_phase_complete(
                phase_id="measure-2-2",
                readline=lambda: {"event": "phase_complete", "phase_id": "warmup-1-1"},
                poll=lambda: None,
                load_stats=lambda: {"tokens_emitted": 3844},
                timeout_s=1.0,
                clock=lambda: 0.0,
                wait_readable=lambda remaining: True,
            )
        self.assertEqual(ctx.exception.reason, "phase_mismatch")

    def test_missing_stats_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "absent.stats.json")
            with self.assertRaises(PacerPhaseError) as ctx:
                load_phase_stats(path, "measure-1-1")
            self.assertEqual(ctx.exception.reason, "missing_stats")

    def test_read_stats_requires_phase_id(self):
        sig = inspect.signature(PacerClient.read_stats)
        self.assertIn("phase_id", sig.parameters)
        self.assertIs(sig.parameters["phase_id"].default, inspect.Parameter.empty)

    def test_phase_id_mismatch_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "pacer.stats.json")
            self._write_stats(path, "measure-1-1", 10)
            with self.assertRaises(PacerPhaseError) as ctx:
                load_phase_stats(path, "measure-9-9")
            self.assertEqual(ctx.exception.reason, "stale_phase")

    def test_expected_phase_completes_and_accepts_exact_stats(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "pacer.stats.json")
            self._write_stats(path, "measure-2-99", 11532)
            messages = [{"event": "phase_complete", "phase_id": "measure-2-99"}]

            def readline():
                return messages.pop(0) if messages else None

            stats = wait_for_phase_complete(
                phase_id="measure-2-99",
                readline=readline,
                poll=lambda: None,
                load_stats=lambda: load_phase_stats(path, "measure-2-99"),
                timeout_s=1.0,
                clock=lambda: 0.0,
                wait_readable=lambda remaining: True,
            )
            self.assertEqual(stats["tokens_emitted"], 11532)
            self.assertEqual(stats["phase_id"], "measure-2-99")

    def test_pacer_death_before_completion_fails(self):
        def readline():
            return None

        with self.assertRaises(PacerPhaseError) as ctx:
            wait_for_phase_complete(
                phase_id="measure-1-1",
                readline=readline,
                poll=lambda: 1,
                load_stats=lambda: {},
                timeout_s=1.0,
                clock=lambda: 0.0,
                wait_readable=lambda remaining: True,
            )
        self.assertEqual(ctx.exception.reason, "pacer_exited")

    def test_phase_completion_timeout_fails(self):
        ticks = {"n": 0}

        def clock():
            ticks["n"] += 1
            return 0.0 if ticks["n"] == 1 else 10.0

        with self.assertRaises(PacerPhaseError) as ctx:
            wait_for_phase_complete(
                phase_id="measure-1-1",
                readline=lambda: None,
                poll=lambda: None,
                load_stats=lambda: {},
                timeout_s=1.0,
                clock=clock,
                wait_readable=lambda remaining: False,
            )
        self.assertEqual(ctx.exception.reason, "phase_timeout")

    def test_source_handshake_is_phase_id_not_sleep(self):
        initiator = (ROOT / "src/mqtt_client_bench/roles/rtt_initiator.py").read_text()
        pacer = (ROOT / "src/mqtt_client_bench/roles/rate_pacer.py").read_text()
        client = (ROOT / "src/mqtt_client_bench/pacing.py").read_text()
        self.assertIn("wait_phase_complete", initiator)
        self.assertIn("phase_id", initiator)
        self.assertIn("phase_complete", pacer)
        self.assertIn("phase_id_from_start_command", pacer)
        self.assertIn("wait_phase_complete", client)


class BoundaryTokenTests(unittest.TestCase):
    def test_token_just_after_nominal_until_is_in_window(self):
        rec = PaceRecorder(mode="external", target_rate=1000.0, target_interval_ns=1_000)
        rec.set_window(0, 10_000, 1_000, grace_ns=5_000_000)
        self.assertEqual(rec.tokens_expected_in_measure_window, 10)
        for seq in range(10):
            recv = seq * 1_000 + 2 if seq < 9 else 10_000 + 50
            rec.record_receiver(PaceToken(seq, seq * 1_000, seq * 1_000 + 1), recv)
        rec.tokens_scheduled = 10
        rec.tokens_emitted = 10
        rec.token_send_failures = 0
        self.assertEqual(rec.tokens_received, 10)
        self.assertEqual(rec.last_sequence, 9)
        self.assertEqual(rec.tokens_received_after_nominal_until, 1)
        self.assertEqual(rec.receiver_after_nominal_until_max_ns, 50)
        self.assertTrue(rec.stimulus_valid())
        self.assertTrue(
            receive_window_open(
                now_ns=10_000 + 50,
                nominal_until_ns=10_000,
                grace_ns=5_000_000,
                tokens_received=9,
                expected_tokens=10,
            )
        )

    def test_suffix_loss_after_bounded_grace_fails(self):
        self.assertFalse(
            receive_window_open(
                now_ns=10_000 + 5_000_000,
                nominal_until_ns=10_000,
                grace_ns=5_000_000,
                tokens_received=9,
                expected_tokens=10,
            )
        )
        rec = PaceRecorder(mode="external", target_rate=1000.0, target_interval_ns=1_000)
        rec.set_window(0, 10_000, 1_000, grace_ns=5_000_000)
        for seq in range(9):
            rec.record_receiver(PaceToken(seq, seq * 1_000, seq * 1_000 + 1), seq * 1_000 + 2)
        rec.tokens_scheduled = 10
        rec.tokens_emitted = 10
        self.assertIn(STIMULUS_SUFFIX_LOSS, rec.completeness_reasons())
        self.assertFalse(rec.stimulus_valid())

    def test_sequence_equal_to_expected_is_out_of_window(self):
        rec = PaceRecorder(mode="external", target_rate=1000.0, target_interval_ns=1_000)
        rec.set_window(0, 10_000, 1_000)
        for seq in range(10):
            rec.record_receiver(PaceToken(seq, seq * 1_000, seq * 1_000 + 1), seq * 1_000 + 2)
        rec.record_receiver(PaceToken(10, 10_000, 10_001), 10_002)
        rec.tokens_scheduled = 10
        rec.tokens_emitted = 10
        self.assertEqual(rec.tokens_received, 10)
        self.assertEqual(rec.last_sequence, 9)
        self.assertEqual(rec.invalid_tokens, 1)
        self.assertIn(STIMULUS_OUT_OF_WINDOW, rec.completeness_reasons())
        self.assertFalse(rec.stimulus_valid())

    def test_grace_does_not_create_tokens_or_shift_deadlines(self):
        clock = FakeClock(0)
        pacer = ExternalRatePacer(
            interval_ns=1_000,
            start_ns=0,
            spin_ns=0,
            clock=clock,
            send_fn=lambda token: True,
        )
        rec = pacer.emit_until(10_000)
        self.assertEqual(rec.tokens_scheduled, 10)
        self.assertEqual(pacer.deadline(9), 9_000)
        self.assertLess(pacer.deadline(9), 10_000)
        self.assertEqual(DEFAULT_PACER_RECEIVE_GRACE_NS, 5_000_000)


if __name__ == "__main__":
    unittest.main()
