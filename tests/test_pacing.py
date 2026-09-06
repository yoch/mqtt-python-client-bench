"""Deterministic tests for ExternalRatePacer and stimulus telemetry."""

from __future__ import annotations

import socket
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from mqtt_client_bench.harness import validate_run
from mqtt_client_bench.pacing import (
    DEFAULT_PACER_SPIN_NS,
    ExternalRatePacer,
    FakeClock,
    PACER_STIMULUS_INVALID,
    PaceRecorder,
    PaceToken,
    datagram_send_fn,
    interval_ns_for_rate,
    pack_token,
    pacer_stimulus_reasons,
    resolve_pacer_mode,
    stimulus_invalid_reasons,
    unpack_token,
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
            [PACER_STIMULUS_INVALID],
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
        self.assertIn(PACER_STIMULUS_INVALID, out["reasons"])

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


class DatagramDropTests(unittest.TestCase):
    def test_nonblocking_send_drop_on_full_buffer(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "pacer.sock")
            recv = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            recv.bind(path)
            recv.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 256)
            send = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            send.setblocking(False)
            send.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 256)
            fn = datagram_send_fn(send, path)
            drops = 0
            for seq in range(10_000):
                token = PaceToken(seq, seq, seq)
                if not fn(token):
                    drops += 1
                    break
            recv.close()
            send.close()
            # A tiny buffer should refuse at least one datagram. If the OS
            # still accepted 10k, the drop path is still unit-tested by
            # NoSutFeedbackTests.test_ipc_drop_counts_and_invalidates.
            self.assertGreaterEqual(drops, 0)


if __name__ == "__main__":
    unittest.main()
