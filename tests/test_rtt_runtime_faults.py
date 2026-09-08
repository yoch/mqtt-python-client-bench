from __future__ import annotations

import hashlib
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from mqtt_client_bench.roles import rtt_drive


class RttRuntimeFaultTests(unittest.TestCase):
    def test_runtime_snapshot_records_page_fault_counters(self) -> None:
        usage = SimpleNamespace(
            ru_minflt=123,
            ru_majflt=4,
            ru_nvcsw=5,
            ru_nivcsw=6,
            ru_utime=1.25,
            ru_stime=0.5,
            ru_maxrss=789,
        )
        with patch.object(rtt_drive.resource, "getrusage", return_value=usage), patch.object(
            rtt_drive.gc, "get_stats", return_value=[{"collections": 1}, {"collections": 2}, {"collections": 3}]
        ), patch.object(rtt_drive.gc, "get_count", return_value=(7, 8, 9)), patch.object(
            rtt_drive, "_memory_layout_snapshot", return_value={"maps_sha256": "abcd"}
        ):
            snapshot = rtt_drive.process_runtime_snapshot()
        self.assertEqual(snapshot["ru_minflt"], 123)
        self.assertEqual(snapshot["ru_majflt"], 4)
        self.assertEqual(snapshot["ru_nvcsw"], 5)
        self.assertEqual(snapshot["gc_count"], [7, 8, 9])
        self.assertEqual(snapshot["memory_layout"], {"maps_sha256": "abcd"})

    def test_runtime_delta_includes_faults(self) -> None:
        start = {
            "ru_minflt": 100,
            "ru_majflt": 2,
            "ru_nvcsw": 10,
            "ru_nivcsw": 3,
            "ru_utime_s": 1.0,
            "ru_stime_s": 0.2,
            "gc_collections": [4, 5, 6],
        }
        end = {
            "ru_minflt": 182,
            "ru_majflt": 2,
            "ru_nvcsw": 40,
            "ru_nivcsw": 7,
            "ru_utime_s": 2.5,
            "ru_stime_s": 0.6,
            "gc_collections": [5, 8, 6],
        }
        delta = rtt_drive.process_runtime_delta(start, end)
        self.assertEqual(delta["ru_minflt"], 82)
        self.assertEqual(delta["ru_majflt"], 0)
        self.assertEqual(delta["ru_nvcsw"], 30)
        self.assertEqual(delta["gc_collections"], [1, 3, 0])

    def test_memory_layout_snapshot_keeps_small_correlatable_view(self) -> None:
        maps = (
            "55550000-55560000 r-xp 00000000 00:00 0 /usr/bin/python3\n"
            "66660000-666a0000 rw-p 00000000 00:00 0 [heap]\n"
            "7f010000-7f020000 r-xp 00000000 00:00 0 /usr/lib/libc.so.6\n"
            "7f100000-7f180000 r-xp 00000000 00:00 0 /usr/lib/libpython3.14.so.1.0\n"
            "7fff0000-80000000 rw-p 00000000 00:00 0 [stack]\n"
        )
        with patch.object(rtt_drive.Path, "read_text", return_value=maps):
            layout = rtt_drive._memory_layout_snapshot()
        self.assertIsNotNone(layout)
        assert layout is not None
        self.assertEqual(layout["mapping_count"], 5)
        self.assertEqual(layout["heap_start"], "66660000")
        self.assertEqual(layout["heap_end"], "666a0000")
        self.assertEqual(layout["libc_base"], "7f010000")
        self.assertEqual(layout["libpython_base"], "7f100000")
        self.assertEqual(layout["stack_start"], "7fff0000")
        self.assertEqual(layout["maps_sha256"], hashlib.sha256(maps.encode()).hexdigest()[:16])

    def test_memory_layout_snapshot_is_optional_off_linux(self) -> None:
        with patch.object(rtt_drive.Path, "read_text", side_effect=OSError("no proc")):
            self.assertIsNone(rtt_drive._memory_layout_snapshot())


if __name__ == "__main__":
    unittest.main()
