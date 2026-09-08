from __future__ import annotations

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
        ), patch.object(rtt_drive.gc, "get_count", return_value=(7, 8, 9)):
            snapshot = rtt_drive.process_runtime_snapshot()
        self.assertEqual(snapshot["ru_minflt"], 123)
        self.assertEqual(snapshot["ru_majflt"], 4)
        self.assertEqual(snapshot["ru_nvcsw"], 5)
        self.assertEqual(snapshot["gc_count"], [7, 8, 9])

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


if __name__ == "__main__":
    unittest.main()
