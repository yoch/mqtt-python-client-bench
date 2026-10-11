# Python versions and Tachyon diagnostics

The harness supports Python 3.12–3.15. `.python-version` keeps 3.12 as the
default. Select another version, build or executable explicitly:

```bash
PYTHONPATH=src python -m mqtt_client_bench.run envs --sync --python 3.15
PYTHONPATH=src python -m mqtt_client_bench.run envs --sync --python 3.14
PYTHONPATH=src python -m mqtt_client_bench.run run --python 3.15 \
  --clients paho,mqttium --points pub_qos1_fixed,sub_qos1_fixed \
  --profile smoke --no-ceiling --output /tmp/python315-smoke.json
PYTHONPATH=src python -m mqtt_client_bench.run harness-cost --python 3.15
```

`uv python find` resolves version selectors; an executable path also works.
Each actual runtime/build gets its own `.venvs/<build-key>/<extra>` environments.
The key includes the full Python build, ABI, architecture, JIT/GIL settings and
UTF-8 mode. Existing `.venvs/<extra>` environments are not reused. Re-run `envs
--sync` to create the new layout. aiomqtt v2/v3 remain isolated. An installation
failure is reported per client, the other requested installs continue, and the
command exits nonzero. A missing client is never benchmarked through a fallback.

The worker reports its actual Python, JIT and GIL state after importing and
running the client. Session harness cost uses the selected interpreter, not
the orchestrator's default Python. Campaigns freeze the runtime, installed
package versions/provenance, lockfile digest and harness source/revision. A
resume refuses changes before starting the broker or appending evidence.
Older campaigns without those identities remain readable but cannot be resumed
safely; start a new campaign. Reports refuse mixed Python versions/builds.

## Capture the client worker

Tachyon is available in Python 3.15. Captures attach to the worker's PID after
READY and warm-up, beginning at the measured-window boundary. The requested
duration is rounded up to whole seconds as required by Tachyon's CLI. Startup
of the external profiler adds a small offset; `started_ns`, the run schedule
and Tachyon's actual duration are retained. Profiles are diagnostic, not exact
sub-millisecond phase attribution.

```bash
PYTHONPATH=src python -m mqtt_client_bench.run run --python 3.15 \
  --clients paho --points pub_qos1_fixed --profile smoke --no-ceiling \
  --sampling cpu --sampling-dir /tmp/paho-cpu --output /tmp/paho-cpu.json

PYTHONPATH=src python -m mqtt_client_bench.run campaign --python 3.15 \
  --clients mqttium --points pub_qos1_fixed_v5,sub_qos1_fixed \
  --sampling async-all --sampling-rate 1000 \
  --sampling-dir /tmp/mqttium-waits --output-dir /tmp/diagnostic-campaigns
```

Modes are `cpu`, `wall`, `gil`, `exception`, `async-running` and `async-all`.
CPU/wall/GIL/exception recipes include all worker threads. Async recipes
reconstruct await chains without incompatible CPU or all-thread flags.
`async-all` includes suspended tasks; their weights overlap and must not be
summed as elapsed latency. Use separate CPU and wait captures to distinguish
computation, flow control, pacing, broker round trips and delivery pressure.

On sufficiently large hosts the profiler gets its own physical core. A smoke
host with fewer cores may share a core; the allocation is recorded. The CPU
and memory metrics still cover the worker PID, while host-noise readings can
show the profiler. Broker/peer counters and READY/GO protocols are unchanged.

Every instrumented run carries `instrumented` and `non_comparable`. Even a
standard-duration sampling campaign is excluded from default report selection.
Never use instrumented runs in published performance rankings. To inspect one:

```bash
PYTHONPATH=src python -m mqtt_client_bench.run report \
  --input /tmp/diagnostic-campaigns --campaign CAMPAIGN --output /tmp/diagnostic-site
```

An explicit diagnostic report has a visible banner and copies available profile
files into the self-contained site, linking them by run ID. The run record maps
that ID to its client and scenario. Keep external artifacts until rendering.
Raw profiles can contain source filenames and source lines.

## Formats, validation and failures

CPU/wall/GIL/exception modes retain a binary capture and render HTML after the
measurement. Python 3.15 rejects binary output with async-aware mode, so those
recipes capture a self-contained flamegraph directly. Both recipes save its
embedded tree as `profile.json`, with mode/rate/PID/window metadata and sampling
statistics. The extraction is guarded against CPython HTML format changes.
Zero-stack captures, unavailable profilers, permission errors, failed exports
and timeouts are explicit failures; processes are terminated/reaped and logs
are retained. Throughput and latency conclusions always require fresh,
uninstrumented runs.

The profiler runs with the worker's interpreter. On Linux with Yama enabled,
only an explicitly instrumented worker authorizes its parent and descendants
to inspect it. The machine's ptrace policy and privileges are not changed.
Other platforms retain their normal inspection requirements.

Qualify the recipes against known workloads before analyzing real clients:

```bash
PYTHONPATH=src python3.15 tools/qualify_sampling.py --output /tmp/tachyon-fixtures
```

This checks CPU work in threads, the named active coroutine, and a known
suspended task. MQTTium also provides `benchmarks/sampling_profile.py` for fresh
process A/A controls and baseline/sample/sample/baseline overhead trials at
1 kHz and 10 kHz. Extend windows when sampling errors or same-code variation
make attribution uncertain. Keep cProfile for exact call counts, tracemalloc
for allocations, and perf for detailed native costs such as awscrt/TLS.

## Interpreter experiments

Run 3.14 and 3.15 campaigns separately with identical client versions, offered
rates and host/broker conditions. Use uninstrumented interleaved repetitions
and new runtime-specific memory/performance references. A smoke campaign only
validates wiring and protocol checks; it does not establish a performance gain.

For a JIT-enabled CPython build, set `PYTHON_JIT=1` for both environment sync and
the campaign; use `PYTHON_JIT=0` for the control. Runtime identity records actual
availability/enabled state. Select `--python 3.15t` for free-threaded experiments,
and inspect each worker's actual `gil_enabled`: native extensions can enable
the GIL. These builds remain separate experiments and do not imply thread-safe
client APIs. Warm-up, startup, sustained load and actual execution state must
all be included in any conclusion.

Reference: [Python 3.15 Tachyon](https://docs.python.org/3.15/library/profiling.sampling.html).
