# Python builds and reproducible campaigns

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
They also refuse mixed actual GIL states within one client/scenario's runs;
different native clients may legitimately activate the GIL differently.
An unrelated historical or diagnostic campaign cannot block a selected report,
but the selected campaign still undergoes all runtime checks.

The manifest is saved atomically before any result, including an unsupported-only
campaign. Existing results without a manifest are refused because their original
runtime and settings cannot be verified; preserve them and start a new campaign.

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
