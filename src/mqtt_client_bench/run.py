"""``python -m mqtt_client_bench.run`` and the ``mqtt-client-bench`` script."""

import sys

from mqtt_client_bench.bench.cli import main

if __name__ == "__main__":
    sys.exit(main())
