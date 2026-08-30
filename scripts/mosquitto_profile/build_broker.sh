#!/usr/bin/env bash
# Build two local Mosquitto 2.0.20 brokers: stock-like (-O2 + memory tracking)
# and fast (-O3 -flto, no tracking, jemalloc, no unused features).
set -euo pipefail

ROOT="${MOSQ_SRC:-/tmp/mosq-profile/mosquitto-2.0.20}"
OUT="${MOSQ_OUT:-/tmp/mosq-profile/bin}"
PATCH="${PATCH:-$(cd "$(dirname "$0")/../.." && pwd)/mosquitto/patches/0001-socket-read-ahead-buffer.patch}"
mkdir -p "$OUT"

if [[ -f "$PATCH" && -d "$ROOT/.git" ]]; then
	git -C "$ROOT" apply --check "$PATCH" 2>/dev/null && git -C "$ROOT" apply "$PATCH" || true
fi

COMMON=(
	WITH_DOCS=no
	WITH_WEBSOCKETS=no
	WITH_TLS=yes
	WITH_TLS_PSK=no
	WITH_SRV=no
	WITH_ADNS=no
	WITH_WRAP=no
	WITH_SYSTEMD=no
	WITH_STRIP=no
	WITH_SHARED_LIBRARIES=yes
	WITH_CJSON=no
	WITH_CONTROL=no
	WITH_EPOLL=yes
	prefix="${OUT}"
)

build_one() {
	local name="$1"
	shift
	local -a make_args=("$@")
	echo "=== building $name ==="
	make -C "$ROOT" clean >/dev/null
	# Broker only: skip clients/apps/plugins (cJSON, docs).
	make -C "$ROOT/lib" -j"$(nproc)" "${make_args[@]}"
	make -C "$ROOT/src" -j"$(nproc)" "${make_args[@]}"
	install -m755 "$ROOT/src/mosquitto" "$OUT/mosquitto-$name"
	echo "wrote $OUT/mosquitto-$name"
}

# Match the Docker image shape as closely as a glibc host allows.
stock_cflags='-Wall -O2 -g -fno-omit-frame-pointer'
build_one stock \
	"${COMMON[@]}" \
	WITH_MEMORY_TRACKING=yes \
	WITH_BRIDGE=yes \
	WITH_PERSISTENCE=yes \
	WITH_SYS_TREE=yes \
	WITH_JEMALLOC=no \
	"CFLAGS=${stock_cflags}"

fast_cflags='-Wall -O3 -g -fno-omit-frame-pointer -flto -march=native -mtune=native'
build_one fast \
	"${COMMON[@]}" \
	WITH_MEMORY_TRACKING=no \
	WITH_BRIDGE=no \
	WITH_PERSISTENCE=no \
	WITH_SYS_TREE=yes \
	WITH_JEMALLOC=yes \
	"CFLAGS=${fast_cflags}" \
	"LDFLAGS=-flto -ljemalloc"

ls -l "$OUT"/mosquitto-*
