# Mosquitto 2.0.20 rebuilt for the ingress ceiling.
#
# Official eclipse-mosquitto:2.0.20 is Alpine/musl, -O2, stripped, with
# libwebsockets linked in, and packet__read() issues a read(2) per MQTT
# header byte (~4 syscalls per 256-byte PUBLISH). This image is glibc,
# -O3 -flto, jemalloc, no websockets, no heap tracking, plus the
# socket read-ahead patch (see mosquitto/patches).
#
# Ranking campaigns that used the upstream image are not comparable with
# numbers produced against this binary.

FROM debian:bookworm-slim AS build

ARG MOSQUITTO_VERSION=2.0.20
ARG MOSQUITTO_SHA256=851fb4b5bf5e2ddc4c341efcfe24c877fcb3a4356c79838103a44751c5731f6a

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        libjemalloc-dev \
        libssl-dev \
        patch \
        wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
RUN wget -q -O mosquitto.tar.gz \
        "https://github.com/eclipse/mosquitto/archive/refs/tags/v${MOSQUITTO_VERSION}.tar.gz" \
    && echo "${MOSQUITTO_SHA256}  mosquitto.tar.gz" | sha256sum -c - \
    && tar -xzf mosquitto.tar.gz

COPY mosquitto/patches /build/patches
RUN patch -p1 -d "/build/mosquitto-${MOSQUITTO_VERSION}" \
        < /build/patches/0001-socket-read-ahead-buffer.patch

WORKDIR /build/mosquitto-${MOSQUITTO_VERSION}
RUN make -C lib -j"$(nproc)" \
        WITH_DOCS=no \
        WITH_WEBSOCKETS=no \
        WITH_TLS=yes \
        WITH_TLS_PSK=no \
        WITH_SRV=no \
        WITH_ADNS=no \
        WITH_WRAP=no \
        WITH_SYSTEMD=no \
        WITH_STRIP=no \
        WITH_SHARED_LIBRARIES=yes \
        WITH_CJSON=no \
        WITH_CONTROL=no \
        WITH_EPOLL=yes \
        WITH_SYS_TREE=yes \
        WITH_MEMORY_TRACKING=no \
        WITH_BRIDGE=no \
        WITH_PERSISTENCE=no \
        WITH_JEMALLOC=yes \
        CFLAGS="-Wall -O3 -flto -fno-omit-frame-pointer" \
        LDFLAGS="-flto -ljemalloc" \
    && make -C src -j"$(nproc)" \
        WITH_DOCS=no \
        WITH_WEBSOCKETS=no \
        WITH_TLS=yes \
        WITH_TLS_PSK=no \
        WITH_SRV=no \
        WITH_ADNS=no \
        WITH_WRAP=no \
        WITH_SYSTEMD=no \
        WITH_STRIP=no \
        WITH_SHARED_LIBRARIES=yes \
        WITH_CJSON=no \
        WITH_CONTROL=no \
        WITH_EPOLL=yes \
        WITH_SYS_TREE=yes \
        WITH_MEMORY_TRACKING=no \
        WITH_BRIDGE=no \
        WITH_PERSISTENCE=no \
        WITH_JEMALLOC=yes \
        CFLAGS="-Wall -O3 -flto -fno-omit-frame-pointer" \
        LDFLAGS="-flto -ljemalloc"

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libjemalloc2 \
        libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r -g 1883 mosquitto \
    && useradd -r -u 1883 -g mosquitto -d /var/empty -s /usr/sbin/nologin mosquitto \
    && mkdir -p /mosquitto/config /mosquitto/data /mosquitto/log \
    && chown -R mosquitto:mosquitto /mosquitto

COPY --from=build /build/mosquitto-2.0.20/src/mosquitto /usr/sbin/mosquitto
COPY mosquitto/mosquitto.conf /mosquitto/config/mosquitto.conf

USER mosquitto
EXPOSE 11883 11884
ENTRYPOINT ["/usr/sbin/mosquitto"]
CMD ["-c", "/mosquitto/config/mosquitto.conf"]
