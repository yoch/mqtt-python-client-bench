/*
 * mqtt_peer: the neutral party on the other side of the broker.
 *
 * The client under test is measured against this program, never against
 * another Python library, so the only Python in a run is the one being
 * ranked. Three modes, one MQTT 3.1.1 / 5 connection each:
 *
 *   sink    subscribe to --topic, count every PUBLISH, acknowledge QoS 1
 *           (PUBACK) and QoS 2 (PUBREC, then PUBCOMP), and histogram the
 *           one-way latency of stamped payloads.
 *   source  publish to --topic at --rate msgs/s (QoS 0, 1 or 2), stamping
 *           each payload; count completions (PUBACK, or PUBCOMP after PUBREL).
 *   echo    subscribe to --topic and republish every payload unchanged to
 *           --reply-topic, so the client measures its own round trip.
 *
 * With --topics N > 1 the source spreads its messages round-robin over
 * <topic>/<i/10>/<i%10>, i < N, and a sink or echo subscribes to <topic>/#.
 * With --props (MQTT 5) the source attaches a fixed set of PUBLISH
 * properties; a sink always counts the messages that carry a user property.
 * With --sizes a,b,... a sink counts payloads whose length is not listed.
 *
 * Schedule: after connecting (and subscribing) the peer prints
 * {"event":"ready"} on stdout, then reads one line from stdin:
 *
 *   GO <t_start> <t_measure> <t_end> <t_stop>
 *
 * absolute CLOCK_MONOTONIC nanoseconds, the clock Python's
 * time.monotonic_ns() reads. A source publishes over [t_start, t_end). Window
 * counters cover [t_measure, t_end); totals cover the whole run, up to
 * t_stop. One JSON object is printed on exit.
 *
 * A stamp is the first 8 payload bytes, little-endian CLOCK_MONOTONIC ns.
 * Latency buckets are log-linear, 16 per power of two (<= 6.25 % wide), with
 * values below 32 ns exact; src/mqtt_client_bench/bench/histogram.py implements the same indexing.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

enum mode { M_SINK, M_SOURCE, M_ECHO };

static enum mode g_mode;
static const char *g_host = "127.0.0.1";
static int g_port = 11883;
static const char *g_topic = "bench/t";
static const char *g_reply_topic = "bench/reply";
static const char *g_client_id = NULL;
static int g_qos = 0;
static int g_reply_qos = 0;
static int g_v5 = 0;
static int g_payload = 256;
static uint64_t g_rate = 0;
static int g_tick_us = 250;
static int g_topics = 1;
static int g_props = 0;
#define MAX_SIZES 16
static uint32_t g_sizes[MAX_SIZES];
static int g_nsizes = 0;

static uint64_t t_start, t_measure, t_end, t_stop;
static int g_fd = -1;
static char g_error[256];
/* The source's reader thread answers PUBREC with PUBREL on the same socket. */
static pthread_mutex_t g_write_lock = PTHREAD_MUTEX_INITIALIZER;

static uint64_t now_ns(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void sleep_until(uint64_t deadline)
{
	struct timespec ts = {.tv_sec = (time_t)(deadline / 1000000000ull),
			      .tv_nsec = (long)(deadline % 1000000000ull)};
	while (clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, NULL) == EINTR) {
	}
}

static void fail(const char *what)
{
	if (g_error[0] == '\0') {
		snprintf(g_error, sizeof(g_error), "%s: %s", what, errno ? strerror(errno) : "protocol");
	}
}

/* ---------------------------------------------------------------- histogram */

#define HBUCKETS 976

struct latency {
	uint64_t buckets[HBUCKETS];
	uint64_t count, min, max, sum, negative;
};

static struct latency g_lat = {.min = UINT64_MAX};

static inline int bucket_of(uint64_t v)
{
	if (v < 32) {
		return (int)v;
	}
	int e = 63 - __builtin_clzll(v);
	return (e - 3) * 16 + (int)((v >> (e - 4)) & 15);
}

static inline void lat_record(uint64_t stamp, uint64_t now)
{
	if (stamp > now) {
		g_lat.negative++;
		return;
	}
	uint64_t v = now - stamp;
	g_lat.buckets[bucket_of(v)]++;
	g_lat.count++;
	g_lat.sum += v;
	if (v < g_lat.min) {
		g_lat.min = v;
	}
	if (v > g_lat.max) {
		g_lat.max = v;
	}
}

static inline uint64_t read_stamp(const uint8_t *p)
{
	uint64_t v = 0;
	for (int i = 7; i >= 0; i--) {
		v = (v << 8) | p[i];
	}
	return v;
}

static inline void write_stamp(uint8_t *p, uint64_t v)
{
	for (int i = 0; i < 8; i++) {
		p[i] = (uint8_t)(v >> (8 * i));
	}
}

/* ------------------------------------------------------------------ socket */

static int tcp_connect(void)
{
	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		return -1;
	}
	int one = 1;
	setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
	int buf = 1 << 20;
	setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf, sizeof(buf));
	setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf));
	struct sockaddr_in addr = {.sin_family = AF_INET, .sin_port = htons((uint16_t)g_port)};
	if (inet_pton(AF_INET, g_host, &addr.sin_addr) != 1 ||
	    connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		close(fd);
		return -1;
	}
	return fd;
}

static int write_all(int fd, const uint8_t *p, size_t n)
{
	int rc = 0;
	pthread_mutex_lock(&g_write_lock);
	while (n > 0) {
		ssize_t w = send(fd, p, n, MSG_NOSIGNAL);
		if (w < 0) {
			if (errno == EINTR || errno == EAGAIN) {
				continue;
			}
			rc = -1;
			break;
		}
		p += w;
		n -= (size_t)w;
	}
	pthread_mutex_unlock(&g_write_lock);
	return rc;
}

struct wbuf {
	uint8_t *p;
	size_t len, cap;
};

static void wbuf_reserve(struct wbuf *b, size_t extra)
{
	if (b->len + extra <= b->cap) {
		return;
	}
	size_t cap = b->cap ? b->cap : 65536;
	while (cap < b->len + extra) {
		cap *= 2;
	}
	b->p = realloc(b->p, cap);
	b->cap = cap;
}

static void wbuf_put(struct wbuf *b, const void *src, size_t n)
{
	wbuf_reserve(b, n);
	memcpy(b->p + b->len, src, n);
	b->len += n;
}

static int wbuf_flush(struct wbuf *b, int fd)
{
	if (b->len == 0) {
		return 0;
	}
	int rc = write_all(fd, b->p, b->len);
	b->len = 0;
	return rc;
}

/* Buffered reader: one recv() yields every packet it carries. */
struct rbuf {
	uint8_t *p;
	size_t start, end, cap;
};

static int rbuf_fill(struct rbuf *b, int timeout_ms)
{
	if (b->start > 0) {
		memmove(b->p, b->p + b->start, b->end - b->start);
		b->end -= b->start;
		b->start = 0;
	}
	if (b->end == b->cap) {
		b->cap *= 2;
		b->p = realloc(b->p, b->cap);
	}
	struct pollfd pfd = {.fd = g_fd, .events = POLLIN};
	int pr = poll(&pfd, 1, timeout_ms);
	if (pr == 0) {
		return 0;
	}
	if (pr < 0) {
		return errno == EINTR ? 0 : -1;
	}
	ssize_t r = recv(g_fd, b->p + b->end, b->cap - b->end, 0);
	if (r <= 0) {
		if (r < 0 && (errno == EINTR || errno == EAGAIN)) {
			return 0;
		}
		return -1;
	}
	b->end += (size_t)r;
	return (int)r;
}

/* 1 and a packet when one is complete, 0 when more bytes are needed. */
static int next_packet(struct rbuf *b, uint8_t *hdr, uint8_t **body, uint32_t *len)
{
	size_t avail = b->end - b->start;
	if (avail < 2) {
		return 0;
	}
	uint8_t *p = b->p + b->start;
	uint32_t value = 0, mul = 1;
	size_t i = 1;
	for (;;) {
		if (i >= avail) {
			return 0;
		}
		uint8_t byte = p[i++];
		value += (uint32_t)(byte & 0x7f) * mul;
		if ((byte & 0x80) == 0) {
			break;
		}
		mul *= 128;
		if (i > 4) {
			return -1;
		}
	}
	if (avail < i + value) {
		while (b->cap < i + value) {
			b->cap *= 2;
			b->p = realloc(b->p, b->cap);
		}
		return 0;
	}
	*hdr = p[0];
	*body = p + i;
	*len = value;
	b->start += i + value;
	return 1;
}

static int wait_packet(struct rbuf *b, uint8_t want, uint8_t **body, uint32_t *len)
{
	uint64_t deadline = now_ns() + 10000000000ull;
	for (;;) {
		uint8_t hdr;
		int rc;
		while ((rc = next_packet(b, &hdr, body, len)) == 1) {
			if ((hdr & 0xf0) == want) {
				return 0;
			}
		}
		if (rc < 0 || now_ns() > deadline || rbuf_fill(b, 200) < 0) {
			return -1;
		}
	}
}

static size_t put_varint(uint8_t *out, uint32_t v)
{
	size_t n = 0;
	do {
		uint8_t byte = v % 128;
		v /= 128;
		if (v) {
			byte |= 0x80;
		}
		out[n++] = byte;
	} while (v);
	return n;
}

static size_t put_str(uint8_t *out, const char *s)
{
	size_t n = strlen(s);
	out[0] = (uint8_t)(n >> 8);
	out[1] = (uint8_t)n;
	memcpy(out + 2, s, n);
	return n + 2;
}

static uint32_t get_varint(const uint8_t *p, uint32_t len, uint32_t *used)
{
	uint32_t value = 0, mul = 1, i = 0;
	while (i < len && i < 4) {
		uint8_t byte = p[i++];
		value += (uint32_t)(byte & 0x7f) * mul;
		if ((byte & 0x80) == 0) {
			break;
		}
		mul *= 128;
	}
	*used = i;
	return value;
}

static int mqtt_connect(struct rbuf *rb)
{
	uint8_t vh[512];
	size_t n = 0;
	n += put_str(vh + n, "MQTT");
	vh[n++] = g_v5 ? 5 : 4;
	vh[n++] = 0x02; /* clean session / clean start */
	/* 120 s: the broker's max_keepalive, which refuses 0 on MQTT 3.1.1. A run
	 * is far shorter than the 1.5x keepalive the broker waits before
	 * dropping a silent peer, so the peer never needs to PINGREQ. */
	vh[n++] = 0;
	vh[n++] = 120;
	if (g_v5) {
		vh[n++] = 0; /* no properties */
	}
	n += put_str(vh + n, g_client_id);
	uint8_t pkt[600];
	size_t i = 0;
	pkt[i++] = 0x10;
	i += put_varint(pkt + i, (uint32_t)n);
	memcpy(pkt + i, vh, n);
	i += n;
	if (write_all(g_fd, pkt, i) < 0) {
		return -1;
	}
	uint8_t *body;
	uint32_t len;
	if (wait_packet(rb, 0x20, &body, &len) < 0 || len < 2 || body[1] != 0) {
		errno = 0;
		return -1;
	}
	return 0;
}

static int mqtt_subscribe(struct rbuf *rb, const char *topic, int qos)
{
	uint8_t vh[600];
	size_t n = 0;
	vh[n++] = 0;
	vh[n++] = 1;
	if (g_v5) {
		vh[n++] = 0;
	}
	n += put_str(vh + n, topic);
	vh[n++] = (uint8_t)qos;
	uint8_t pkt[700];
	size_t i = 0;
	pkt[i++] = 0x82;
	i += put_varint(pkt + i, (uint32_t)n);
	memcpy(pkt + i, vh, n);
	i += n;
	if (write_all(g_fd, pkt, i) < 0) {
		return -1;
	}
	uint8_t *body;
	uint32_t len;
	if (wait_packet(rb, 0x90, &body, &len) < 0 || len < 3 || body[len - 1] >= 0x80) {
		errno = 0;
		return -1;
	}
	return 0;
}

/* Pre-encoded PUBLISH with the offsets that change per message. */
struct publish_template {
	uint8_t *pkt;
	size_t len;
	size_t pid_off;   /* 0 when QoS 0 */
	size_t stamp_off; /* 0 when the payload is shorter than a stamp */
};

/* The realistic telemetry set: expiry, content type and two user properties.
 * No payload format indicator: the payload is a binary stamp, not UTF-8.
 * Shorter than 128 bytes, so its length is one varint byte. */
static size_t put_properties(uint8_t *p)
{
	size_t i = 0;
	p[i++] = 0x02;
	p[i++] = 0;
	p[i++] = 0;
	p[i++] = 0;
	p[i++] = 60;
	p[i++] = 0x03;
	i += put_str(p + i, "application/octet-stream");
	p[i++] = 0x26;
	i += put_str(p + i, "schema");
	i += put_str(p + i, "telemetry.v1");
	p[i++] = 0x26;
	i += put_str(p + i, "region");
	i += put_str(p + i, "eu-west-1");
	return i;
}

static void topic_name(char *out, size_t cap, int index)
{
	if (g_topics <= 1) {
		snprintf(out, cap, "%s", g_topic);
	} else {
		snprintf(out, cap, "%s/%d/%d", g_topic, index / 10, index % 10);
	}
}

static void build_publish(struct publish_template *t, const char *topic, int qos, size_t payload)
{
	uint8_t props[128];
	size_t props_len = g_v5 && g_props ? put_properties(props) : 0;
	size_t tlen = strlen(topic);
	uint32_t remaining =
		(uint32_t)(2 + tlen + (qos ? 2 : 0) + (g_v5 ? 1 + props_len : 0) + payload);
	t->pkt = malloc(5 + remaining);
	size_t i = 0;
	t->pkt[i++] = (uint8_t)(0x30 | (qos << 1));
	i += put_varint(t->pkt + i, remaining);
	i += put_str(t->pkt + i, topic);
	t->pid_off = 0;
	if (qos) {
		t->pid_off = i;
		t->pkt[i++] = 0;
		t->pkt[i++] = 1;
	}
	if (g_v5) {
		t->pkt[i++] = (uint8_t)props_len;
		memcpy(t->pkt + i, props, props_len);
		i += props_len;
	}
	t->stamp_off = payload >= 8 ? i : 0;
	memset(t->pkt + i, 'A', payload);
	t->len = i + payload;
}

/* ------------------------------------------------------------------ counts */

static atomic_uint_fast64_t c_sent_total, c_sent_window, c_acked_total;
static uint64_t c_recv_total, c_recv_window, c_echoed_total, c_echo_acks;
static uint64_t c_props_seen, c_size_mismatch;

static inline bool in_window(uint64_t now)
{
	return now >= t_measure && now < t_end;
}

/* True when a PUBLISH property block holds a user property (0x26). */
static bool has_user_property(const uint8_t *p, uint32_t n)
{
	uint32_t i = 0;
	while (i < n) {
		uint8_t id = p[i++];
		uint32_t used, skip;
		switch (id) {
		case 0x01:
			skip = 1;
			break;
		case 0x23:
			skip = 2;
			break;
		case 0x02:
			skip = 4;
			break;
		case 0x0B:
			get_varint(p + i, n - i, &used);
			skip = used;
			break;
		case 0x03:
		case 0x08:
		case 0x09:
			if (i + 2 > n) {
				return false;
			}
			skip = 2 + ((uint32_t)p[i] << 8 | p[i + 1]);
			break;
		case 0x26:
			return true;
		default:
			return false;
		}
		i += skip;
	}
	return false;
}

static bool size_listed(uint32_t plen)
{
	for (int i = 0; i < g_nsizes; i++) {
		if (g_sizes[i] == plen) {
			return true;
		}
	}
	return false;
}

/* Parse one inbound PUBLISH; returns the payload and its packet id. */
static int parse_publish(uint8_t hdr, uint8_t *body, uint32_t len, uint8_t **payload, uint32_t *plen,
			 uint16_t *pid)
{
	if (len < 2) {
		return -1;
	}
	uint32_t off = 2 + ((uint32_t)body[0] << 8 | body[1]);
	int qos = (hdr >> 1) & 3;
	*pid = 0;
	if (qos) {
		if (off + 2 > len) {
			return -1;
		}
		*pid = (uint16_t)(body[off] << 8 | body[off + 1]);
		off += 2;
	}
	if (g_v5) {
		uint32_t used;
		uint32_t plen_props = get_varint(body + off, len - off, &used);
		off += used;
		if (off + plen_props > len) {
			return -1;
		}
		if (plen_props && has_user_property(body + off, plen_props)) {
			c_props_seen++;
		}
		off += plen_props;
	}
	if (off > len) {
		return -1;
	}
	*payload = body + off;
	*plen = len - off;
	return qos;
}

/* PUBACK 0x40, PUBREC 0x50, PUBREL 0x62, PUBCOMP 0x70: success, no properties. */
static void put_ack(struct wbuf *out, uint8_t type, uint16_t pid)
{
	uint8_t ack[4] = {type, 0x02, (uint8_t)(pid >> 8), (uint8_t)pid};
	wbuf_put(out, ack, 4);
}

static uint16_t ack_pid(const uint8_t *body, uint32_t len)
{
	return len >= 2 ? (uint16_t)(body[0] << 8 | body[1]) : 0;
}

/* v5 PUBREC may carry a failure reason, after which no PUBREL is due. */
static bool ack_failed(const uint8_t *body, uint32_t len)
{
	return g_v5 && len >= 3 && body[2] >= 0x80;
}

/* sink and echo share one single-threaded read loop. */
static void run_receiver(struct rbuf *rb)
{
	struct wbuf out = {0};
	uint16_t next_pid = 0;
	for (;;) {
		uint64_t now = now_ns();
		if (now >= t_stop) {
			break;
		}
		uint64_t wait_ms = (t_stop - now) / 1000000ull + 1;
		int rc = rbuf_fill(rb, wait_ms > 100 ? 100 : (int)wait_ms);
		if (rc < 0) {
			fail("recv");
			break;
		}
		if (rc == 0) {
			continue;
		}
		now = now_ns();
		bool win = in_window(now);
		uint8_t hdr, *body;
		uint32_t len;
		int got;
		while ((got = next_packet(rb, &hdr, &body, &len)) == 1) {
			uint8_t type = hdr & 0xf0;
			if (type == 0x40 || type == 0x70) {
				c_echo_acks++;
				continue;
			}
			if (type == 0x50) {
				if (!ack_failed(body, len)) {
					put_ack(&out, 0x62, ack_pid(body, len));
				}
				continue;
			}
			if (type == 0x60) {
				put_ack(&out, 0x70, ack_pid(body, len));
				continue;
			}
			if (type != 0x30) {
				continue;
			}
			uint8_t *payload;
			uint32_t plen;
			uint16_t pid;
			int qos = parse_publish(hdr, body, len, &payload, &plen, &pid);
			if (qos < 0) {
				errno = 0;
				fail("malformed PUBLISH");
				continue;
			}
			c_recv_total++;
			if (qos == 1) {
				put_ack(&out, 0x40, pid);
			} else if (qos == 2) {
				put_ack(&out, 0x50, pid);
			}
			if (g_mode == M_SINK) {
				if (g_nsizes && !size_listed(plen)) {
					c_size_mismatch++;
				}
				if (win) {
					c_recv_window++;
					if (plen >= 8) {
						lat_record(read_stamp(payload), now);
					}
				}
				continue;
			}
			/* echo: same payload, reply topic, fresh packet id. */
			uint8_t head[600];
			size_t tlen = strlen(g_reply_topic);
			uint32_t remaining =
				(uint32_t)(2 + tlen + (g_reply_qos ? 2 : 0) + (g_v5 ? 1 : 0) + plen);
			size_t i = 0;
			head[i++] = (uint8_t)(0x30 | (g_reply_qos << 1));
			i += put_varint(head + i, remaining);
			i += put_str(head + i, g_reply_topic);
			if (g_reply_qos) {
				next_pid = next_pid == 65535 ? 1 : next_pid + 1;
				head[i++] = (uint8_t)(next_pid >> 8);
				head[i++] = (uint8_t)next_pid;
			}
			if (g_v5) {
				head[i++] = 0;
			}
			wbuf_put(&out, head, i);
			wbuf_put(&out, payload, plen);
			c_echoed_total++;
		}
		if (got < 0) {
			errno = 0;
			fail("malformed packet length");
			break;
		}
		if (wbuf_flush(&out, g_fd) < 0) {
			fail("send");
			break;
		}
	}
	free(out.p);
}

/* source: the reader thread counts completions and answers PUBREC. */
static void *source_reader(void *arg)
{
	struct rbuf *rb = arg;
	struct wbuf out = {0};
	while (now_ns() < t_stop) {
		int rc = rbuf_fill(rb, 100);
		if (rc < 0) {
			break;
		}
		uint8_t hdr, *body;
		uint32_t len;
		while (next_packet(rb, &hdr, &body, &len) == 1) {
			uint8_t type = hdr & 0xf0;
			if (type == 0x40 || type == 0x70) {
				atomic_fetch_add_explicit(&c_acked_total, 1, memory_order_relaxed);
			} else if (type == 0x50 && !ack_failed(body, len)) {
				put_ack(&out, 0x62, ack_pid(body, len));
			}
		}
		if (wbuf_flush(&out, g_fd) < 0) {
			break;
		}
	}
	free(out.p);
	return NULL;
}

#define MAX_BATCH_BYTES (64 * 1024)

static void run_source(struct rbuf *rb)
{
	pthread_t reader;
	pthread_create(&reader, NULL, source_reader, rb);

	int ntpl = g_topics > 1 ? g_topics : 1;
	struct publish_template *tpl = calloc((size_t)ntpl, sizeof(*tpl));
	size_t max_len = 0;
	for (int i = 0; i < ntpl; i++) {
		char name[512];
		topic_name(name, sizeof(name), i);
		build_publish(&tpl[i], name, g_qos, (size_t)g_payload);
		if (tpl[i].len > max_len) {
			max_len = tpl[i].len;
		}
	}
	size_t batch_max = MAX_BATCH_BYTES / max_len;
	if (batch_max < 1) {
		batch_max = 1;
	}
	uint8_t *batch = malloc(batch_max * max_len);
	uint64_t seq = 0;
	uint16_t pid = 0;
	uint64_t tick_ns = (uint64_t)g_tick_us * 1000ull;
	/* Credit pacing: owe rate*elapsed, but never more than 4 ticks at once,
	 * so a late wake-up is not repaid as a burst. */
	uint64_t max_due = g_rate ? (g_rate * tick_ns * 4) / 1000000000ull + 1 : batch_max;
	uint64_t sent = 0, skipped = 0;
	uint64_t next = t_start;
	sleep_until(t_start);
	for (;;) {
		uint64_t now = now_ns();
		if (now >= t_end) {
			break;
		}
		uint64_t due;
		if (g_rate) {
			uint64_t owed = (uint64_t)((__uint128_t)g_rate * (now - t_start) / 1000000000ull);
			due = owed > sent + skipped ? owed - sent - skipped : 0;
			if (due > max_due) {
				skipped += due - max_due;
				due = max_due;
			}
		} else {
			due = batch_max;
		}
		while (due > 0) {
			size_t k = due > batch_max ? batch_max : (size_t)due;
			uint64_t stamp = now_ns();
			size_t off = 0;
			for (size_t i = 0; i < k; i++) {
				const struct publish_template *t = &tpl[seq++ % (uint64_t)ntpl];
				uint8_t *pkt = batch + off;
				memcpy(pkt, t->pkt, t->len);
				if (t->pid_off) {
					pid = pid == 65535 ? 1 : pid + 1;
					pkt[t->pid_off] = (uint8_t)(pid >> 8);
					pkt[t->pid_off + 1] = (uint8_t)pid;
				}
				if (t->stamp_off) {
					write_stamp(pkt + t->stamp_off, stamp);
				}
				off += t->len;
			}
			if (write_all(g_fd, batch, off) < 0) {
				fail("send");
				goto out;
			}
			sent += k;
			atomic_fetch_add_explicit(&c_sent_total, k, memory_order_relaxed);
			if (in_window(stamp)) {
				atomic_fetch_add_explicit(&c_sent_window, k, memory_order_relaxed);
			}
			due -= k;
		}
		if (g_rate) {
			next += tick_ns;
			if (next < now) {
				next = now + tick_ns;
			}
			sleep_until(next);
		}
	}
out:
	pthread_join(reader, NULL);
	free(batch);
	for (int i = 0; i < ntpl; i++) {
		free(tpl[i].pkt);
	}
	free(tpl);
}

static void print_result(void)
{
	printf("{\"role\":\"%s\",\"protocol\":\"%s\",\"qos\":%d,\"error\":",
	       g_mode == M_SINK ? "sink" : g_mode == M_SOURCE ? "source" : "echo", g_v5 ? "MQTTv5" : "MQTTv311",
	       g_qos);
	if (g_error[0]) {
		printf("\"%s\"", g_error);
	} else {
		printf("null");
	}
	printf(",\"window_ns\":%llu,\"sent_total\":%llu,\"sent_window\":%llu,\"acked_total\":%llu,"
	       "\"received_total\":%llu,\"received_window\":%llu,\"echoed_total\":%llu,\"echo_acks\":%llu",
	       (unsigned long long)(t_end - t_measure), (unsigned long long)atomic_load(&c_sent_total),
	       (unsigned long long)atomic_load(&c_sent_window), (unsigned long long)atomic_load(&c_acked_total),
	       (unsigned long long)c_recv_total, (unsigned long long)c_recv_window,
	       (unsigned long long)c_echoed_total, (unsigned long long)c_echo_acks);
	printf(",\"props_seen\":%llu,\"size_mismatch\":%llu", (unsigned long long)c_props_seen,
	       (unsigned long long)c_size_mismatch);
	printf(",\"latency\":{\"count\":%llu,\"min_ns\":%llu,\"max_ns\":%llu,\"sum_ns\":%llu,\"negative\":%llu,"
	       "\"buckets\":[",
	       (unsigned long long)g_lat.count, (unsigned long long)(g_lat.count ? g_lat.min : 0),
	       (unsigned long long)g_lat.max, (unsigned long long)g_lat.sum, (unsigned long long)g_lat.negative);
	bool first = true;
	for (int i = 0; i < HBUCKETS; i++) {
		if (g_lat.buckets[i]) {
			printf("%s[%d,%llu]", first ? "" : ",", i, (unsigned long long)g_lat.buckets[i]);
			first = false;
		}
	}
	printf("]}}\n");
	fflush(stdout);
}

static int read_schedule(void)
{
	char line[256];
	if (!fgets(line, sizeof(line), stdin)) {
		return -1;
	}
	unsigned long long a, b, c, d;
	if (sscanf(line, "GO %llu %llu %llu %llu", &a, &b, &c, &d) == 4) {
		t_start = a, t_measure = b, t_end = c, t_stop = d;
		return 0;
	}
	/* GO_REL: offsets in ms from now, for running the peer by hand. */
	if (sscanf(line, "GO_REL %llu %llu %llu %llu", &a, &b, &c, &d) == 4) {
		uint64_t now = now_ns();
		t_start = now + a * 1000000ull, t_measure = now + b * 1000000ull;
		t_end = now + c * 1000000ull, t_stop = now + d * 1000000ull;
		return 0;
	}
	return -1;
}

static void usage(void)
{
	fprintf(stderr,
		"usage: mqtt_peer sink|source|echo [--host H] [--port P] [--topic T] [--qos Q]\n"
		"                 [--v5] [--client-id ID] [--payload B] [--rate R] [--tick-us U]\n"
		"                 [--reply-topic T] [--reply-qos Q] [--topics N] [--props]\n"
		"                 [--sizes B,B,...]\n");
}

static int parse_sizes(char *list)
{
	for (char *tok = strtok(list, ","); tok; tok = strtok(NULL, ",")) {
		if (g_nsizes == MAX_SIZES) {
			return -1;
		}
		g_sizes[g_nsizes++] = (uint32_t)strtoul(tok, NULL, 10);
	}
	return 0;
}

int main(int argc, char **argv)
{
	if (argc < 2) {
		usage();
		return 2;
	}
	if (strcmp(argv[1], "buckets") == 0) {
		/* Bucket index of each argument: pins histogram.py to this file. */
		for (int i = 2; i < argc; i++) {
			printf("%s%d", i > 2 ? " " : "", bucket_of(strtoull(argv[i], NULL, 10)));
		}
		printf("\n");
		return 0;
	}
	if (strcmp(argv[1], "sink") == 0) {
		g_mode = M_SINK;
	} else if (strcmp(argv[1], "source") == 0) {
		g_mode = M_SOURCE;
	} else if (strcmp(argv[1], "echo") == 0) {
		g_mode = M_ECHO;
	} else {
		usage();
		return 2;
	}
	static const struct option opts[] = {
		{"host", required_argument, NULL, 'h'},	      {"port", required_argument, NULL, 'p'},
		{"topic", required_argument, NULL, 't'},      {"qos", required_argument, NULL, 'q'},
		{"v5", no_argument, NULL, '5'},		      {"client-id", required_argument, NULL, 'i'},
		{"payload", required_argument, NULL, 's'},    {"rate", required_argument, NULL, 'r'},
		{"tick-us", required_argument, NULL, 'k'},    {"reply-topic", required_argument, NULL, 'R'},
		{"reply-qos", required_argument, NULL, 'Q'},  {"topics", required_argument, NULL, 'T'},
		{"props", no_argument, NULL, 'P'},	      {"sizes", required_argument, NULL, 'S'},
		{0, 0, 0, 0},
	};
	optind = 2;
	int c;
	while ((c = getopt_long(argc, argv, "", opts, NULL)) != -1) {
		switch (c) {
		case 'h': g_host = optarg; break;
		case 'p': g_port = atoi(optarg); break;
		case 't': g_topic = optarg; break;
		case 'q': g_qos = atoi(optarg); break;
		case '5': g_v5 = 1; break;
		case 'i': g_client_id = optarg; break;
		case 's': g_payload = atoi(optarg); break;
		case 'r': g_rate = strtoull(optarg, NULL, 10); break;
		case 'k': g_tick_us = atoi(optarg) < 50 ? 50 : atoi(optarg); break;
		case 'R': g_reply_topic = optarg; break;
		case 'Q': g_reply_qos = atoi(optarg); break;
		case 'T': g_topics = atoi(optarg); break;
		case 'P': g_props = 1; break;
		case 'S':
			if (parse_sizes(optarg) < 0) {
				fprintf(stderr, "at most %d sizes\n", MAX_SIZES);
				return 2;
			}
			break;
		default: usage(); return 2;
		}
	}
	if (g_qos < 0 || g_qos > 2 || g_reply_qos < 0 || g_reply_qos > 2) {
		fprintf(stderr, "QoS must be 0, 1 or 2\n");
		return 2;
	}
	if (g_topics < 1 || g_topics > 10000) {
		fprintf(stderr, "--topics must be in 1..10000\n");
		return 2;
	}
	char cid[64];
	if (g_client_id == NULL) {
		snprintf(cid, sizeof(cid), "peer-%s-%d", argv[1], (int)getpid());
		g_client_id = cid;
	}
	signal(SIGPIPE, SIG_IGN);

	struct rbuf rb = {.p = malloc(1 << 20), .cap = 1 << 20};
	g_fd = tcp_connect();
	if (g_fd < 0 || mqtt_connect(&rb) < 0) {
		fail("connect");
		print_result();
		return 1;
	}
	char filter[512];
	snprintf(filter, sizeof(filter), g_topics > 1 ? "%s/#" : "%s", g_topic);
	if (g_mode != M_SOURCE && mqtt_subscribe(&rb, filter, g_qos) < 0) {
		fail("subscribe");
		print_result();
		return 1;
	}
	printf("{\"event\":\"ready\"}\n");
	fflush(stdout);
	if (read_schedule() < 0) {
		errno = 0;
		fail("schedule");
		print_result();
		return 1;
	}
	if (g_mode == M_SOURCE) {
		run_source(&rb);
	} else {
		run_receiver(&rb);
	}
	uint8_t disc[2] = {0xe0, 0x00};
	write_all(g_fd, disc, 2);
	close(g_fd);
	print_result();
	free(rb.p);
	return g_error[0] ? 1 : 0;
}
