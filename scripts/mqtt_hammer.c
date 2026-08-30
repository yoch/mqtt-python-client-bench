/*
 * In-tree MQTT 3.1.1 QoS0 pub/sub load generator for this harness.
 *
 * Written for mqtt-python-client-bench (PR #4). Not emqtt-bench, not
 * mosquitto_pub: TCP_NODELAY, and an aggregate --rate pacer that sleeps once
 * per tick and sends what the tick owes, rather than spinning per message.
 *
 * Usage:
 *   mqtt_hammer sub  --host 127.0.0.1 --port 11883 --topic t --duration 12
 *   mqtt_hammer pub  --host 127.0.0.1 --port 11883 --topic t --clients 2 \
 *                    --payload 256 --duration 12 --rate 200000
 *
 * Prints one JSON object on stdout at the end; sub also prints per-second
 * recv rates on stderr. A pub count is a completed write(2) of one MQTT
 * PUBLISH packet — confirm against $SYS received/sent and a subscriber.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
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

static const char *g_host = "127.0.0.1";
static int g_port = 11883;
static const char *g_topic = "bench/ceiling";
static int g_clients = 32;
static int g_payload = 256;
static int g_duration_s = 12;
static int g_interval_us = 0;
/* Aggregate msgs/s cap across all pub threads. 0 = unlimited (firehose). */
static int g_rate = 0;
static atomic_uint_fast64_t g_next_ns;
static volatile sig_atomic_t g_stop = 0;

static void on_sig(int sig)
{
	(void)sig;
	g_stop = 1;
}

static uint64_t nsec_now(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static int set_sockopts(int fd)
{
	int one = 1;
	setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
	int buf = 1 << 20;
	setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf, sizeof(buf));
	setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf));
	return 0;
}

static int tcp_connect(void)
{
	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		return -1;
	}
	set_sockopts(fd);
	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons((uint16_t)g_port);
	if (inet_pton(AF_INET, g_host, &addr.sin_addr) != 1) {
		close(fd);
		return -1;
	}
	if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		close(fd);
		return -1;
	}
	return fd;
}

static int write_all(int fd, const uint8_t *buf, size_t n)
{
	size_t off = 0;
	while (off < n) {
		if (g_stop) {
			errno = EINTR;
			return -1;
		}
		ssize_t w = send(fd, buf + off, n - off, MSG_NOSIGNAL);
		if (w < 0) {
			if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
				continue;
			}
			return -1;
		}
		if (w == 0) {
			return -1;
		}
		off += (size_t)w;
	}
	return 0;
}

static int read_all(int fd, uint8_t *buf, size_t n)
{
	size_t off = 0;
	while (off < n) {
		if (g_stop) {
			errno = EINTR;
			return -1;
		}
		ssize_t r = recv(fd, buf + off, n - off, 0);
		if (r < 0) {
			if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
				continue;
			}
			return -1;
		}
		if (r == 0) {
			return -1;
		}
		off += (size_t)r;
	}
	return 0;
}

static int encode_rl(uint32_t len, uint8_t *out)
{
	int n = 0;
	do {
		uint8_t b = (uint8_t)(len % 128);
		len /= 128;
		if (len) {
			b |= 0x80;
		}
		out[n++] = b;
	} while (len);
	return n;
}

static int mqtt_connect(int fd, const char *client_id)
{
	size_t id_len = strlen(client_id);
	uint8_t vh[10] = {0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3c};
	uint32_t remaining = (uint32_t)(sizeof(vh) + 2 + id_len);
	uint8_t pkt[512];
	size_t i = 0;
	pkt[i++] = 0x10;
	i += (size_t)encode_rl(remaining, pkt + i);
	memcpy(pkt + i, vh, sizeof(vh));
	i += sizeof(vh);
	pkt[i++] = (uint8_t)((id_len >> 8) & 0xff);
	pkt[i++] = (uint8_t)(id_len & 0xff);
	memcpy(pkt + i, client_id, id_len);
	i += id_len;
	if (write_all(fd, pkt, i) < 0) {
		return -1;
	}
	uint8_t ack[4];
	if (read_all(fd, ack, 4) < 0) {
		return -1;
	}
	if (ack[0] != 0x20 || ack[3] != 0x00) {
		return -1;
	}
	return 0;
}

static int mqtt_subscribe(int fd, const char *topic)
{
	size_t tlen = strlen(topic);
	uint32_t remaining = (uint32_t)(2 + 2 + tlen + 1);
	uint8_t pkt[512];
	size_t i = 0;
	pkt[i++] = 0x82;
	i += (size_t)encode_rl(remaining, pkt + i);
	pkt[i++] = 0x00;
	pkt[i++] = 0x01;
	pkt[i++] = (uint8_t)((tlen >> 8) & 0xff);
	pkt[i++] = (uint8_t)(tlen & 0xff);
	memcpy(pkt + i, topic, tlen);
	i += tlen;
	pkt[i++] = 0x00;
	if (write_all(fd, pkt, i) < 0) {
		return -1;
	}
	uint8_t hdr[2];
	if (read_all(fd, hdr, 2) < 0) {
		return -1;
	}
	if ((hdr[0] & 0xf0) != 0x90) {
		return -1;
	}
	uint32_t rl = 0;
	unsigned mul = 1;
	uint8_t b = hdr[1];
	int extra = 0;
	rl = (uint32_t)(b & 0x7f);
	while (b & 0x80) {
		if (read_all(fd, &b, 1) < 0) {
			return -1;
		}
		extra++;
		rl += (uint32_t)(b & 0x7f) * mul * 128;
		mul *= 128;
	}
	(void)extra;
	uint8_t rest[8];
	if (rl > sizeof(rest)) {
		return -1;
	}
	if (read_all(fd, rest, rl) < 0) {
		return -1;
	}
	return 0;
}

static atomic_uint_fast64_t g_pub_ok;
static atomic_uint_fast64_t g_sub_ok;

/* Aggregate rate pacing, one sleep per tick instead of one spin per message.
 *
 * The original pacer busy-waited on a shared slot, once per message, because a
 * 5 µs period is below what nanosleep can hold. The conclusion did not follow
 * from the premise: nothing requires sleeping *per message*. Sleeping per tick
 * and sending the messages the tick owes costs one syscall per tick, and a
 * 250 µs tick is well within what clock_nanosleep holds.
 *
 * What the spin cost, measured on the reference host with the publisher pinned
 * to one physical core: 200% CPU — both hyperthreads saturated — at every
 * requested rate, including 100k msgs/s where it delivered exactly 100k. About
 * nine tenths of the core group went into the wait loop rather than into
 * publishing. The visible symptom was that asking for less delivered less than
 * asking for more: 200k requested came back 186k while 300k requested came back
 * 230k, because the slower the target, the more the pacer spun, on the very
 * cores it was competing with.
 *
 * Each thread owns a share of the aggregate rate and its own absolute deadline
 * schedule, so there is no shared atomic on the hot path at all.
 *
 * No catch-up burst, which the spin pacer was careful about and this keeps: a
 * thread that wakes late owes one tick, never the pile it slept through.
 * Otherwise a scheduling hiccup would be repaid as a burst, and a burst is
 * precisely what an offer must not contain.
 */
static int g_tick_us = 250;

/* Largest single write the pacer will issue. Sized to stay inside a normal
 * socket buffer so a batch does not block half-written. */
#define MAX_BATCH_BYTES (64 * 1024)

struct pacer {
	uint64_t next_ns;   /* absolute deadline of the current tick */
	uint64_t tick_ns;
	uint64_t per_tick;  /* whole messages this thread owes each tick */
	uint64_t rem_num;   /* fractional remainder, carried exactly */
	uint64_t rem_den;
	uint64_t rem_acc;
};

static void pacer_init(struct pacer *p, uint64_t thread_rate)
{
	p->tick_ns = (uint64_t)g_tick_us * 1000ull;
	p->rem_den = 1000000000ull / p->tick_ns; /* ticks per second */
	if (p->rem_den == 0) {
		p->rem_den = 1;
	}
	p->per_tick = thread_rate / p->rem_den;
	p->rem_num = thread_rate % p->rem_den;
	p->rem_acc = 0;
	p->next_ns = nsec_now() + p->tick_ns;
}

/* Sleep until this thread's next tick; return how many messages it owes.
 *
 * The fractional part is carried rather than rounded, so a rate that is not a
 * whole number of messages per tick still comes out exact over a second — at
 * 200k msgs/s across 3 threads that is 16.67 per tick, and rounding either way
 * would miss the target by 2%. */
static uint64_t pacer_wait(struct pacer *p)
{
	struct timespec ts;
	ts.tv_sec = (time_t)(p->next_ns / 1000000000ull);
	ts.tv_nsec = (long)(p->next_ns % 1000000000ull);
	while (clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, NULL) == EINTR) {
		if (g_stop) {
			return 0;
		}
	}
	uint64_t now = nsec_now();
	p->next_ns += p->tick_ns;
	if (p->next_ns < now) {
		/* Woke late. Resynchronise instead of owing the missed ticks. */
		p->next_ns = now + p->tick_ns;
	}
	uint64_t due = p->per_tick;
	p->rem_acc += p->rem_num;
	if (p->rem_acc >= p->rem_den) {
		p->rem_acc -= p->rem_den;
		due++;
	}
	return due;
}

static void wait_interval(uint64_t *next_ns, uint64_t period_ns)
{
	if (period_ns == 0) {
		return;
	}
	*next_ns += period_ns;
	uint64_t now = nsec_now();
	if (*next_ns < now) {
		*next_ns = now;
	}
	while (!g_stop && nsec_now() < *next_ns) {
	}
}

struct pub_arg {
	int id;
	const uint8_t *pkt;
	size_t pkt_len;
};

static void *pub_thread(void *raw)
{
	struct pub_arg *arg = raw;
	char cid[64];
	snprintf(cid, sizeof(cid), "hpub-%04d", arg->id);
	int fd = tcp_connect();
	if (fd < 0 || mqtt_connect(fd, cid) < 0) {
		fprintf(stderr, "pub %d connect failed: %s\n", arg->id, strerror(errno));
		return NULL;
	}
	const uint8_t *pkt = arg->pkt;
	size_t n = arg->pkt_len;
	uint64_t period_ns = g_rate > 0 ? 0ull : (uint64_t)g_interval_us * 1000ull;
	uint64_t next_ns = nsec_now();

	if (g_rate > 0) {
		/* Split the aggregate rate across threads, giving the remainder to
		 * the low-numbered ones so the total is exact rather than short. */
		uint64_t rate = (uint64_t)g_rate;
		uint64_t clients = (uint64_t)(g_clients > 0 ? g_clients : 1);
		uint64_t share = rate / clients;
		if ((uint64_t)arg->id < rate % clients) {
			share++;
		}
		struct pacer pacer;
		pacer_init(&pacer, share);

		/* Coalesce a tick's packets into one write.
		 *
		 * Pacing per tick removed the spin; this removes the syscall that
		 * replaced it as the limit. The packets a tick owes are identical
		 * and consecutive, so they can go out in a single send() instead of
		 * one per message — the broker reads the same bytes either way, and
		 * TCP would have coalesced them in the socket buffer regardless.
		 *
		 * The buffer is capped rather than sized from the tick: a large rate
		 * with a large tick would otherwise allocate megabytes, and a write
		 * that large stops fitting the socket buffer and blocks mid-way,
		 * which is the back-pressure this pacer exists to avoid. Anything
		 * above the cap goes out as several full writes. */
		size_t batch_max = MAX_BATCH_BYTES / n;
		if (batch_max < 1) {
			batch_max = 1;
		}
		uint8_t *batch = malloc(batch_max * n);
		if (batch == NULL) {
			fprintf(stderr, "pub %d: out of memory for batch buffer\n", arg->id);
			goto done;
		}
		for (size_t i = 0; i < batch_max; i++) {
			memcpy(batch + i * n, pkt, n);
		}
		while (!g_stop) {
			uint64_t due = pacer_wait(&pacer);
			while (due > 0 && !g_stop) {
				size_t k = due > batch_max ? batch_max : (size_t)due;
				if (write_all(fd, batch, k * n) < 0) {
					free(batch);
					goto done;
				}
				atomic_fetch_add_explicit(&g_pub_ok, (uint_fast64_t)k,
							  memory_order_relaxed);
				due -= k;
			}
		}
		free(batch);
		goto done;
	}

	while (!g_stop) {
		if (write_all(fd, pkt, n) < 0) {
			break;
		}
		atomic_fetch_add_explicit(&g_pub_ok, 1, memory_order_relaxed);
		wait_interval(&next_ns, period_ns);
	}
done:;
	close(fd);
	return NULL;
}

static int decode_rl_stream(int fd, uint32_t *out)
{
	uint32_t value = 0;
	unsigned mul = 1;
	for (int i = 0; i < 4; i++) {
		uint8_t b;
		if (read_all(fd, &b, 1) < 0) {
			return -1;
		}
		value += (uint32_t)(b & 0x7f) * mul;
		if ((b & 0x80) == 0) {
			*out = value;
			return 0;
		}
		mul *= 128;
	}
	return -1;
}

static int run_sub(void)
{
	int fd = tcp_connect();
	if (fd < 0 || mqtt_connect(fd, "hsub-0000") < 0 || mqtt_subscribe(fd, g_topic) < 0) {
		fprintf(stderr, "sub connect/subscribe failed: %s\n", strerror(errno));
		return 1;
	}
	struct timeval tv = {.tv_sec = 1, .tv_usec = 0};
	setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	uint64_t t0 = nsec_now();
	uint64_t last = t0;
	uint64_t last_count = 0;
	uint64_t end = t0 + (uint64_t)g_duration_s * 1000000000ull;
	uint8_t scratch[65536];
	while (!g_stop && nsec_now() < end) {
		uint8_t hdr;
		int rc = read_all(fd, &hdr, 1);
		if (rc < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
				uint64_t now = nsec_now();
				if (now - last >= 1000000000ull) {
					uint64_t c = atomic_load(&g_sub_ok);
					fprintf(stderr, "recv_rate=%.0f\n",
						(double)(c - last_count) * 1e9 / (double)(now - last));
					last = now;
					last_count = c;
				}
				continue;
			}
			break;
		}
		uint32_t rl = 0;
		if (decode_rl_stream(fd, &rl) < 0) {
			break;
		}
		if (rl > sizeof(scratch)) {
			break;
		}
		if (read_all(fd, scratch, rl) < 0) {
			break;
		}
		if ((hdr & 0xf0) == 0x30) {
			atomic_fetch_add_explicit(&g_sub_ok, 1, memory_order_relaxed);
		}
		uint64_t now = nsec_now();
		if (now - last >= 1000000000ull) {
			uint64_t c = atomic_load(&g_sub_ok);
			fprintf(stderr, "recv_rate=%.0f\n", (double)(c - last_count) * 1e9 / (double)(now - last));
			last = now;
			last_count = c;
		}
	}
	uint64_t t1 = nsec_now();
	double secs = (double)(t1 - t0) / 1e9;
	uint64_t count = atomic_load(&g_sub_ok);
	printf("{\"role\":\"sub\",\"msgs\":%llu,\"seconds\":%.3f,\"msgs_per_s\":%.1f}\n",
		(unsigned long long)count, secs, count / secs);
	close(fd);
	return 0;
}

static int run_pub(void)
{
	size_t tlen = strlen(g_topic);
	uint32_t remaining = (uint32_t)(2 + tlen + (uint32_t)g_payload);
	uint8_t *pkt = malloc(5 + remaining);
	if (!pkt) {
		return 1;
	}
	size_t i = 0;
	pkt[i++] = 0x30;
	i += (size_t)encode_rl(remaining, pkt + i);
	pkt[i++] = (uint8_t)((tlen >> 8) & 0xff);
	pkt[i++] = (uint8_t)(tlen & 0xff);
	memcpy(pkt + i, g_topic, tlen);
	i += tlen;
	memset(pkt + i, 'A', (size_t)g_payload);
	i += (size_t)g_payload;

	pthread_t *ths = calloc((size_t)g_clients, sizeof(pthread_t));
	struct pub_arg *args = calloc((size_t)g_clients, sizeof(struct pub_arg));
	if (!ths || !args) {
		return 1;
	}
	atomic_store_explicit(&g_next_ns, nsec_now(), memory_order_relaxed);
	for (int c = 0; c < g_clients; c++) {
		args[c].id = c;
		args[c].pkt = pkt;
		args[c].pkt_len = i;
		if (pthread_create(&ths[c], NULL, pub_thread, &args[c]) != 0) {
			fprintf(stderr, "pthread_create failed\n");
			g_stop = 1;
			break;
		}
	}
	uint64_t t0 = nsec_now();
	struct timespec sl = {.tv_sec = g_duration_s, .tv_nsec = 0};
	nanosleep(&sl, NULL);
	g_stop = 1;
	for (int c = 0; c < g_clients; c++) {
		if (ths[c]) {
			pthread_join(ths[c], NULL);
		}
	}
	uint64_t t1 = nsec_now();
	double secs = (double)(t1 - t0) / 1e9;
	uint64_t count = atomic_load(&g_pub_ok);
	printf("{\"role\":\"pub\",\"clients\":%d,\"rate_limit\":%d,\"msgs\":%llu,\"seconds\":%.3f,\"msgs_per_s\":%.1f}\n",
		g_clients, g_rate, (unsigned long long)count, secs, count / secs);
	free(pkt);
	free(ths);
	free(args);
	return 0;
}

static void usage(const char *argv0)
{
	fprintf(stderr,
		"Usage: %s pub|sub [--host H] [--port P] [--topic T] [--clients N]\n"
		"                 [--payload B] [--duration S] [--rate R] [--interval-us U]\n"
		"  --rate R         aggregate QoS0 pubs/s cap (0 = unlimited)\n"
		"  --tick-us U      pacer tick in us (default 250); larger is cheaper,\n"
		"                   smaller spreads arrivals more evenly\n"
		"  --interval-us U  per-thread period when --rate is unset (busy-wait)\n",
		argv0);
}

int main(int argc, char **argv)
{
	if (argc < 2) {
		usage(argv[0]);
		return 2;
	}
	const char *mode = argv[1];
	static const struct option opts[] = {
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"topic", required_argument, NULL, 't'},
		{"clients", required_argument, NULL, 'c'},
		{"payload", required_argument, NULL, 's'},
		{"duration", required_argument, NULL, 'd'},
		{"interval-us", required_argument, NULL, 'i'},
		{"rate", required_argument, NULL, 'r'},
		{"tick-us", required_argument, NULL, 'k'},
		{0, 0, 0, 0},
	};
	optind = 2;
	int c;
	while ((c = getopt_long(argc, argv, "", opts, NULL)) != -1) {
		switch (c) {
		case 'h':
			g_host = optarg;
			break;
		case 'p':
			g_port = atoi(optarg);
			break;
		case 't':
			g_topic = optarg;
			break;
		case 'c':
			g_clients = atoi(optarg);
			break;
		case 's':
			g_payload = atoi(optarg);
			break;
		case 'd':
			g_duration_s = atoi(optarg);
			break;
		case 'i':
			g_interval_us = atoi(optarg);
			break;
		case 'r':
			g_rate = atoi(optarg);
			if (g_rate < 0) {
				g_rate = 0;
			}
			break;
		case 'k':
			g_tick_us = atoi(optarg);
			if (g_tick_us < 10) {
				/* Below this the sleep granularity dominates and the
				 * pacer stops being cheaper than the spin it replaced. */
				g_tick_us = 10;
			}
			break;
		default:
			usage(argv[0]);
			return 2;
		}
	}
	signal(SIGPIPE, SIG_IGN);
	signal(SIGINT, on_sig);
	signal(SIGTERM, on_sig);
	if (strcmp(mode, "sub") == 0) {
		return run_sub();
	}
	if (strcmp(mode, "pub") == 0) {
		return run_pub();
	}
	usage(argv[0]);
	return 2;
}
