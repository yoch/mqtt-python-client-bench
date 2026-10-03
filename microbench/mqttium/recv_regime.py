"""Cost of asyncio's ``sock.recv(256 KiB)`` per small message, by glibc malloc regime.

asyncio's selector transport reads with ``sock.recv(max_size)`` where
``max_size`` is 256 KiB. CPython allocates the full ``bytes`` object, then
shrinks it to the received length. Above glibc's mmap threshold (128 KiB at
start) the allocation is an ``mmap`` and the shrink unmaps it, so every read
pays mmap + munmap + page faults. The threshold only rises when ``free()``
releases a larger mmapped block, so one large read (or any other large free)
switches the process to heap allocations for good.

The script measures small reads in a fresh process, after one full 256 KiB
read, and with ``recv_into`` into a preallocated buffer (mqttium's push path).
"""

import resource
import socket

READ_SIZE = 256 * 1024
MESSAGE = b"x" * 300
N = 20000


def measure(label, a, b, read):
    ru0 = resource.getrusage(resource.RUSAGE_SELF)
    for _ in range(N):
        a.send(MESSAGE)
        read(b)
    ru1 = resource.getrusage(resource.RUSAGE_SELF)
    print("%-44s minflt/msg=%5.2f  sys_us/msg=%6.2f  user_us/msg=%6.2f" % (
        label,
        (ru1.ru_minflt - ru0.ru_minflt) / N,
        (ru1.ru_stime - ru0.ru_stime) * 1e6 / N,
        (ru1.ru_utime - ru0.ru_utime) * 1e6 / N,
    ))


def drain(sock):
    sock.setblocking(False)
    try:
        while sock.recv(READ_SIZE):
            pass
    except BlockingIOError:
        pass
    sock.setblocking(True)


def main():
    a, b = socket.socketpair()
    a.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 21)
    b.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 21)
    buf = bytearray(READ_SIZE)
    measure("recv_into preallocated buffer", a, b, lambda s: s.recv_into(buf))
    measure("recv(256 KiB), fresh process", a, b, lambda s: s.recv(READ_SIZE))
    a.sendall(b"y" * (2 * READ_SIZE))
    full = len(b.recv(READ_SIZE))
    drain(b)
    measure("recv(256 KiB), after one %d-byte read" % full, a, b, lambda s: s.recv(READ_SIZE))


if __name__ == "__main__":
    main()
