"""Throughput / latency of the storage engine and of a 3-node in-process cluster.

Numbers depend on the machine; CI runs this on a GitHub-hosted runner and
prints the table into the job summary. In-process means no network: it
measures the engine and replication logic, not HTTP.

    python scripts/bench.py [ops]
"""
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dkv.node import Config, Node  # noqa: E402
from dkv.ring import HashRing  # noqa: E402
from dkv.storage import LogStore, Record, Version  # noqa: E402
from dkv.transport import LocalTransport  # noqa: E402

OPS = int(sys.argv[1]) if len(sys.argv) > 1 else 20_000
VALUE = b"x" * 100


def measure(fn, n):
    lat = []
    start = time.perf_counter()
    for i in range(n):
        t = time.perf_counter()
        fn(i)
        lat.append(time.perf_counter() - t)
    total = time.perf_counter() - start
    lat.sort()
    return n / total, statistics.median(lat) * 1e6, lat[int(0.99 * len(lat))] * 1e6


rows = []
with tempfile.TemporaryDirectory() as d:
    for fsync in (False, True):
        n = OPS if not fsync else min(OPS, 2000)
        with LogStore(Path(d) / f"s{fsync}", fsync=fsync) as s:
            rows.append((f"LogStore put (fsync={fsync})", n,
                         *measure(lambda i: s.apply(Record(f"k{i}".encode(), VALUE, Version(i + 1))), n)))
            if not fsync:
                rows.append(("LogStore get", n, *measure(lambda i: s.get(f"k{i}".encode()), n)))
                t = time.perf_counter()
                s.close()
                s2 = LogStore(Path(d) / f"s{fsync}")
                rows.append((f"Recovery: reopen {n} keys", 1, 1 / (time.perf_counter() - t), 0, 0))
                s2.close()

    net, ring = LocalTransport(), HashRing()
    nodes = []
    for nid in ("n1", "n2", "n3"):
        ring.add(nid)
    for nid in ("n1", "n2", "n3"):
        node = Node(nid, Path(d) / nid, ring, net.for_node(nid), Config(3, 2, 2))
        net.register(node)
        nodes.append(node)
    c = nodes[0]
    rows.append(("Cluster put (N=3, W=2)", OPS // 4, *measure(lambda i: c.put(f"k{i}".encode(), VALUE), OPS // 4)))
    rows.append(("Cluster get (N=3, R=2)", OPS // 4, *measure(lambda i: c.get(f"k{i}".encode()), OPS // 4)))
    for node in nodes:
        node.close()

print(f"| Operation | Ops | Ops/s | p50 µs | p99 µs |\n|---|---|---|---|---|")
for name, n, ops, p50, p99 in rows:
    if name.startswith("Recovery"):
        print(f"| {name} | {n} | {1 / ops * 1000:.0f} ms total | | |")
    else:
        print(f"| {name} | {n} | {ops:,.0f} | {p50:.0f} | {p99:.0f} |")
print(f"\n100-byte values. Python {sys.version.split()[0]}, single thread, in-process (no network).")
