"""Failure-recovery test against the docker-compose cluster (run in CI).

    docker compose up -d --build && python scripts/e2e.py

0. Concurrent writes through all three nodes at once: none may fail and no
   hints may appear (every node is up, so a hint means a spurious timeout).
1. 100 writes through n1, all readable through n3.
2. Stop n3; 50 more writes still succeed (W=2) and are readable.
3. Restart n3 (same volume): it recovers its log from disk, then hinted
   handoff / anti-entropy bring it the 50 writes it missed.
4. Stop n2 and n3: writes fail with 503 (no quorum) instead of pretending.
"""
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

URL = {"n1": "http://127.0.0.1:8001", "n2": "http://127.0.0.1:8002", "n3": "http://127.0.0.1:8003"}


def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True)


def wait_healthy(node, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        try:
            return httpx.get(URL[node] + "/health", timeout=1).json()
        except httpx.HTTPError:
            time.sleep(1)
    sys.exit(f"{node} did not become healthy")


def step(msg):
    print(f"\n== {msg}", flush=True)


for n in URL:
    wait_healthy(n)

step("concurrent writers: 3 coordinators x 4 threads x 20 writes")
CONCURRENT = 0


def writer(args):
    node, worker = args
    failures = 0
    for i in range(20):
        r = httpx.put(f"{URL[node]}/kv/c-{node}-{worker}-{i}", content=b"x", timeout=10)
        failures += r.status_code != 200
    return failures


t = time.perf_counter()
with ThreadPoolExecutor(max_workers=12) as pool:
    failed = sum(pool.map(writer, [(n, w) for n in URL for w in range(4)]))
CONCURRENT = 3 * 4 * 20
hints = {n: httpx.get(URL[n] + "/health").json()["pending_hints"] for n in URL}
print(f"{CONCURRENT} writes in {time.perf_counter() - t:.1f}s, failed={failed}, hints={hints}")
assert failed == 0 and not any(hints.values()), "spurious failures with every node up"

step("100 writes via n1, read back via n3")
for i in range(100):
    httpx.put(f"{URL['n1']}/kv/key{i}", content=f"v{i}".encode()).raise_for_status()
assert all(httpx.get(f"{URL['n3']}/kv/key{i}").content == f"v{i}".encode() for i in range(100))
print("ok")

step("stop n3, 50 more writes")
compose("stop", "n3")
t = time.perf_counter()
for i in range(100, 150):
    httpx.put(f"{URL['n1']}/kv/key{i}", content=f"v{i}".encode(), timeout=5).raise_for_status()
print(f"ok: 50 writes with one replica down in {time.perf_counter() - t:.1f}s")
print("n1 pending hints:", httpx.get(URL["n1"] + "/health").json()["pending_hints"])
assert httpx.get(f"{URL['n2']}/kv/key120").content == b"v120"

step("restart n3; it must catch up")
compose("start", "n3")
health = wait_healthy("n3")
print("n3 keys right after restart (recovered from its log):", health["keys"])
assert health["keys"] >= 100 + CONCURRENT
deadline = time.time() + 60
while time.time() < deadline and httpx.get(URL["n3"] + "/health").json()["keys"] < 150 + CONCURRENT:
    time.sleep(1)
health = httpx.get(URL["n3"] + "/health").json()
print("n3 after repair:", health)
assert health["keys"] == 150 + CONCURRENT, health

step("stop n2 and n3: writes must fail with 503")
compose("stop", "n2", "n3")
r = httpx.put(f"{URL['n1']}/kv/no-quorum", content=b"x", timeout=10)
print(r.status_code, r.text)
assert r.status_code == 503
compose("start", "n2", "n3")
print("\nall end-to-end checks passed")
