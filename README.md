# DistributedKV

A fault-tolerant, leaderless distributed key-value store in Python, built from first principles: a log-structured storage engine, consistent hashing, quorum replication, hinted handoff, read repair and Merkle-style anti-entropy. The design follows Amazon's [Dynamo paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) (DeCandia et al., 2007) and Bitcask.

Every failure mode below is covered by a test that injects it: node crashes, torn writes, corrupted logs, network partitions, clock skew, and lost hints.

## Architecture

```mermaid
flowchart LR
    C[Client] -->|PUT /kv/key| CO
    subgraph Cluster["Cluster (N=3, R=2, W=2)"]
        CO["Coordinator<br/>(any node)"]
        CO -->|replicate| A[Replica A]
        CO -->|replicate| B[Replica B]
        CO -. "down: store hint" .-> H[(Hint log)]
        H -. "replay when back" .-> X[Replica C]
        A <-->|"anti-entropy<br/>(bucket digests)"| X
    end
```

Inside each node:

```mermaid
flowchart TB
    API["FastAPI server<br/>/kv/* client API · /internal/* replication API"] --> N["Node<br/>coordinator + replica logic"]
    N --> R["HashRing<br/>128 virtual nodes per node"]
    N --> S["LogStore<br/>append-only segments + in-memory keydir"]
    N --> HS["Hint stores<br/>one LogStore per unreachable peer"]
    N --> CL["Hybrid logical clock"]
    N --> T["Transport<br/>HTTP in production · in-process with fault injection in tests"]
```

| Module | What it does |
|---|---|
| [`dkv/storage.py`](dkv/storage.py) | Bitcask-style engine. Writes append CRC32-checked records to the active segment; an in-memory index maps each key to its latest record's offset, so a read is one seek. Deletes are tombstones. Segments roll at 4 MiB; `compact()` rewrites only the latest records. |
| [`dkv/clock.py`](dkv/clock.py) | Hybrid logical clock: 48-bit milliseconds + 16-bit counter. Never goes backwards, and advances past every version it observes. |
| [`dkv/ring.py`](dkv/ring.py) | Consistent hashing (BLAKE2b, 64-bit ring) with virtual nodes. The *preference list* is the next N distinct physical nodes clockwise. |
| [`dkv/node.py`](dkv/node.py) | Leaderless replication. Any node coordinates: writes to N replicas and waits for W acks; reads from N, needs R answers, returns the newest version and repairs stale replicas. Hinted handoff and anti-entropy. |
| [`dkv/transport.py`](dkv/transport.py) | The only way nodes talk. `LocalTransport` simulates crashes and partitions in-process. |
| [`dkv/server.py`](dkv/server.py) | HTTP API and the background repair thread (hint delivery + anti-entropy with a random peer every few seconds). |

## Design decisions

**Last-writer-wins with hybrid logical clocks, not vector clocks.** Dynamo returns conflicting siblings and makes the client merge them. That is the right call for a shopping cart but pushes complexity onto every caller. Here each write gets a version `(hlc_timestamp, node_id)` that totally orders writes, and replicas keep the highest. Concurrent writes to the same key resolve deterministically: one wins and the other is lost. That is the documented trade-off.

**Re-stamping makes "later" really mean later.** Wall clocks on different machines disagree. With plain LWW, a coordinator whose clock is behind could issue a version lower than a write that has *already completed*, and the newer write would silently lose. A replica that rejects a write as stale therefore replies with the version it holds; the coordinator advances its clock past it and re-stamps. Because R + W > N, any later write reaches at least one replica that has the earlier one. The randomised test runs with node clocks up to 2 seconds apart. If replicas don't report newer versions back, that test fails.

**Strict quorums plus durable hints.** A write needs W acks from the key's real replicas. Writes to unreachable replicas are stored as *hints*. A hint store is itself a `LogStore`, so it survives a coordinator restart, and it is replayed when the replica answers again. Hints don't count toward W (Dynamo's *sloppy* quorum does count them; that improves availability but weakens the R + W > N overlap).

**No rollback on a failed write.** If only 1 of W replicas acks, the client gets `503`, but that replica keeps the write, as in Dynamo and Cassandra. A later successful write has a higher version and wins. The partition test checks exactly this.

**Crash recovery that refuses to guess.** A torn record at the tail of the newest segment is what a crash mid-write looks like: it is truncated on startup. A bad checksum anywhere else means real corruption of acknowledged data, so the node refuses to start rather than silently dropping it.

**Compaction is crash-safe.** The new segment is written to a temp file, fsynced, and atomically renamed before old segments are deleted. A crash in between leaves duplicates (the newest version wins on reload), never loss. Tombstones are kept by default, because dropping them early lets an out-of-date replica resurrect deleted keys during anti-entropy.

**Anti-entropy compares digests first.** Two nodes hash the `(key, version)` pairs they both replicate into 64 buckets (a one-level Merkle tree) and exchange records only for buckets that differ. A second pass over an in-sync pair transfers nothing.

## Guarantees and limits

What the tests show (with R + W > N, at most N − W replicas down, stable membership):
- An acknowledged write is readable from any coordinator, and a read returns the latest acknowledged write. This is checked over 300 random steps with random node failures, with and without clock skew.
- A replica that missed writes converges via hints, read repair or anti-entropy.
- Data survives process restarts; torn writes are discarded; corruption is detected.

What it does **not** do (yet):
- **Membership changes at runtime.** The ring is fixed at start-up from `DKV_PEERS`. Adding a node needs a restart, and keys are not streamed to a new owner except through anti-entropy.
- **Linearizability.** Quorum reads/writes with LWW are not linearizable under concurrent writers or failures (no read-your-writes across coordinators during partitions, no compare-and-set).
- **Tombstone garbage collection** is manual (`compact(drop_tombstones_older_than=...)`).
- **Security.** No authentication or TLS on the client or internal API; run it on a private network.
- **Scale.** The keydir holds every key in memory (as in Bitcask). Anti-entropy scans the whole store each round.

## Results

Measured on a GitHub-hosted runner by the [CI benchmark job](.github/workflows/ci.yml); see the job summary of the latest run. <!-- BENCHMARK -->

## Run it

```bash
pip install -e ".[dev]"
pytest -q                       # storage, ring, cluster (fault injection), HTTP: a few seconds, no network

docker compose up -d --build    # 3 nodes on :8001-8003
curl -X PUT localhost:8001/kv/hello -d world
curl localhost:8003/kv/hello    # -> world, read through another node
docker compose stop n3          # writes still succeed (W=2); n1 stores hints for n3
docker compose start n3         # n3 recovers its log, then hints / anti-entropy catch it up
python scripts/e2e.py           # the scripted version of the above, run by CI
python scripts/bench.py         # engine + in-process cluster throughput
```

## Tests

| Suite | Covers |
|---|---|
| `test_storage.py` | CRUD, LWW ordering and tie-break, restart, torn tail, bad checksum at tail, corruption mid-log, segment roll + compaction, tombstone GC, interrupted compaction, HLC monotonicity |
| `test_ring.py` | determinism, load balance with/without vnodes, ~1/N movement on join, only the leaver's keys move |
| `test_cluster.py` | N-way placement, one/two replicas down, hinted handoff, read repair, delete vs. resurrection, anti-entropy (and its no-op second pass), restart with durable hints, partitions, concurrent writers converge, randomised latest-write check with and without clock skew |
| `test_server.py` | HTTP round trip and versions, dead node → hints delivered over HTTP, no quorum → 503, size limit |
| `scripts/e2e.py` (CI) | real 3-container cluster: stop / restart a node, recovery from its volume, catch-up, no-quorum 503 |

## Roadmap

- [x] Log-structured storage engine with crash recovery and compaction
- [x] Consistent hashing with virtual nodes
- [x] Quorum replication, hinted handoff, read repair, anti-entropy
- [x] HTTP server, Docker Compose cluster, failure-recovery e2e in CI
- [ ] Runtime membership (join/leave with key streaming) via gossip
- [ ] Parallel fan-out to replicas (currently sequential per request)
- [ ] Hierarchical Merkle trees and incremental digests
- [ ] Jepsen-style history checker for the randomised tests
