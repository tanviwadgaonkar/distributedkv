"""Replication, quorums, hinted handoff, read repair, anti-entropy and restarts,
on an in-process 5-node cluster with injected failures (N=3, R=2, W=2)."""
import random
import time

import pytest

from dkv.clock import HybridClock
from dkv.node import Config, Node, QuorumError
from dkv.ring import HashRing
from dkv.transport import LocalTransport

NODES = [f"n{i}" for i in range(5)]


class Cluster:
    def __init__(self, root, config=Config(), skew_ms=None):
        self.root, self.config = root, config
        self.skew_ms = skew_ms or {}
        self.net = LocalTransport()
        self.ring = HashRing(vnodes=32)
        for nid in NODES:
            self.ring.add(nid)
        self.nodes = {nid: self._start(nid) for nid in NODES}

    def _start(self, nid):
        skew = self.skew_ms.get(nid, 0)
        clock = HybridClock(wall_ms=lambda: time.time_ns() // 1_000_000 + skew)
        node = Node(nid, self.root / nid, self.ring, self.net.for_node(nid), self.config, clock=clock)
        self.net.register(node)
        return node

    def restart(self, nid):
        self.nodes[nid].close()
        self.nodes[nid] = self._start(nid)

    def replicas(self, key):
        return self.ring.preference_list(key, self.config.n)

    def stored_on(self, key):
        return {nid for nid, n in self.nodes.items() if n.store.get_record(key) is not None}

    def coordinator_outside(self, key):
        return next(n for n in NODES if n not in self.replicas(key))

    def close(self):
        for n in self.nodes.values():
            n.close()


@pytest.fixture
def cluster(tmp_path):
    c = Cluster(tmp_path)
    yield c
    c.close()


def test_config_validation():
    with pytest.raises(ValueError):
        Config(n=3, r=4, w=2)


def test_any_node_can_coordinate_and_data_lands_on_n_replicas(cluster):
    cluster.nodes["n0"].put(b"user:1", b"alice")
    for nid in NODES:
        assert cluster.nodes[nid].get(b"user:1") == b"alice"
    assert cluster.stored_on(b"user:1") == set(cluster.replicas(b"user:1"))


def test_write_survives_one_replica_down_and_hint_is_delivered(cluster):
    key = b"cart:7"
    down = cluster.replicas(key)[0]
    coord = cluster.nodes[cluster.coordinator_outside(key)]
    cluster.net.down.add(down)

    coord.put(key, b"3 items")                       # W=2 of the 2 live replicas
    assert coord.get(key) == b"3 items"              # R=2 still reachable
    assert coord.pending_hints() == {down: 1}
    assert cluster.nodes[down].store.get(key) is None

    assert coord.deliver_hints() == 0                # still down: hint kept
    cluster.net.down.discard(down)
    assert coord.deliver_hints() == 1
    assert cluster.nodes[down].store.get(key) == b"3 items"
    assert coord.pending_hints() == {}


def test_two_replicas_down_fails_the_quorum(cluster):
    key = b"k"
    cluster.net.down |= set(cluster.replicas(key)[:2])
    coord = cluster.nodes[cluster.coordinator_outside(key)]
    with pytest.raises(QuorumError) as err:
        coord.put(key, b"v")
    assert err.value.got == 1 and err.value.need == 2
    with pytest.raises(QuorumError):
        coord.get(key)


def test_read_repair_fixes_a_stale_replica(cluster):
    key = b"profile:9"
    a, b, c = cluster.replicas(key)
    coord = cluster.nodes[cluster.coordinator_outside(key)]
    coord.put(key, b"v1")
    cluster.net.down.add(c)
    coord.put(key, b"v2")
    coord._hints.clear()                             # pretend the hint was lost
    cluster.net.down.discard(c)
    assert cluster.nodes[c].store.get(key) == b"v1"  # stale
    assert coord.get(key) == b"v2"
    assert cluster.nodes[c].store.get(key) == b"v2"  # repaired by the read
    assert coord.stats["read_repairs"] == 1


def test_delete_wins_over_older_value_and_is_not_resurrected(cluster):
    key = b"session:1"
    a, b, c = cluster.replicas(key)
    coord = cluster.nodes[cluster.coordinator_outside(key)]
    coord.put(key, b"live")
    cluster.net.down.add(c)
    coord.delete(key)
    coord._hints.clear()
    cluster.net.down.discard(c)
    assert cluster.nodes[c].store.get(key) == b"live"   # c missed the delete
    cluster.nodes[a].anti_entropy(c)
    assert cluster.nodes[c].store.get(key) is None      # tombstone propagated, not the old value
    assert coord.get(key) is None


def test_anti_entropy_repairs_only_what_differs(cluster):
    victim = "n3"
    cluster.net.down.add(victim)
    coord = cluster.nodes["n0"]
    keys = [f"k{i}".encode() for i in range(200)]
    for k in keys:
        try:
            coord.put(k, b"v-" + k)
        except QuorumError:
            pass  # cannot happen with one node down, kept for clarity
    coord._hints.clear()                                # hints lost: only anti-entropy can help
    cluster.net.down.discard(victim)

    missing = [k for k in keys if victim in cluster.replicas(k)]
    assert missing and all(cluster.nodes[victim].store.get(k) is None for k in missing)
    for peer in NODES:
        if peer != victim:
            cluster.nodes[peer].anti_entropy(victim)
    assert all(cluster.nodes[victim].store.get(k) == b"v-" + k for k in missing)
    # A second pass finds identical digests and exchanges nothing.
    assert sum(cluster.nodes[p].anti_entropy(victim) for p in NODES if p != victim) == 0


def test_restart_recovers_data_and_pending_hints(cluster):
    key = b"order:1"
    down = cluster.replicas(key)[1]
    coord_id = cluster.coordinator_outside(key)
    cluster.net.down.add(down)
    cluster.nodes[coord_id].put(key, b"paid")

    cluster.restart(coord_id)                           # coordinator crashes and comes back
    assert cluster.nodes[coord_id].pending_hints() == {down: 1}   # hints are durable
    replica = cluster.replicas(key)[0]
    cluster.restart(replica)
    assert cluster.nodes[replica].store.get(key) == b"paid"

    cluster.net.down.discard(down)
    assert cluster.nodes[coord_id].deliver_hints() == 1
    assert cluster.nodes[down].store.get(key) == b"paid"


def test_minority_side_of_a_partition_cannot_write(cluster):
    key = b"inventory:42"
    reps = cluster.replicas(key)
    outsiders = [n for n in NODES if n not in reps]
    minority = {reps[0], outsiders[0]}
    majority = set(NODES) - minority
    cluster.net.partition(minority, majority)

    with pytest.raises(QuorumError):
        cluster.nodes[outsiders[0]].put(key, b"from-minority")   # reaches 1 replica
    cluster.nodes[outsiders[1]].put(key, b"from-majority")       # reaches 2 replicas
    cluster.net.heal()
    # The minority's failed write still landed on one replica (no rollback), but
    # it is older than the majority's successful write, so LWW discards it.
    assert cluster.nodes[reps[0]].get(key) == b"from-majority"


def test_concurrent_writers_converge(cluster):
    key = b"counter"
    a, b, c = cluster.replicas(key)
    cluster.net.partition({a}, set(NODES) - {a})  # a is fully isolated
    others = [n for n in NODES if n not in (a, b, c)]
    # Each side writes through a coordinator that can reach it (W=1 here).
    cluster.config = Config(n=3, r=1, w=1)
    for n in cluster.nodes.values():
        n.config = cluster.config
    cluster.nodes[a].put(key, b"from-a")
    cluster.nodes[others[0]].put(key, b"from-bc")
    cluster.net.heal()
    for x in (a, b, c):
        for y in (a, b, c):
            if x != y:
                cluster.nodes[x].anti_entropy(y)
    values = {cluster.nodes[x].store.get(key) for x in (a, b, c)}
    assert len(values) == 1                            # all replicas agree on one winner


@pytest.mark.parametrize("skewed", [False, True])
def test_randomised_reads_see_the_latest_successful_write(tmp_path, skewed):
    """With R + W > N and at most one node down at a time, every read returns
    the value of the latest successful write (300 random steps). The skewed
    run gives nodes wall clocks up to 2 s apart: without re-stamping, a write
    coordinated by a node whose clock is behind would lose to an older write."""
    skew = {nid: -500 * i for i, nid in enumerate(NODES)} if skewed else {}
    cluster = Cluster(tmp_path, skew_ms=skew)
    rng = random.Random(7)
    keys = [f"key{i}".encode() for i in range(10)]
    latest: dict[bytes, bytes | None] = {}
    for step in range(300):
        cluster.net.down = {rng.choice(NODES)} if rng.random() < 0.5 else set()
        live = [n for n in NODES if n not in cluster.net.down]
        coord = cluster.nodes[rng.choice(live)]
        key = rng.choice(keys)
        if rng.random() < 0.6:
            value = None if rng.random() < 0.1 else f"v{step}".encode()
            coord.put(key, value)
            latest[key] = value
        elif key in latest:
            assert coord.get(key) == latest[key], f"step {step}"
        if rng.random() < 0.2:
            for n in cluster.nodes.values():
                n.deliver_hints()
    if skewed:
        assert sum(n.stats["restamps"] for n in cluster.nodes.values()) > 0
    cluster.close()
