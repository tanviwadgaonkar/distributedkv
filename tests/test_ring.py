from collections import Counter

from dkv.ring import HashRing

KEYS = [f"user:{i}".encode() for i in range(20_000)]


def ring_of(n, vnodes=128):
    r = HashRing(vnodes=vnodes)
    for i in range(n):
        r.add(f"node{i}")
    return r


def test_empty_ring():
    assert HashRing().preference_list(b"k", 3) == [] and HashRing().owner(b"k") is None


def test_preference_list_is_distinct_and_deterministic():
    r = ring_of(5)
    prefs = r.preference_list(b"key", 3)
    assert len(prefs) == 3 == len(set(prefs))
    assert ring_of(5).preference_list(b"key", 3) == prefs  # same on every process / node
    assert r.preference_list(b"key", 10) == r.preference_list(b"key", 5)  # capped at node count


def test_virtual_nodes_balance_load():
    ring = ring_of(5)
    load = Counter(ring.owner(k) for k in KEYS)
    mean = len(KEYS) / 5
    # With 128 vnodes per node, every node is within 25% of the mean.
    assert max(load.values()) < 1.25 * mean and min(load.values()) > 0.75 * mean
    one_point = ring_of(5, vnodes=1)
    single = Counter(one_point.owner(k) for k in KEYS)
    assert max(single.values()) - min(single.values()) > max(load.values()) - min(load.values())


def test_adding_a_node_moves_about_one_nth_of_keys():
    before = ring_of(4)
    after = ring_of(4)
    after.add("node4")
    moved = [k for k in KEYS if before.owner(k) != after.owner(k)]
    assert all(after.owner(k) == "node4" for k in moved)  # keys only move *to* the new node
    assert 0.12 < len(moved) / len(KEYS) < 0.28  # ideal is 1/5 = 0.20


def test_removing_a_node_only_moves_its_keys():
    full = ring_of(5)
    reduced = ring_of(5)
    reduced.remove("node2")
    for k in KEYS[:5000]:
        if full.owner(k) != "node2":
            assert reduced.owner(k) == full.owner(k)
    assert "node2" not in reduced.nodes


def test_replicas_shift_by_one_when_owner_leaves():
    r = ring_of(5)
    prefs = r.preference_list(b"k", 3)
    r.remove(prefs[0])
    assert r.preference_list(b"k", 3)[:2] == prefs[1:]
