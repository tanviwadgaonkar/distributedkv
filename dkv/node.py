"""A storage node that can also coordinate client requests (Dynamo-style, leaderless).

Write path (coordinator):
  1. Stamp the write with a hybrid-logical-clock version.
  2. Send it to the key's N replicas (its preference list on the ring).
     A replica holding a *newer* version rejects the write and returns that
     version; the coordinator advances its clock past it and re-stamps. This
     makes a write that starts after another write finished always win, even
     if the coordinator's wall clock is behind: with R + W > N, at least one
     replica it reaches holds the previous write.
  3. Succeed once W replicas acknowledge. For each replica that did not answer,
     store a *hint* durably; `deliver_hints()` replays it when the replica is back.
  Fewer than W acks raises QuorumError. Replicas that did ack keep the write
  (no rollback, as in Dynamo/Cassandra); a later read or anti-entropy spreads it.

Read path (coordinator):
  1. Ask the N replicas, need R answers.
  2. Return the value with the highest version (last-writer-wins).
  3. Read repair: push that newest record to replicas that answered with an
     older version or nothing.

With R + W > N every read quorum overlaps every write quorum, so a successful
read sees the latest successful write, as long as membership is stable.

Background repair: `anti_entropy(peer)` compares per-bucket digests of the
keys both nodes replicate (a one-level Merkle tree) and exchanges only the
records in buckets that differ.
"""
from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dkv.clock import HybridClock
from dkv.ring import HashRing, hash64
from dkv.storage import LogStore, Record, Version
from dkv.transport import NodeUnavailable, Transport

DIGEST_BUCKETS = 64
MAX_RESTAMPS = 3


class QuorumError(RuntimeError):
    def __init__(self, op: str, got: int, need: int, failed: list[str]):
        super().__init__(f"{op}: {got}/{need} replicas answered (unreachable: {', '.join(failed) or '-'})")
        self.got, self.need, self.failed = got, need, failed


@dataclass(frozen=True)
class Config:
    n: int = 3  # replicas per key
    r: int = 2  # read quorum
    w: int = 2  # write quorum

    def __post_init__(self) -> None:
        if not (1 <= self.r <= self.n and 1 <= self.w <= self.n):
            raise ValueError("need 1 <= R, W <= N")


# ---------- wire format (JSON-safe, used by every transport) ----------

def _b64(b: bytes) -> str:
    return base64.b64encode(b).decode()


def to_wire(rec: Record) -> dict:
    return {"key": _b64(rec.key), "value": None if rec.value is None else _b64(rec.value),
            "ts": rec.version.ts, "node": rec.version.node}


def from_wire(d: dict) -> Record:
    value = None if d["value"] is None else base64.b64decode(d["value"])
    return Record(base64.b64decode(d["key"]), value, Version(d["ts"], d["node"]))


def bucket_of(key: bytes) -> int:
    return hash64(key) % DIGEST_BUCKETS


class Node:
    def __init__(self, node_id: str, data_dir: str | Path, ring: HashRing, transport: Transport,
                 config: Config = Config(), *, fsync: bool = False, clock: HybridClock | None = None):
        self.id = node_id
        self.ring = ring
        self.net = transport
        self.config = config
        self.clock = clock or HybridClock()
        self.store = LogStore(Path(data_dir) / "data", fsync=fsync)
        self._hint_root = Path(data_dir) / "hints"
        self._hints: dict[str, LogStore] = {
            p.name: LogStore(p, fsync=fsync) for p in self._hint_root.glob("*") if p.is_dir()
        }
        self.stats = {"read_repairs": 0, "hints_stored": 0, "hints_delivered": 0, "anti_entropy_records": 0,
                      "restamps": 0}

    # ---------- helpers ----------

    def _replicas(self, key: bytes) -> list[str]:
        return self.ring.preference_list(key, self.config.n)

    def _call(self, target: str, method: str, **args: Any) -> Any:
        if target == self.id:  # no network hop to ourselves
            return getattr(self, "handle_" + method)(**args)
        return self.net.call(target, method, **args)

    def _hint_store(self, target: str) -> LogStore:
        if target not in self._hints:
            self._hints[target] = LogStore(self._hint_root / target, fsync=self.store.fsync)
        return self._hints[target]

    # ---------- client API (this node coordinates) ----------

    def put(self, key: bytes, value: bytes | None) -> Version:
        for _ in range(MAX_RESTAMPS):
            version = Version(self.clock.now(), self.id)
            wire = to_wire(Record(key, value, version))
            acks, failed, newer = 0, [], 0
            for target in self._replicas(key):
                try:
                    reply = self._call(target, "replicate", record=wire)
                except NodeUnavailable:
                    failed.append(target)
                    continue
                if reply["applied"]:
                    acks += 1
                else:
                    newer = max(newer, reply["ts"])
            if newer:  # our clock was behind a version already stored: re-stamp above it
                self.clock.observe(newer)
                self.stats["restamps"] += 1
                continue
            break
        for target in failed:  # hinted handoff: keep the write for each replica that missed it
            self._hint_store(target).apply(Record(key, value, version))
            self.stats["hints_stored"] += 1
        if acks < self.config.w:
            raise QuorumError("write", acks, self.config.w, failed)
        return version

    def delete(self, key: bytes) -> Version:
        return self.put(key, None)

    def get(self, key: bytes) -> bytes | None:
        rec = self.get_record(key)
        return None if rec is None else rec.value

    def get_record(self, key: bytes) -> Record | None:
        answers: dict[str, Record | None] = {}
        failed = []
        for target in self._replicas(key):
            try:
                raw = self._call(target, "read", key=_b64(key))
                answers[target] = None if raw is None else from_wire(raw)
            except NodeUnavailable:
                failed.append(target)
        if len(answers) < self.config.r:
            raise QuorumError("read", len(answers), self.config.r, failed)

        newest = max((r for r in answers.values() if r is not None), key=lambda r: r.version, default=None)
        if newest is not None:
            self.clock.observe(newest.version.ts)
            wire = to_wire(newest)
            for target, rec in answers.items():
                if rec is None or rec.version < newest.version:
                    try:
                        self._call(target, "replicate", record=wire)
                        self.stats["read_repairs"] += 1
                    except NodeUnavailable:
                        pass  # anti-entropy will catch it later
        return newest

    # ---------- background repair ----------

    def deliver_hints(self) -> int:
        """Replay stored hints to replicas that are reachable again. Returns how many were delivered."""
        delivered = 0
        for target, hints in list(self._hints.items()):
            pending = list(hints.records())
            try:
                for rec in pending:
                    self._call(target, "replicate", record=to_wire(rec))
                    delivered += 1
            except NodeUnavailable:
                continue
            # Every hint reached the target: drop the hint log.
            hints.close()
            for f in hints.dir.glob("*"):
                f.unlink()
            hints.dir.rmdir()
            del self._hints[target]
        self.stats["hints_delivered"] += delivered
        return delivered

    def pending_hints(self) -> dict[str, int]:
        return {t: sum(1 for _ in h.records()) for t, h in self._hints.items()}

    def _shared_records(self, peer: str) -> list[Record]:
        """Our latest records for keys that both we and `peer` should replicate."""
        return [rec for rec in self.store.records()
                if {self.id, peer} <= set(self._replicas(rec.key))]

    @staticmethod
    def _digests(records: list[Record]) -> list[str]:
        buckets = [hashlib.blake2b(digest_size=16) for _ in range(DIGEST_BUCKETS)]
        for rec in sorted(records, key=lambda r: r.key):
            buckets[bucket_of(rec.key)].update(rec.key + b"\0" + rec.version.ts.to_bytes(8, "big") +
                                               rec.version.node.encode() + b"\0")
        return [b.hexdigest() for b in buckets]

    def anti_entropy(self, peer: str) -> int:
        """Bring `peer` and this node in sync for the keys they share. Returns records exchanged."""
        mine = self._shared_records(peer)
        theirs = self._call(peer, "digests", peer=self.id)
        differing = [i for i, (a, b) in enumerate(zip(self._digests(mine), theirs)) if a != b]
        if not differing:
            return 0
        # Push our records in differing buckets; pull theirs. LWW on both sides
        # means sending extra records is harmless, just wasted bandwidth.
        push = [to_wire(r) for r in mine if bucket_of(r.key) in differing]
        pulled = self._call(peer, "sync_buckets", peer=self.id, buckets=differing, records=push)
        for wire in pulled:
            rec = from_wire(wire)
            self.clock.observe(rec.version.ts)
            self.store.apply(rec)
        exchanged = len(push) + len(pulled)
        self.stats["anti_entropy_records"] += exchanged
        return exchanged

    # ---------- handlers (called by other nodes through the transport) ----------

    def handle_replicate(self, record: dict) -> dict:
        """Apply if newer. Replies with the version now stored, so a coordinator with a lagging clock can catch up."""
        rec = from_wire(record)
        self.clock.observe(rec.version.ts)
        applied = self.store.apply(rec)
        current = self.store.version(rec.key)
        return {"applied": applied or current == rec.version, "ts": current.ts}

    def handle_read(self, key: str) -> dict | None:
        rec = self.store.get_record(base64.b64decode(key))
        return None if rec is None else to_wire(rec)

    def handle_digests(self, peer: str) -> list[str]:
        return self._digests(self._shared_records(peer))

    def handle_sync_buckets(self, peer: str, buckets: list[int], records: list[dict]) -> list[dict]:
        wanted = set(buckets)
        reply = [to_wire(r) for r in self._shared_records(peer) if bucket_of(r.key) in wanted]
        for wire in records:
            self.handle_replicate(wire)
        return reply

    def close(self) -> None:
        self.store.close()
        for h in self._hints.values():
            h.close()
