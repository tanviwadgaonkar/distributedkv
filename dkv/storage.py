"""Log-structured storage engine (Bitcask-style).

Every write is appended to the active segment file; an in-memory index
("keydir") maps each key to the file offset of its latest record. Reads are
one seek. Deletes append a tombstone. Old segments are rewritten by
`compact()`, which keeps only live records.

Record layout (little-endian):

    crc32   u32   over everything after this field
    ts      u64   version timestamp (hybrid logical clock, see dkv.clock)
    node    u16   length of the writer's node id
    klen    u32
    vlen    i32   -1 marks a tombstone
    node_id bytes
    key     bytes
    value   bytes

Crash safety: a write is durable once `put()` returns if `fsync=True`
(the default for tests is False, for speed). On open, every segment is
scanned; a torn or corrupt record at the *tail* of the newest segment (a
crash mid-write) is truncated away, while corruption anywhere else raises,
because silently dropping committed data is worse than refusing to start.
"""
from __future__ import annotations

import os
import struct
import threading
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

HEADER = struct.Struct("<IQHIi")
TOMBSTONE = -1
SEGMENT_SUFFIX = ".seg"


class CorruptionError(RuntimeError):
    """A record failed its checksum somewhere other than the tail of the newest segment."""


@dataclass(frozen=True, order=True)
class Version:
    """Total order for last-writer-wins: timestamp first, node id breaks ties."""

    ts: int
    node: str = ""


@dataclass(frozen=True)
class Record:
    key: bytes
    value: bytes | None  # None = tombstone
    version: Version


@dataclass
class _Location:
    segment: int
    offset: int
    size: int
    version: Version
    tombstone: bool


def encode(rec: Record) -> bytes:
    node = rec.version.node.encode()
    vlen = TOMBSTONE if rec.value is None else len(rec.value)
    body = struct.pack("<QHIi", rec.version.ts, len(node), len(rec.key), vlen) + node + rec.key + (rec.value or b"")
    return struct.pack("<I", zlib.crc32(body)) + body


def _scan(data: bytes) -> Iterator[tuple[int, int, Record | None]]:
    """Yield (offset, size, record) for each record; record is None at the first bad/torn one, then stop."""
    pos = 0
    while pos < len(data):
        if len(data) - pos < HEADER.size:
            yield pos, len(data) - pos, None
            return
        crc, ts, nlen, klen, vlen = HEADER.unpack_from(data, pos)
        size = HEADER.size + nlen + klen + max(vlen, 0)
        if vlen < TOMBSTONE or pos + size > len(data) or zlib.crc32(data[pos + 4:pos + size]) != crc:
            yield pos, len(data) - pos, None
            return
        start = pos + HEADER.size
        node = data[start:start + nlen].decode()
        key = data[start + nlen:start + nlen + klen]
        value = None if vlen == TOMBSTONE else data[start + nlen + klen:start + size]
        yield pos, size, Record(key, value, Version(ts, node))
        pos += size


class LogStore:
    """A single node's durable key-value store. Thread-safe."""

    def __init__(self, directory: str | Path, *, fsync: bool = False, max_segment_bytes: int = 4 * 1024 * 1024):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.fsync = fsync
        self.max_segment_bytes = max_segment_bytes
        self._lock = threading.RLock()
        self._keydir: dict[bytes, _Location] = {}
        self.recovered_truncated_bytes = 0
        for stale in self.dir.glob("*.tmp"):  # a compaction that crashed before publishing
            stale.unlink()
        segments = sorted(int(p.stem) for p in self.dir.glob(f"*{SEGMENT_SUFFIX}"))
        for i, seg in enumerate(segments):
            self._load(seg, is_newest=i == len(segments) - 1)
        self._active_id = segments[-1] if segments else 0
        self._active = open(self._path(self._active_id), "ab")

    # ---------- files ----------

    def _path(self, seg: int) -> Path:
        return self.dir / f"{seg:010d}{SEGMENT_SUFFIX}"

    def _load(self, seg: int, is_newest: bool) -> None:
        path = self._path(seg)
        data = path.read_bytes()
        for offset, size, rec in _scan(data):
            if rec is None:
                if not is_newest:
                    raise CorruptionError(f"{path.name}: bad record at offset {offset}")
                # Torn write from a crash: drop the incomplete tail.
                with open(path, "r+b") as fh:
                    fh.truncate(offset)
                self.recovered_truncated_bytes += size
                return
            self._index(rec, seg, offset, size)

    def _index(self, rec: Record, seg: int, offset: int, size: int) -> None:
        cur = self._keydir.get(rec.key)
        if cur is None or rec.version >= cur.version:
            self._keydir[rec.key] = _Location(seg, offset, size, rec.version, rec.value is None)

    def _roll_if_full(self) -> None:
        if self._active.tell() >= self.max_segment_bytes:
            self._active.close()
            self._active_id += 1
            self._active = open(self._path(self._active_id), "ab")

    # ---------- writes ----------

    def apply(self, rec: Record) -> bool:
        """Store `rec` if it is newer than what we have (last-writer-wins). Returns True if applied.

        Replicas call this with versions chosen by the coordinator, so the same
        write delivered twice, or out of order, converges to the same state.
        """
        with self._lock:
            cur = self._keydir.get(rec.key)
            if cur is not None and rec.version <= cur.version:
                return False
            self._roll_if_full()
            data = encode(rec)
            offset = self._active.tell()
            self._active.write(data)
            self._active.flush()
            if self.fsync:
                os.fsync(self._active.fileno())
            self._index(rec, self._active_id, offset, len(data))
            return True

    # ---------- reads ----------

    def get_record(self, key: bytes) -> Record | None:
        """Latest record for `key`, including tombstones (replication needs to see deletes)."""
        with self._lock:
            loc = self._keydir.get(key)
            if loc is None:
                return None
            if loc.segment == self._active_id:
                self._active.flush()
            with open(self._path(loc.segment), "rb") as fh:
                fh.seek(loc.offset)
                data = fh.read(loc.size)
        _, _, rec = next(_scan(data))
        if rec is None:
            raise CorruptionError(f"segment {loc.segment}: bad record at offset {loc.offset}")
        return rec

    def get(self, key: bytes) -> bytes | None:
        rec = self.get_record(key)
        return None if rec is None else rec.value

    def keys(self) -> list[bytes]:
        with self._lock:
            return [k for k, loc in self._keydir.items() if not loc.tombstone]

    def records(self) -> Iterator[Record]:
        """Every key's latest record, tombstones included (for anti-entropy and rebalancing)."""
        with self._lock:
            keys = list(self._keydir)
        for k in keys:
            rec = self.get_record(k)
            if rec is not None:
                yield rec

    def __len__(self) -> int:
        with self._lock:
            return sum(1 for loc in self._keydir.values() if not loc.tombstone)

    # ---------- maintenance ----------

    def compact(self, *, drop_tombstones_older_than: int | None = None) -> dict:
        """Rewrite all sealed and active data into one fresh segment holding only latest records.

        Tombstones are kept by default: a replica that missed the delete could
        otherwise resurrect the key during anti-entropy. Pass a timestamp to
        drop tombstones older than it (once all replicas are known to have them).
        """
        with self._lock:
            before = sum(p.stat().st_size for p in self.dir.glob(f"*{SEGMENT_SUFFIX}"))
            old = sorted(int(p.stem) for p in self.dir.glob(f"*{SEGMENT_SUFFIX}"))
            live = list(self.records())
            self._active.close()
            new_id = (old[-1] if old else 0) + 1
            tmp = self.dir / f"{new_id:010d}.tmp"
            keydir: dict[bytes, _Location] = {}
            with open(tmp, "wb") as fh:
                for rec in live:
                    if rec.value is None and drop_tombstones_older_than is not None \
                            and rec.version.ts < drop_tombstones_older_than:
                        continue
                    data = encode(rec)
                    keydir[rec.key] = _Location(new_id, fh.tell(), len(data), rec.version, rec.value is None)
                    fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            # Publish the new segment before deleting old ones: a crash in
            # between leaves duplicates (harmless, newest version wins), never loss.
            os.replace(tmp, self._path(new_id))
            for seg in old:
                self._path(seg).unlink()
            self._keydir = keydir
            self._active_id = new_id
            self._active = open(self._path(new_id), "ab")
            after = self._path(new_id).stat().st_size
            return {"bytes_before": before, "bytes_after": after, "records": len(keydir)}

    def close(self) -> None:
        with self._lock:
            self._active.close()

    def __enter__(self) -> "LogStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
