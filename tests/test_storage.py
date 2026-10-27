import os

import pytest

from dkv.clock import HybridClock
from dkv.storage import CorruptionError, LogStore, Record, Version


def put(store, key, value, ts, node="n1"):
    return store.apply(Record(key, value, Version(ts, node)))


def test_put_get_overwrite_delete(tmp_path):
    with LogStore(tmp_path) as s:
        put(s, b"k", b"v1", 1)
        put(s, b"k", b"v2", 2)
        assert s.get(b"k") == b"v2"
        put(s, b"k", None, 3)
        assert s.get(b"k") is None
        assert s.get_record(b"k").value is None  # the tombstone is still visible to replication
        assert len(s) == 0 and s.keys() == []


def test_last_writer_wins_ignores_stale_and_duplicate_writes(tmp_path):
    with LogStore(tmp_path) as s:
        assert put(s, b"k", b"new", 10)
        assert not put(s, b"k", b"old", 5)        # arrives late
        assert not put(s, b"k", b"new", 10)       # duplicate delivery
        assert put(s, b"k", b"tie-break", 10, node="n2")  # same ts, higher node id wins
        assert s.get(b"k") == b"tie-break"


def test_binary_keys_and_empty_values(tmp_path):
    with LogStore(tmp_path) as s:
        put(s, b"\x00\xff", b"", 1)
        assert s.get(b"\x00\xff") == b""
        assert s.get(b"missing") is None


def test_data_survives_restart(tmp_path):
    with LogStore(tmp_path) as s:
        for i in range(100):
            put(s, f"k{i}".encode(), f"v{i}".encode(), i + 1)
        put(s, b"k5", None, 1000)
    with LogStore(tmp_path) as s:
        assert len(s) == 99
        assert s.get(b"k42") == b"v42" and s.get(b"k5") is None


def test_torn_tail_write_is_truncated_on_recovery(tmp_path):
    with LogStore(tmp_path) as s:
        put(s, b"a", b"1", 1)
        put(s, b"b", b"2", 2)
    seg = next(tmp_path.glob("*.seg"))
    size = seg.stat().st_size
    with open(seg, "r+b") as fh:  # simulate a crash halfway through writing the last record
        fh.truncate(size - 3)
    with LogStore(tmp_path) as s:
        assert s.get(b"a") == b"1" and s.get(b"b") is None
        assert s.recovered_truncated_bytes > 0
        put(s, b"c", b"3", 3)  # the log is writable again after the truncation
    with LogStore(tmp_path) as s:
        assert s.get(b"c") == b"3" and s.recovered_truncated_bytes == 0


def test_corrupt_tail_record_checksum_is_truncated(tmp_path):
    with LogStore(tmp_path) as s:
        put(s, b"a", b"1", 1)
        put(s, b"b", b"hello", 2)
    seg = next(tmp_path.glob("*.seg"))
    data = bytearray(seg.read_bytes())
    data[-1] ^= 0xFF  # flip a bit in the last value byte
    seg.write_bytes(bytes(data))
    with LogStore(tmp_path) as s:
        assert s.get(b"a") == b"1" and s.get(b"b") is None


def test_corruption_in_sealed_segment_refuses_to_start(tmp_path):
    with LogStore(tmp_path, max_segment_bytes=64) as s:
        for i in range(10):
            put(s, f"k{i}".encode(), b"x" * 40, i + 1)
    segs = sorted(tmp_path.glob("*.seg"))
    assert len(segs) > 2
    data = bytearray(segs[0].read_bytes())
    data[10] ^= 0xFF
    segs[0].write_bytes(bytes(data))
    with pytest.raises(CorruptionError):
        LogStore(tmp_path)


def test_segments_roll_and_compaction_reclaims_space(tmp_path):
    with LogStore(tmp_path, max_segment_bytes=1024) as s:
        for round_ in range(20):
            for i in range(20):
                put(s, f"k{i}".encode(), f"value-{round_}".encode() * 4, round_ * 100 + i + 1)
        put(s, b"k0", None, 10_000)
        assert len(list(tmp_path.glob("*.seg"))) > 1
        stats = s.compact()
        assert stats["bytes_after"] < stats["bytes_before"] / 10
        assert len(list(tmp_path.glob("*.seg"))) == 1
        assert s.get(b"k7") == b"value-19" * 4
        assert s.get_record(b"k0").value is None  # tombstone kept by default
        put(s, b"new", b"after-compaction", 20_000)
    with LogStore(tmp_path) as s:
        assert len(s) == 20 and s.get(b"new") == b"after-compaction"


def test_compaction_can_drop_old_tombstones(tmp_path):
    with LogStore(tmp_path) as s:
        put(s, b"gone", b"x", 1)
        put(s, b"gone", None, 2)
        put(s, b"kept", None, 50)
        s.compact(drop_tombstones_older_than=10)
        assert s.get_record(b"gone") is None
        assert s.get_record(b"kept").value is None


def test_interrupted_compaction_loses_nothing(tmp_path):
    # Crash after the new segment is published but before old ones are deleted:
    # both copies exist on disk, and recovery must pick the newest versions.
    with LogStore(tmp_path) as s:
        put(s, b"a", b"1", 1)
        put(s, b"a", b"2", 2)
    old = next(tmp_path.glob("*.seg"))
    backup = old.read_bytes()
    with LogStore(tmp_path) as s:
        s.compact()
    old.write_bytes(backup)  # "undelete" the old segment
    (tmp_path / "0000000099.tmp").write_bytes(b"partial")  # and leave a stale temp file
    with LogStore(tmp_path) as s:
        assert s.get(b"a") == b"2"
    assert not list(tmp_path.glob("*.tmp"))


def test_fsync_mode_writes_through(tmp_path):
    with LogStore(tmp_path, fsync=True) as s:
        put(s, b"k", b"v", 1)
        assert os.path.getsize(next(tmp_path.glob("*.seg"))) > 0


def test_hybrid_clock_is_monotonic_and_observes_remote_time():
    wall = [1000]
    c = HybridClock(wall_ms=lambda: wall[0])
    a = c.now()
    wall[0] = 900  # wall clock jumps backwards (NTP step)
    b = c.now()
    assert b > a
    c.observe(b + 10_000)
    assert c.now() > b + 10_000
