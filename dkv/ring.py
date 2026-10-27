"""Consistent hashing with virtual nodes.

Each physical node owns `vnodes` points on a 64-bit ring (hash of
"<node>#<i>"). A key belongs to the first point clockwise from its hash; its
replicas are the next distinct *physical* nodes clockwise (the "preference
list", as in Dynamo). Adding or removing one node of N moves only about
1/N of the keys, and virtual nodes keep the load even.
"""
from __future__ import annotations

import bisect
import hashlib
from dataclasses import dataclass, field


def hash64(data: bytes) -> int:
    # blake2b is fast, stable across processes (unlike hash()), and well mixed.
    return int.from_bytes(hashlib.blake2b(data, digest_size=8).digest(), "big")


@dataclass
class HashRing:
    vnodes: int = 128
    _points: list[int] = field(default_factory=list, repr=False)
    _owners: list[str] = field(default_factory=list, repr=False)
    _nodes: set[str] = field(default_factory=set)

    @property
    def nodes(self) -> set[str]:
        return set(self._nodes)

    def add(self, node: str) -> None:
        if node in self._nodes:
            return
        self._nodes.add(node)
        for i in range(self.vnodes):
            p = hash64(f"{node}#{i}".encode())
            idx = bisect.bisect_left(self._points, p)
            self._points.insert(idx, p)
            self._owners.insert(idx, node)

    def remove(self, node: str) -> None:
        if node not in self._nodes:
            return
        self._nodes.discard(node)
        keep = [(p, o) for p, o in zip(self._points, self._owners) if o != node]
        self._points = [p for p, _ in keep]
        self._owners = [o for _, o in keep]

    def preference_list(self, key: bytes, n: int) -> list[str]:
        """The first `n` distinct physical nodes clockwise from the key's hash (fewer if the ring is smaller)."""
        if not self._points:
            return []
        n = min(n, len(self._nodes))
        idx = bisect.bisect_right(self._points, hash64(key)) % len(self._points)
        out: list[str] = []
        for step in range(len(self._points)):
            owner = self._owners[(idx + step) % len(self._points)]
            if owner not in out:
                out.append(owner)
                if len(out) == n:
                    break
        return out

    def owner(self, key: bytes) -> str | None:
        prefs = self.preference_list(key, 1)
        return prefs[0] if prefs else None
