"""How nodes talk to each other.

`Transport.call(target, method, **args)` invokes `method` on another node and
returns its result, or raises `NodeUnavailable`. The cluster logic only sees
this interface, so the same code runs over HTTP (dkv.server) or in-process
(`LocalTransport`, used by the tests), where failures are injected precisely:
a crashed node, a one-way network partition, or a dropped message.
"""
from __future__ import annotations

from typing import Any, Protocol


class NodeUnavailable(ConnectionError):
    """The target did not answer (down, partitioned, timed out)."""


class Transport(Protocol):
    def call(self, target: str, method: str, **args: Any) -> Any: ...


class LocalTransport:
    """In-process network with fault injection."""

    def __init__(self) -> None:
        self.nodes: dict[str, Any] = {}
        self.down: set[str] = set()
        self.cut: set[tuple[str, str]] = set()  # (src, dst) links that drop messages
        self.calls = 0

    def register(self, node: Any) -> None:
        self.nodes[node.id] = node

    def for_node(self, src: str) -> "_Bound":
        return _Bound(self, src)

    def partition(self, a: set[str], b: set[str]) -> None:
        """Cut every link between group a and group b (both directions)."""
        for x in a:
            for y in b:
                self.cut |= {(x, y), (y, x)}

    def heal(self) -> None:
        self.cut.clear()

    def _call(self, src: str, target: str, method: str, args: dict) -> Any:
        self.calls += 1
        if target in self.down or src in self.down or (src, target) in self.cut or target not in self.nodes:
            raise NodeUnavailable(f"{src} -> {target}")
        return getattr(self.nodes[target], "handle_" + method)(**args)


class _Bound:
    """A LocalTransport as seen from one node (so partitions know the sender)."""

    def __init__(self, net: LocalTransport, src: str) -> None:
        self.net, self.src = net, src

    def call(self, target: str, method: str, **args: Any) -> Any:
        return self.net._call(self.src, target, method, args)
