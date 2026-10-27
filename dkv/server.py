"""HTTP server for one node: client API, internal replication API, background repair.

Configuration (environment):
    DKV_NODE_ID   this node's id, e.g. n1
    DKV_PEERS     every node including this one: "n1=http://n1:8000,n2=http://n2:8000,..."
    DKV_DATA      data directory (default ./data/<id>)
    DKV_N, DKV_R, DKV_W              replication settings (default 3/2/2)
    DKV_REPAIR_SECONDS               hint delivery + anti-entropy interval (default 5, 0 = off)

Run:  uvicorn dkv.server:app --port 8000
"""
from __future__ import annotations

import logging
import os
import random
import threading
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request, Response

from dkv.node import Config, Node, QuorumError
from dkv.ring import HashRing
from dkv.transport import NodeUnavailable

log = logging.getLogger("dkv")
MAX_VALUE_BYTES = 1024 * 1024
INTERNAL_METHODS = {"replicate", "read", "digests", "sync_buckets"}


class HttpTransport:
    """Calls another node's /internal/<method> endpoint. Timeouts and connection errors become NodeUnavailable."""

    def __init__(self, peers: dict[str, str], timeout: float = 2.0, clients: dict[str, httpx.Client] | None = None):
        self.peers = peers
        self._clients = clients or {nid: httpx.Client(base_url=url, timeout=timeout) for nid, url in peers.items()}

    def call(self, target: str, method: str, **args: Any) -> Any:
        try:
            r = self._clients[target].post(f"/internal/{method}", json=args)
        except (httpx.TransportError, KeyError) as exc:
            raise NodeUnavailable(f"{target}: {exc}") from exc
        if r.status_code >= 500:
            raise NodeUnavailable(f"{target}: HTTP {r.status_code}")
        r.raise_for_status()
        return r.json()


def parse_peers(spec: str) -> dict[str, str]:
    peers = {}
    for item in filter(None, (s.strip() for s in spec.split(","))):
        nid, _, url = item.partition("=")
        if not nid or not url:
            raise ValueError(f"bad DKV_PEERS entry {item!r}; expected id=url")
        peers[nid] = url.rstrip("/")
    return peers


def build_node(node_id: str, peers: dict[str, str], data_dir: str, config: Config, transport=None) -> Node:
    ring = HashRing()
    for nid in peers:
        ring.add(nid)
    return Node(node_id, data_dir, ring, transport or HttpTransport(peers), config)


def repair_loop(node: Node, interval: float, stop: threading.Event) -> None:
    peers = sorted(node.ring.nodes - {node.id})
    while not stop.wait(interval):
        try:
            node.deliver_hints()
            if peers:
                node.anti_entropy(random.choice(peers))
        except NodeUnavailable:
            pass  # peer down: try another next round
        except Exception:  # never let the repair thread die silently
            log.exception("repair round failed")


def create_app(node: Node | None = None, repair_seconds: float | None = None) -> FastAPI:
    state: dict[str, Any] = {"node": node}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if state["node"] is None:
            nid = os.environ["DKV_NODE_ID"]
            cfg = Config(int(os.getenv("DKV_N", 3)), int(os.getenv("DKV_R", 2)), int(os.getenv("DKV_W", 2)))
            state["node"] = build_node(nid, parse_peers(os.environ["DKV_PEERS"]),
                                       os.getenv("DKV_DATA", f"data/{nid}"), cfg)
        interval = repair_seconds if repair_seconds is not None else float(os.getenv("DKV_REPAIR_SECONDS", 5))
        stop = threading.Event()
        if interval > 0:
            threading.Thread(target=repair_loop, args=(state["node"], interval, stop), daemon=True).start()
        yield
        stop.set()
        state["node"].close()

    app = FastAPI(title="DistributedKV node", lifespan=lifespan)

    def n() -> Node:
        return state["node"]

    @app.get("/health")
    def health() -> dict:
        return {"node": n().id, "keys": len(n().store), "pending_hints": n().pending_hints(), "stats": n().stats}

    @app.get("/kv/{key}")
    def get_value(key: str) -> Response:
        try:
            rec = n().get_record(key.encode())
        except QuorumError as exc:
            raise HTTPException(503, str(exc)) from exc
        if rec is None or rec.value is None:
            raise HTTPException(404, "key not found")
        return Response(rec.value, media_type="application/octet-stream",
                        headers={"X-Version": f"{rec.version.ts}:{rec.version.node}"})

    @app.put("/kv/{key}")
    async def put_value(key: str, request: Request) -> dict:
        body = await request.body()
        if len(body) > MAX_VALUE_BYTES:
            raise HTTPException(413, "value larger than 1 MiB")
        try:
            v = n().put(key.encode(), body)
        except QuorumError as exc:
            raise HTTPException(503, str(exc)) from exc
        return {"key": key, "version": f"{v.ts}:{v.node}"}

    @app.delete("/kv/{key}")
    def delete_value(key: str) -> dict:
        try:
            v = n().delete(key.encode())
        except QuorumError as exc:
            raise HTTPException(503, str(exc)) from exc
        return {"key": key, "deleted": True, "version": f"{v.ts}:{v.node}"}

    @app.post("/internal/{method}")
    async def internal(method: str, request: Request) -> Any:
        if method not in INTERNAL_METHODS:
            raise HTTPException(404, "unknown method")
        args = await request.json()
        return getattr(n(), "handle_" + method)(**args)

    return app


app = create_app()
