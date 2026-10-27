"""The HTTP layer end to end: three node apps talking through HttpTransport.

Each node's peers are FastAPI TestClients (httpx clients without sockets), so
the real request/response path runs without starting servers."""
import httpx
import pytest
from fastapi.testclient import TestClient

from dkv.node import Config
from dkv.server import HttpTransport, build_node, create_app, parse_peers

IDS = ["n0", "n1", "n2"]


class Switchable(httpx.BaseTransport):
    """Routes to a node's app, or fails like a dead host when switched off."""

    def __init__(self):
        self.app_transport = None
        self.up = True

    def handle_request(self, request):
        if not self.up:
            raise httpx.ConnectError("connection refused", request=request)
        return self.app_transport.handle_request(request)


@pytest.fixture
def cluster(tmp_path):
    switches = {nid: Switchable() for nid in IDS}
    peer_clients = {nid: httpx.Client(transport=switches[nid], base_url=f"http://{nid}") for nid in IDS}
    peers = {nid: f"http://{nid}" for nid in IDS}
    clients, nodes = {}, {}
    for nid in IDS:
        node = build_node(nid, peers, str(tmp_path / nid), Config(3, 2, 2),
                          transport=HttpTransport(peers, clients=peer_clients))
        nodes[nid] = node
        client = TestClient(create_app(node, repair_seconds=0))
        client.__enter__()
        switches[nid].app_transport = client._transport
        clients[nid] = client
    yield clients, switches, nodes
    for c in clients.values():
        c.__exit__(None, None, None)


def test_parse_peers():
    assert parse_peers("a=http://a:1/, b=http://b:2") == {"a": "http://a:1", "b": "http://b:2"}
    with pytest.raises(ValueError):
        parse_peers("nourl")


def test_round_trip_and_versions(cluster):
    clients, _, _ = cluster
    v = clients["n0"].put("/kv/greeting", content=b"hello").json()["version"]
    r = clients["n2"].get("/kv/greeting")
    assert r.status_code == 200 and r.content == b"hello" and r.headers["x-version"] == v
    assert clients["n1"].delete("/kv/greeting").status_code == 200
    assert clients["n0"].get("/kv/greeting").status_code == 404
    assert clients["n0"].get("/kv/never-written").status_code == 404


def test_one_node_down_then_hint_delivered(cluster):
    clients, switches, nodes = cluster
    switches["n2"].up = False
    assert clients["n0"].put("/kv/k", content=b"v").status_code == 200   # W=2 of 3
    assert clients["n1"].get("/kv/k").content == b"v"
    assert clients["n0"].get("/health").json()["pending_hints"] == {"n2": 1}
    switches["n2"].up = True
    assert nodes["n0"].deliver_hints() == 1                              # over HTTP
    assert clients["n0"].get("/health").json()["pending_hints"] == {}
    assert nodes["n2"].store.get(b"k") == b"v"


def test_two_nodes_down_is_503(cluster):
    clients, switches, nodes = cluster
    switches["n1"].up = switches["n2"].up = False
    r = clients["n0"].put("/kv/k", content=b"v")
    assert r.status_code == 503 and "1/2" in r.json()["detail"]


def test_value_size_limit(cluster):
    clients, _, _ = cluster
    assert clients["n0"].put("/kv/big", content=b"x" * (1024 * 1024 + 1)).status_code == 413


def test_unknown_internal_method_is_404(cluster):
    clients, _, _ = cluster
    assert clients["n0"].post("/internal/close", json={}).status_code == 404
