import sys
from types import SimpleNamespace

import pytest

import ray
from ray.experimental.sandbox._internal import egress_guard
from ray.experimental.sandbox.config import EGRESS_ALWAYS_DENIED


@pytest.fixture
def fresh_cache(monkeypatch):
    monkeypatch.setattr(egress_guard, "_ray_addresses_cache", None)


def test_always_denied_adds_the_kubernetes_api_and_operator_networks(monkeypatch):
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.96.0.1")
    monkeypatch.setenv(
        "RAY_SANDBOX_EGRESS_DENY", " 100.124.0.139, 10.200.0.7/16 ,,fd00::/8"
    )
    denied = egress_guard.always_denied()
    assert denied[: len(EGRESS_ALWAYS_DENIED)] == list(EGRESS_ALWAYS_DENIED)
    assert denied[len(EGRESS_ALWAYS_DENIED) :] == [
        "10.96.0.1/32",
        "100.124.0.139/32",
        "10.200.0.0/16",
        "fd00::/8",
    ]


def test_always_denied_refuses_a_bad_operator_entry(monkeypatch):
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    monkeypatch.setenv("RAY_SANDBOX_EGRESS_DENY", "10.0.0.1,apiserver.internal")
    with pytest.raises(ValueError, match="apiserver.internal"):
        egress_guard.always_denied()


def test_always_denied_without_kubernetes_is_the_static_list(monkeypatch):
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    monkeypatch.delenv("RAY_SANDBOX_EGRESS_DENY", raising=False)
    assert egress_guard.always_denied() == list(EGRESS_ALWAYS_DENIED)
    assert "169.254.0.0/16" in EGRESS_ALWAYS_DENIED


def test_collapse_merges_overlaps_per_family():
    v4, v6 = egress_guard.collapse(
        ["127.0.0.1/32", "127.0.0.0/8", "10.0.0.1/32", "10.0.0.1", "::1", "fe80::/10"]
    )
    assert v4 == ["10.0.0.1/32", "127.0.0.0/8"]
    assert v6 == ["::1/128", "fe80::/10"]


def _fake_ray(monkeypatch, nodes, gcs_address):
    calls = []

    def fake_nodes():
        calls.append(1)
        return nodes

    monkeypatch.setattr(ray, "is_initialized", lambda: True)
    monkeypatch.setattr(ray, "nodes", fake_nodes)
    monkeypatch.setattr(
        ray,
        "get_runtime_context",
        lambda: SimpleNamespace(gcs_address=gcs_address),
    )
    return calls


def test_cluster_addresses_cover_every_alive_node_and_the_gcs_host(
    monkeypatch, fresh_cache
):
    """Every alive Ray node and the GCS host (a Service name on KubeRay) are
    closed, along with this worker's own addresses."""
    monkeypatch.setattr(egress_guard, "_local_addresses", lambda: ["10.0.0.9/32"])
    _fake_ray(
        monkeypatch,
        [
            {"NodeManagerAddress": "10.0.0.84", "Alive": True},
            {"NodeManagerAddress": "10.0.0.85", "Alive": False},
            {"NodeManagerAddress": "2001:db8::5", "Alive": True},
        ],
        "localhost:6379",
    )
    addresses = egress_guard.cluster_addresses()
    assert addresses[0] == "10.0.0.9/32"
    assert "10.0.0.84/32" in addresses
    assert "2001:db8::5/128" in addresses
    assert "10.0.0.85/32" not in addresses
    # The GCS host's name resolves to its addresses.
    assert "127.0.0.1/32" in addresses or "::1/128" in addresses


def test_cluster_addresses_reuse_one_lookup_for_a_while(monkeypatch, fresh_cache):
    monkeypatch.setattr(egress_guard, "_local_addresses", lambda: [])
    calls = _fake_ray(
        monkeypatch, [{"NodeManagerAddress": "10.0.0.84", "Alive": True}], ""
    )
    now = [1000.0]
    monkeypatch.setattr(egress_guard.time, "monotonic", lambda: now[0])
    assert egress_guard.cluster_addresses() == ["10.0.0.84/32"]
    now[0] += egress_guard._RAY_ADDRESSES_TTL_SECONDS - 1
    assert egress_guard.cluster_addresses() == ["10.0.0.84/32"]
    assert len(calls) == 1
    now[0] += 2
    egress_guard.cluster_addresses()
    assert len(calls) == 2


def test_cluster_addresses_raise_when_gcs_does(monkeypatch, fresh_cache):
    """The caller refuses the sandbox rather than start it without them."""
    monkeypatch.setattr(egress_guard, "_local_addresses", lambda: [])
    monkeypatch.setattr(ray, "is_initialized", lambda: True)

    def unavailable():
        raise ConnectionError("GCS unavailable")

    monkeypatch.setattr(ray, "nodes", unavailable)
    with pytest.raises(ConnectionError):
        egress_guard.cluster_addresses()


def test_cluster_addresses_without_ray_are_the_local_ones(monkeypatch, fresh_cache):
    monkeypatch.setattr(ray, "is_initialized", lambda: False)
    monkeypatch.setattr(egress_guard, "_local_addresses", lambda: ["10.0.0.9/32"])
    assert egress_guard.cluster_addresses() == ["10.0.0.9/32"]


def test_local_addresses_list_this_hosts_interfaces():
    addresses = egress_guard._local_addresses()
    assert "127.0.0.1/32" in addresses
    assert all("/" in a for a in addresses)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
