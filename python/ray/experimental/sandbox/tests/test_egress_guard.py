import ipaddress
import sys

import pytest

from ray.experimental.sandbox._internal import egress_guard
from ray.experimental.sandbox.config import EGRESS_ALWAYS_DENIED


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


@pytest.mark.parametrize(
    "address",
    [
        # AWS, GCP, Azure, and most others; ECS task metadata; EKS Pod
        # Identity.
        "169.254.169.254",
        "169.254.170.2",
        "169.254.170.23",
        # Alibaba Cloud, which isn't link-local.
        "100.100.100.200",
        # Azure's platform endpoint, a public address that "internet" covers.
        "168.63.129.16",
        # AWS's IPv6 metadata address.
        "fd00:ec2::254",
    ],
)
def test_always_denied_covers_cloud_metadata_services(address):
    assert any(
        ipaddress.ip_address(address) in ipaddress.ip_network(network)
        for network in EGRESS_ALWAYS_DENIED
    )


def test_collapse_merges_overlaps_per_family():
    v4, v6 = egress_guard.collapse(
        ["127.0.0.1/32", "127.0.0.0/8", "10.0.0.1/32", "10.0.0.1", "::1", "fe80::/10"]
    )
    assert v4 == ["10.0.0.1/32", "127.0.0.0/8"]
    assert v6 == ["::1/128", "fe80::/10"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
