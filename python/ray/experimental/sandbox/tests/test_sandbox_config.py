import re
import sys

import pytest

from ray.experimental.sandbox.config import (
    GVisorSandboxConfig,
    SandboxConfig,
    normalize_cidr_allowlist,
    parse_memory_bytes,
)


def test_default_sandbox_config():
    config = SandboxConfig(image="python:3.10-slim")
    assert config.image == "python:3.10-slim"
    assert config.cpu == 0.0
    assert config.memory == 0
    assert config.workdir is None
    assert config.ttl_seconds is None
    assert config.rootless is True
    assert config.network == "none"
    assert config.readonly is True
    assert config.shell == "/bin/bash"
    assert config._ignore_cgroups is False

    # SandboxConfig requires image
    with pytest.raises(TypeError):
        SandboxConfig()

    with pytest.raises(ValueError):
        SandboxConfig(image="")

    with pytest.raises(ValueError):
        SandboxConfig(image=None)


def test_gvisor_sandbox_config():
    config = GVisorSandboxConfig(
        image="ubuntu:22.04",
        cpu=2.0,
        memory="4Gi",
        env={"TEST_VAR": "value"},
        readonly=False,
        _ignore_cgroups=True,
    )
    assert config.image == "ubuntu:22.04"
    assert config.cpu == 2.0
    assert config.memory == "4Gi"
    assert config.env == {"TEST_VAR": "value"}
    assert config.readonly is False
    assert config._ignore_cgroups is True


def test_capabilities_config():
    config = SandboxConfig(image="python:3.10-slim")
    assert config.capabilities is None

    config = SandboxConfig(
        image="python:3.10-slim", capabilities=["CAP_CHOWN", "CAP_SETUID"]
    )
    assert config.capabilities == ["CAP_CHOWN", "CAP_SETUID"]


def test_invalid_network_mode_rejected():
    with pytest.raises(ValueError, match="network mode"):
        SandboxConfig(image="python:3.10-slim", network="bridge")

    for mode in ("none", "public", "host"):
        assert SandboxConfig(image="python:3.10-slim", network=mode).network == mode
    # "sandbox" additionally requires rootless=False (see the test below).
    config = SandboxConfig(image="python:3.10-slim", network="sandbox", rootless=False)
    assert config.network == "sandbox"


def test_dns_only_valid_with_host_side_networking():
    for mode in ("public", "host"):
        config = SandboxConfig(image="python:3.10-slim", network=mode, dns=["10.0.0.2"])
        assert config.dns == ["10.0.0.2"]

    with pytest.raises(ValueError, match="dns"):
        SandboxConfig(image="python:3.10-slim", dns=["8.8.8.8"])

    with pytest.raises(ValueError, match="dns"):
        SandboxConfig(
            image="python:3.10-slim",
            network="sandbox",
            rootless=False,
            dns=["8.8.8.8"],
        )


def test_cidr_allowlist_only_with_public_network_and_normalized():
    config = SandboxConfig(
        image="python:3.10-slim",
        network="public",
        cidr_allowlist=["10.0.1.5/24", "2001:db8::1"],
    )
    # Host bits are cleared; bare addresses become single-host networks.
    assert config.cidr_allowlist == ["10.0.1.0/24", "2001:db8::1/128"]
    empty = SandboxConfig(image="python:3.10-slim", network="public", cidr_allowlist=[])
    assert empty.cidr_allowlist == []
    assert (
        SandboxConfig(image="python:3.10-slim", network="public").cidr_allowlist is None
    )

    for mode, rootless in (("none", True), ("host", True), ("sandbox", False)):
        with pytest.raises(ValueError, match="cidr_allowlist"):
            SandboxConfig(
                image="python:3.10-slim",
                network=mode,
                rootless=rootless,
                cidr_allowlist=["10.0.0.0/8"],
            )


@pytest.mark.parametrize(
    "entry", ["example.com", "", "10.0.0.0/33", "10.0.0.256", "not a cidr"]
)
def test_cidr_allowlist_rejects_non_ip_entries(entry):
    """Entries feed an nftables ruleset, so anything but an IP literal is
    refused up front, naming the entry."""
    with pytest.raises(ValueError, match=re.escape(repr(entry))):
        SandboxConfig(
            image="python:3.10-slim", network="public", cidr_allowlist=[entry]
        )


def test_normalize_cidr_allowlist():
    assert normalize_cidr_allowlist(["0.0.0.0/0", "::/0"]) == ["0.0.0.0/0", "::/0"]
    assert normalize_cidr_allowlist([" 52.0.0.1 "]) == ["52.0.0.1/32"]
    assert normalize_cidr_allowlist(()) == []
    # A bare string is a mistake, not a one-entry list.
    with pytest.raises(ValueError, match="list"):
        normalize_cidr_allowlist("10.0.0.0/8")


def test_sandbox_network_requires_rootful():
    with pytest.raises(ValueError, match="rootless"):
        SandboxConfig(image="python:3.10-slim", network="sandbox")

    config = SandboxConfig(image="python:3.10-slim", network="sandbox", rootless=False)
    assert config.network == "sandbox"


def test_parse_memory_bytes():
    assert parse_memory_bytes("1Gi") == 1073741824
    assert parse_memory_bytes("1GiB") == 1073741824
    assert parse_memory_bytes("512Mi") == 536870912
    assert parse_memory_bytes("100Ki") == 102400
    assert parse_memory_bytes("2GB") == 2000000000
    assert parse_memory_bytes("500MB") == 500000000
    assert parse_memory_bytes(1024) == 1024
    assert parse_memory_bytes(None) is None
    assert parse_memory_bytes("") is None

    with pytest.raises(ValueError):
        parse_memory_bytes("invalid_format")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
