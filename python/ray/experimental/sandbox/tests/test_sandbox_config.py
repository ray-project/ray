import pytest

from ray.experimental.sandbox.config import (
    GVisorSandboxConfig,
    SandboxConfig,
    parse_memory_bytes,
)


def test_default_sandbox_config():
    config = SandboxConfig(image="python:3.10-slim")
    assert config.image == "python:3.10-slim"
    assert config.cpu == 0.0
    assert config.memory == 0
    assert config.workdir is None
    assert config.ttl_seconds == 3600
    assert config.rootless is True
    assert config.network == "none"
    assert config.resources == {}
    assert config.readonly is True

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
        resources={"custom_res": 1.0},
        readonly=False,
    )
    assert config.image == "ubuntu:22.04"
    assert config.cpu == 2.0
    assert config.memory == "4Gi"
    assert config.env == {"TEST_VAR": "value"}
    assert config.resources == {"custom_res": 1.0}
    assert config.readonly is False


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
