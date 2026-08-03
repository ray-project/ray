import pytest

from ray.experimental.sandbox.config import (
    GVisorSandboxConfig,
    SandboxConfig,
    parse_memory_bytes,
)


def test_default_sandbox_config():
    config = SandboxConfig()
    assert config.image == "python:3.10-slim"
    assert config.cpu == 0.0
    assert config.memory == 0
    assert config.work_dir == "/workspace"
    assert config.ttl_seconds == 3600
    assert config.runsc_path == "runsc"
    assert config.rootless is True
    assert config.network == "none"
    assert config.resources == {}


def test_gvisor_sandbox_config():
    config = GVisorSandboxConfig(
        image="ubuntu:22.04",
        runsc_path="/usr/bin/runsc",
        cpu=2.0,
        memory="4Gi",
        env={"TEST_VAR": "value"},
        resources={"custom_res": 1.0},
    )
    assert config.image == "ubuntu:22.04"
    assert config.runsc_path == "/usr/bin/runsc"
    assert config.cpu == 2.0
    assert config.memory == "4Gi"
    assert config.env == {"TEST_VAR": "value"}
    assert config.resources == {"custom_res": 1.0}


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
