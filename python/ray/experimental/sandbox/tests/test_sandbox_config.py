from ray.experimental.sandbox.config import GVisorSandboxConfig, SandboxConfig


def test_default_sandbox_config():
    config = SandboxConfig()
    assert config.image == "python:3.10-slim"
    assert config.cpu == 1.0
    assert config.memory == "1Gi"
    assert config.work_dir == "/workspace"
    assert config.ttl_seconds == 3600
    assert config.runsc_path == "runsc"
    assert config.rootless is True
    assert config.network == "none"


def test_gvisor_sandbox_config():
    config = GVisorSandboxConfig(
        image="ubuntu:22.04",
        runsc_path="/usr/bin/runsc",
        cpu=2.0,
        memory="4Gi",
        env={"TEST_VAR": "value"},
    )
    assert config.image == "ubuntu:22.04"
    assert config.runsc_path == "/usr/bin/runsc"
    assert config.cpu == 2.0
    assert config.memory == "4Gi"
    assert config.env == {"TEST_VAR": "value"}
