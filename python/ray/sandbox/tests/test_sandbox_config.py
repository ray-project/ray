from ray.sandbox.config import KubernetesSandboxConfig, SandboxConfig


def test_default_sandbox_config():
    config = SandboxConfig()
    assert config.backend == "kubernetes"
    assert config.image == "python:3.10-slim"
    assert config.cpu == 1.0
    assert config.memory == "1Gi"
    assert config.work_dir == "/workspace"
    assert config.ttl_seconds == 3600


def test_kubernetes_sandbox_config():
    config = KubernetesSandboxConfig(
        image="ubuntu:22.04",
        namespace="custom-namespace",
        cpu=2.0,
        memory="4Gi",
        env={"TEST_VAR": "value"},
    )
    assert config.backend == "kubernetes"
    assert config.image == "ubuntu:22.04"
    assert config.namespace == "custom-namespace"
    assert config.cpu == 2.0
    assert config.memory == "4Gi"
    assert config.env == {"TEST_VAR": "value"}
