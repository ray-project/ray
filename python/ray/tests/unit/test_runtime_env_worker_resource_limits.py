import os
import sys
from types import SimpleNamespace

import pytest

from ray._private.runtime_env import image_uri
from ray._private.runtime_env.agent import runtime_env_agent
from ray._private.runtime_env.context import RuntimeEnvContext


@pytest.mark.parametrize("cpu_quota_us", [50000, 100000, 150000])
def test_worker_resource_limits_do_not_contaminate_cached_context(
    monkeypatch, cpu_quota_us
):
    monkeypatch.setattr(
        runtime_env_agent, "validate_worker_resource_limits_support", lambda: None
    )
    base_context = RuntimeEnvContext(
        container={
            "command": ["podman", "run", "--network=host"],
            "run_options": [],
            "entrypoint": "python",
            "image": "podman://rayproject/ray:latest",
            "env_vars": {},
        }
    ).serialize()
    request = SimpleNamespace(
        worker_resource_limits=SimpleNamespace(
            cpu_period_us=100000,
            cpu_quota_us=cpu_quota_us,
            memory_bytes=256 * 1024 * 1024,
        )
    )

    serialized_context = runtime_env_agent._apply_worker_resource_limits(
        request, base_context
    )
    # Retrying against the same cached context must produce the same worker limits.
    assert (
        runtime_env_agent._apply_worker_resource_limits(request, base_context)
        == serialized_context
    )
    limited_context = RuntimeEnvContext.deserialize(serialized_context)

    assert "--cpu-period=100000" in limited_context.container["run_options"]
    assert f"--cpu-quota={cpu_quota_us}" in limited_context.container["run_options"]
    assert "--memory=268435456b" in limited_context.container["run_options"]
    assert RuntimeEnvContext.deserialize(base_context).container["run_options"] == []

    command = []
    monkeypatch.setattr(os, "execvpe", lambda file, args, env: command.extend(args))
    limited_context._exec_container_worker({}, ["/container/default_worker.py"])
    assert command == [
        "podman",
        "run",
        "--network=host",
        "--cpu-period=100000",
        f"--cpu-quota={cpu_quota_us}",
        "--memory=268435456b",
        "--entrypoint",
        "python",
        "podman://rayproject/ray:latest",
        "/container/default_worker.py",
    ]


def test_worker_resource_limits_reject_rootless_cgroup_v1(monkeypatch):
    monkeypatch.setattr(image_uri.sys, "platform", "linux")
    monkeypatch.setattr(image_uri.os, "geteuid", lambda: 1000, raising=False)
    monkeypatch.setattr(
        image_uri, "Path", lambda _: SimpleNamespace(exists=lambda: False)
    )

    with pytest.raises(RuntimeError, match="require cgroup v2"):
        image_uri.validate_worker_resource_limits_support()


def test_worker_resource_limits_reject_unrepresentable_cpu():
    request = SimpleNamespace(
        worker_resource_limits=SimpleNamespace(
            cpu_period_us=0,
            cpu_quota_us=0,
            memory_bytes=0,
            validation_error=(
                "Per-worker container CPU limits below 0.001 CPU cannot be represented"
            ),
        )
    )

    with pytest.raises(ValueError, match="below 0.001 CPU"):
        runtime_env_agent._apply_worker_resource_limits(request, "")


def test_worker_resource_limits_reject_kernel_invalid_cfs_quota():
    context = RuntimeEnvContext(
        container={"command": ["podman", "run"], "run_options": []}
    )

    with pytest.raises(ValueError, match="quota must be at least 1000"):
        image_uri.apply_worker_resource_limits(
            context,
            cpu_period_us=1_000_000,
            cpu_quota_us=999,
            memory_bytes=0,
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
