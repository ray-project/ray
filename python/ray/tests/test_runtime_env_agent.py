import os
import sys
import time

import pytest

import ray


conflict_port = 34567
configured_test_port = 34568


def run_tasks_without_runtime_env():
    assert ray.is_initialized()

    @ray.remote
    def f():
        pass

    for _ in range(10):
        time.sleep(1)
        ray.get(f.remote())


def run_tasks_with_runtime_env():
    assert ray.is_initialized()

    @ray.remote(runtime_env={"pip": ["pip-install-test==0.5"]})
    def f():
        import pip_install_test  # noqa

        pass

    for _ in range(3):
        time.sleep(1)
        ray.get(f.remote())


@pytest.mark.skipif(
    sys.platform == "win32", reason="`runtime_env` with `pip` not supported on Windows."
)
@pytest.mark.parametrize(
    "listen_port",
    [conflict_port],
    indirect=True,
)
@pytest.mark.parametrize(
    "call_ray_start",
    [f"ray start --head --num-cpus=1 --runtime-env-agent-port={conflict_port}"],
    indirect=True,
)
def test_runtime_env_agent_grpc_port_conflict(listen_port, call_ray_start):
    address = call_ray_start
    ray.init(address=address)

    # Tasks without runtime env still work when runtime env agent grpc port conflicts.
    run_tasks_without_runtime_env()
    # Tasks with runtime env couldn't work.
    with pytest.raises(
        ray.exceptions.RuntimeEnvSetupError,
        match="Ray agent couldn't be started due to the port conflict",
    ):
        run_tasks_with_runtime_env()


if __name__ == "__main__":
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
