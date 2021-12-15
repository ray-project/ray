import os
from typing import Dict
import pytest
import sys
from ray.exceptions import RuntimeEnvSetupError
from ray._private.test_utils import (
    wait_for_condition, check_local_files_gced, generate_runtime_env_dict)
import ray

if not os.environ.get("CI"):
    # This flags turns on the local development that link against current ray
    # packages and fall back all the dependencies to current python's site.
    os.environ["RAY_RUNTIME_ENV_LOCAL_DEV_MODE"] = "1"


@pytest.mark.parametrize("field", ["conda", "pip"])
@pytest.mark.parametrize("specify_env_in_init", [False, True])
@pytest.mark.parametrize("spec_format", ["file", "python_object"])
def test_install_failure_logging(start_cluster, specify_env_in_init, field,
                                 spec_format, tmp_path, capsys):
    cluster, address = start_cluster
    using_ray_client = address.startswith("ray://")

    bad_envs: Dict[str, Dict] = {}
    bad_packages: Dict[str, str] = {}
    for scope in "init", "actor", "task":
        bad_packages[scope] = "doesnotexist" + scope
        bad_envs[scope] = generate_runtime_env_dict(
            field, spec_format, tmp_path, pip_list=[bad_packages[scope]])

    if specify_env_in_init:
        if using_ray_client:
            with pytest.raises(ConnectionAbortedError) as excinfo:
                ray.init(address, runtime_env=bad_envs["init"])
            assert bad_packages["init"] in str(excinfo.value)
        else:
            ray.init(address, runtime_env=bad_envs["init"])
            wait_for_condition(
                lambda: bad_packages["init"] in capsys.readouterr().out,
                timeout=30)
        return

    ray.init(address)

    @ray.remote(runtime_env=bad_envs["actor"])
    class A:
        pass

    a = A.remote()  # noqa

    wait_for_condition(
        lambda: bad_packages["actor"] in capsys.readouterr().out, timeout=30)

    @ray.remote(runtime_env=bad_envs["task"])
    def f():
        pass

    with pytest.raises(RuntimeEnvSetupError):
        ray.get(f.remote())

    wait_for_condition(
        lambda: bad_packages["task"] in capsys.readouterr().out, timeout=30)


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform != "linux",
    reason="Requires PR wheels built in CI, so only run on linux CI machines.")
@pytest.mark.parametrize("field", ["conda", "pip"])
@pytest.mark.parametrize("spec_format", ["file", "python_object"])
def test_actor_level_gc(start_cluster, field, spec_format, tmp_path):
    """Tests that actor-level working_dir is GC'd when the actor exits."""
    cluster, address = start_cluster

    ray.init(address)

    runtime_env = generate_runtime_env_dict(field, spec_format, tmp_path)

    @ray.remote
    class A:
        def test_import(self):
            import pip_install_test  # noqa: F401
            return True

    NUM_ACTORS = 5
    actors = [
        A.options(runtime_env=runtime_env).remote() for _ in range(NUM_ACTORS)
    ]
    ray.get([a.test_import.remote() for a in actors])
    for i in range(5):
        assert not check_local_files_gced(cluster)
        ray.kill(actors[i])
    wait_for_condition(lambda: check_local_files_gced(cluster))


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform != "linux",
    reason="Requires PR wheels built in CI, so only run on linux CI machines.")
@pytest.mark.parametrize(
    "ray_start_cluster", [
        {
            "num_nodes": 1,
            "_system_config": {
                "num_workers_soft_limit": 0,
            },
        },
        {
            "num_nodes": 1,
            "_system_config": {
                "num_workers_soft_limit": 5,
            },
        },
    ],
    indirect=True)
@pytest.mark.parametrize("field", ["conda", "pip"])
@pytest.mark.parametrize("spec_format", ["file", "python_object"])
def test_task_level_gc(ray_start_cluster, field, spec_format, tmp_path):
    """Tests that task-level working_dir is GC'd when the actor exits."""

    cluster = ray_start_cluster

    soft_limit_zero = False
    system_config = cluster.list_all_nodes()[0]._ray_params._system_config
    if "num_workers_soft_limit" in system_config and \
            system_config["num_workers_soft_limit"] == 0:
        soft_limit_zero = True

    runtime_env = generate_runtime_env_dict(field, spec_format, tmp_path)

    @ray.remote
    def f():
        import pip_install_test  # noqa: F401
        return True

    @ray.remote
    class A:
        def test_import(self):
            import pip_install_test  # noqa: F401
            return True

    # Start a task with runtime env
    ray.get(f.options(runtime_env=runtime_env).remote())
    if soft_limit_zero:
        # Wait for worker exited and local files gced
        wait_for_condition(lambda: check_local_files_gced(cluster))
    else:
        # Local files should not be gced because of an enough soft limit.
        assert not check_local_files_gced(cluster)

    # Start a actor with runtime env
    actor = A.options(runtime_env=runtime_env).remote()
    ray.get(actor.test_import.remote())
    # Local files should not be gced
    assert not check_local_files_gced(cluster)

    # Kill actor
    ray.kill(actor)
    if soft_limit_zero:
        # Wait for worker exited and local files gced
        wait_for_condition(lambda: check_local_files_gced(cluster))
    else:
        # Local files should not be gced because of an enough soft limit.
        assert not check_local_files_gced(cluster)

    # Start a task with runtime env
    ray.get(f.options(runtime_env=runtime_env).remote())
    if soft_limit_zero:
        # Wait for worker exited and local files gced
        wait_for_condition(lambda: check_local_files_gced(cluster))
    else:
        # Local files should not be gced because of an enough soft limit.
        assert not check_local_files_gced(cluster)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
