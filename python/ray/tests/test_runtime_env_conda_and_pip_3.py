import os
import sys

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import (
    check_local_files_gced,
    generate_runtime_env_dict,
)

if not os.environ.get("CI"):
    # This flags turns on the local development that link against current ray
    # packages and fall back all the dependencies to current python's site.
    os.environ["RAY_RUNTIME_ENV_LOCAL_DEV_MODE"] = "1"


class TestGC:
    @pytest.mark.skipif(
        os.environ.get("CI") and sys.platform != "linux",
        reason="Needs PR wheels built in CI, so only run on linux CI machines.",
    )
    def test_actor_level_gc(
        self, runtime_env_disable_URI_cache, start_cluster, tmp_path
    ):
        """Tests that actor-level working_dir is GC'd when the actor exits."""
        cluster, address = start_cluster

        ray.init(address)

        conda_runtime_env = generate_runtime_env_dict(
            "conda", "python_object", tmp_path
        )
        pip_runtime_env = generate_runtime_env_dict("pip", "python_object", tmp_path)

        @ray.remote
        class A:
            def test_import(self):
                import pip_install_test  # noqa: F401

                return True

        conda_actors = [
            A.options(runtime_env=conda_runtime_env).remote() for _ in range(2)
        ]
        pip_actors = [A.options(runtime_env=pip_runtime_env).remote() for _ in range(2)]
        ray.get([a.test_import.remote() for a in conda_actors + pip_actors])
        for a in conda_actors + pip_actors:
            assert not check_local_files_gced(cluster)
            ray.kill(a)

        wait_for_condition(lambda: check_local_files_gced(cluster), timeout=30)

    @pytest.mark.skipif(
        os.environ.get("CI") and sys.platform != "linux",
        reason="Needs PR wheels built in CI, so only run on linux CI machines.",
    )
    @pytest.mark.parametrize(
        "ray_start_cluster",
        [
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
        indirect=True,
    )
    def test_task_level_gc(
        self,
        runtime_env_disable_URI_cache,
        ray_start_cluster,
        tmp_path,
    ):
        """Tests that task-level working_dir is GC'd when the task exits."""

        cluster = ray_start_cluster

        soft_limit_zero = False
        system_config = cluster.list_all_nodes()[0]._ray_params._system_config
        if (
            "num_workers_soft_limit" in system_config
            and system_config["num_workers_soft_limit"] == 0
        ):
            soft_limit_zero = True

        conda_runtime_env = generate_runtime_env_dict(
            "conda", "python_object", tmp_path
        )
        pip_runtime_env = generate_runtime_env_dict("pip", "python_object", tmp_path)

        @ray.remote
        def f():
            import pip_install_test  # noqa: F401

            return True

        @ray.remote
        class A:
            def test_import(self):
                import pip_install_test  # noqa: F401

                return True

        # Start one task with each runtime_env (conda and pip).
        ray.get(f.options(runtime_env=conda_runtime_env).remote())
        ray.get(f.options(runtime_env=pip_runtime_env).remote())
        if soft_limit_zero:
            # Wait for the worker to exit and the local files to be GC'd.
            wait_for_condition(lambda: check_local_files_gced(cluster))
        else:
            # Local files should not be GC'd because there is enough soft limit.
            assert not check_local_files_gced(cluster)

        # Start one actor with each runtime env (conda and pip).
        conda_actor = A.options(runtime_env=conda_runtime_env).remote()
        pip_actor = A.options(runtime_env=pip_runtime_env).remote()
        ray.get([conda_actor.test_import.remote(), pip_actor.test_import.remote()])

        # Local files should not be GC'd.
        assert not check_local_files_gced(cluster)

        # Kill the actors
        ray.kill(conda_actor)
        ray.kill(pip_actor)
        if soft_limit_zero:
            # Wait for the workers to exit and the local files to be GC'd.
            wait_for_condition(lambda: check_local_files_gced(cluster))
        else:
            # Local files should not be GC'd because there is enough soft limit.
            assert not check_local_files_gced(cluster)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
