import os
import pytest
import sys
import time

from ray._private.test_utils import (
    wait_for_condition,
    check_local_files_gced,
    generate_runtime_env_dict,
)
import ray

from unittest import mock

if not os.environ.get("CI"):
    # This flags turns on the local development that link against current ray
    # packages and fall back all the dependencies to current python's site.
    os.environ["RAY_RUNTIME_ENV_LOCAL_DEV_MODE"] = "1"


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on windows")
def test_multiple_pip_installs(start_cluster, monkeypatch):
    """Test that multiple pip installs don't interfere with each other."""
    monkeypatch.setenv("RUNTIME_ENV_RETRY_TIMES", "0")
    cluster, address = start_cluster

    ray.init(
        address,
        runtime_env={
            "pip": ["pip-install-test"],
            "env_vars": {"TEST_VAR_1": "test_1"},
        },
    )

    @ray.remote
    def f():
        return True

    @ray.remote(
        runtime_env={
            "pip": ["pip-install-test"],
            "env_vars": {"TEST_VAR_2": "test_2"},
        }
    )
    def f2():
        return True

    @ray.remote(
        runtime_env={
            "pip": ["pip-install-test"],
            "env_vars": {"TEST_VAR_3": "test_3"},
        }
    )
    def f3():
        return True

    assert all(ray.get([f.remote(), f2.remote(), f3.remote()]))


class TestGC:
    @pytest.mark.skipif(
        os.environ.get("CI") and sys.platform != "linux",
        reason="Needs PR wheels built in CI, so only run on linux CI machines.",
    )
    @pytest.mark.parametrize("field", ["conda", "pip"])
    @pytest.mark.parametrize("spec_format", ["file", "python_object"])
    def test_actor_level_gc(
        self, runtime_env_disable_URI_cache, start_cluster, field, spec_format, tmp_path
    ):
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
    @pytest.mark.parametrize("field", ["conda", "pip"])
    @pytest.mark.parametrize("spec_format", ["file", "python_object"])
    def test_task_level_gc(
        self,
        runtime_env_disable_URI_cache,
        ray_start_cluster,
        field,
        spec_format,
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


# Set scope to "class" to force this to run before start_cluster, whose scope
# is "function".  We need these env vars to be set before Ray is started.
@pytest.fixture(scope="class")
def skip_local_gc():
    with mock.patch.dict(
        os.environ,
        {
            "RAY_RUNTIME_ENV_SKIP_LOCAL_GC": "1",
        },
    ):
        print("RAY_RUNTIME_ENV_SKIP_LOCAL_GC enabled.")
        yield


class TestSkipLocalGC:
    @pytest.mark.skipif(
        os.environ.get("CI") and sys.platform != "linux",
        reason="Requires PR wheels built in CI, so only run on linux CI machines.",
    )
    @pytest.mark.parametrize("field", ["conda", "pip"])
    def test_skip_local_gc_env_var(self, skip_local_gc, start_cluster, field, tmp_path):
        cluster, address = start_cluster
        runtime_env = generate_runtime_env_dict(field, "python_object", tmp_path)
        ray.init(address, namespace="test", runtime_env=runtime_env)

        @ray.remote
        def f():
            import pip_install_test  # noqa: F401

            return True

        assert ray.get(f.remote())

        ray.shutdown()

        # Give enough time for potentially uninstalling a conda env
        time.sleep(10)

        # Check nothing was GC'ed
        assert not check_local_files_gced(cluster)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
