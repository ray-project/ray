import os
import sys

import pytest

import ray
from ray.exceptions import GetTimeoutError
from ray._private.test_utils import wait_for_condition, check_local_files_gced

# This test requires you have AWS credentials set up (any AWS credentials will
# do, this test only accesses a public bucket).

# This package contains a subdirectory called `test_module`.
# Calling `test_module.one()` should return `2`.
# If you find that confusing, take it up with @jiaodong...
S3_PACKAGE_URI = "s3://runtime-env-test/test_runtime_env.zip"


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform != "linux",
    reason="Requires PR wheels built in CI, so only run on linux CI machines.",
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
        {
            "num_nodes": 1,
            "_system_config": {
                "num_workers_soft_limit": 0,
                # this delay will make worker start slow and time out
                "testing_asio_delay_us": "InternalKVGcsService.grpc_server"
                ".InternalKVGet=2000000:2000000",
                "worker_register_timeout_seconds": 1,
            },
        },
        {
            "num_nodes": 1,
            "_system_config": {
                "num_workers_soft_limit": 5,
                # this delay will make worker start slow and time out
                "testing_asio_delay_us": "InternalKVGcsService.grpc_server"
                ".InternalKVGet=2000000:2000000",
                "worker_register_timeout_seconds": 1,
            },
        },
    ],
    indirect=True,
)
@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("option", ["working_dir", "py_modules"])
def test_task_level_gc(ray_start_cluster, option):
    """Tests that task-level working_dir is GC'd when the worker exits."""

    cluster = ray_start_cluster

    soft_limit_zero = False
    worker_register_timeout = False
    system_config = cluster.list_all_nodes()[0]._ray_params._system_config
    if (
        "num_workers_soft_limit" in system_config
        and system_config["num_workers_soft_limit"] == 0
    ):
        soft_limit_zero = True
    if (
        "worker_register_timeout_seconds" in system_config
        and system_config["worker_register_timeout_seconds"] != 0
    ):
        worker_register_timeout = True

    @ray.remote
    def f():
        import test_module

        test_module.one()

    @ray.remote(num_cpus=1)
    class A:
        def check(self):
            import test_module

            test_module.one()

    if option == "working_dir":
        runtime_env = {"working_dir": S3_PACKAGE_URI}
    else:
        runtime_env = {"py_modules": [S3_PACKAGE_URI]}

    # Note: We should set a bigger timeout if downloads the s3 package slowly.
    get_timeout = 10

    # Start a task with runtime env
    if worker_register_timeout:
        with pytest.raises(GetTimeoutError):
            ray.get(f.options(runtime_env=runtime_env).remote(), timeout=get_timeout)
    else:
        ray.get(f.options(runtime_env=runtime_env).remote())
    if soft_limit_zero or worker_register_timeout:
        # Wait for worker exited and local files gced
        wait_for_condition(lambda: check_local_files_gced(cluster))
    else:
        # Local files should not be gced because of an enough soft limit.
        assert not check_local_files_gced(cluster)

    # Start a actor with runtime env
    actor = A.options(runtime_env=runtime_env).remote()
    if worker_register_timeout:
        with pytest.raises(GetTimeoutError):
            ray.get(actor.check.remote(), timeout=get_timeout)
        # Wait for worker exited and local files gced
        wait_for_condition(lambda: check_local_files_gced(cluster))
    else:
        ray.get(actor.check.remote())
        assert not check_local_files_gced(cluster)

    # Kill actor
    ray.kill(actor)
    if soft_limit_zero or worker_register_timeout:
        # Wait for worker exited and local files gced
        wait_for_condition(lambda: check_local_files_gced(cluster))
    else:
        # Local files should not be gced because of an enough soft limit.
        assert not check_local_files_gced(cluster)

    # Start a task with runtime env
    if worker_register_timeout:
        with pytest.raises(GetTimeoutError):
            ray.get(f.options(runtime_env=runtime_env).remote(), timeout=get_timeout)
    else:
        ray.get(f.options(runtime_env=runtime_env).remote())
    if soft_limit_zero or worker_register_timeout:
        # Wait for worker exited and local files gced
        wait_for_condition(lambda: check_local_files_gced(cluster))
    else:
        # Local files should not be gced because of an enough soft limit.
        assert not check_local_files_gced(cluster)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
