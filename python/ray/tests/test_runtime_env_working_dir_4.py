import os
from pathlib import Path
import sys

import pytest
from pytest_lazyfixture import lazy_fixture

import ray
from ray._private.test_utils import wait_for_condition, check_local_files_gced
from ray.exceptions import GetTimeoutError

# This test requires you have AWS credentials set up (any AWS credentials will
# do, this test only accesses a public bucket).

# This package contains a subdirectory called `test_module`.
# Calling `test_module.one()` should return `2`.
# If you find that confusing, take it up with @jiaodong...
S3_PACKAGE_URI = "s3://runtime-env-test/test_runtime_env.zip"


# TODO(architkulkarni): Deflake and reenable this test.
@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on Mac. Issue #27562")
@pytest.mark.parametrize("option", ["working_dir", "py_modules"])
@pytest.mark.parametrize("source", [S3_PACKAGE_URI, lazy_fixture("tmp_working_dir")])
def test_default_large_cache(start_cluster, option: str, source: str):
    """Check small files aren't GC'ed when using the default large cache."""
    NUM_NODES = 3
    cluster, address = start_cluster
    for i in range(NUM_NODES - 1):  # Head node already added.
        cluster.add_node(num_cpus=1, runtime_env_dir_name=f"node_{i}_runtime_resources")

    if option == "working_dir":
        ray.init(address, runtime_env={"working_dir": source})
    elif option == "py_modules":
        if source != S3_PACKAGE_URI:
            source = str(Path(source) / "test_module")
        ray.init(address, runtime_env={"py_modules": [source]})

    @ray.remote
    def f():
        pass

    # Wait for runtime env to be set up. This can be accomplished by getting
    # the result of a task.
    ray.get(f.remote())
    ray.shutdown()

    # TODO(architkulkarni): We should start several tasks with other files and
    # check that all of the files are there, since GC is only triggered
    # when we attempt to create a URI that exceeds the threshold.
    assert not check_local_files_gced(cluster)

    ray.init(address)

    @ray.remote(num_cpus=1)
    class A:
        def check(self):
            import test_module

            test_module.one()

    if option == "working_dir":
        A = A.options(runtime_env={"working_dir": S3_PACKAGE_URI})
    else:
        A = A.options(runtime_env={"py_modules": [S3_PACKAGE_URI]})

    _ = A.remote()
    ray.shutdown()
    assert not check_local_files_gced(cluster)


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
                "worker_register_timeout_seconds": 0.5,
            },
        },
        {
            "num_nodes": 1,
            "_system_config": {
                "num_workers_soft_limit": 5,
                # this delay will make worker start slow and time out
                "testing_asio_delay_us": "InternalKVGcsService.grpc_server"
                ".InternalKVGet=2000000:2000000",
                "worker_register_timeout_seconds": 0.5,
            },
        },
    ],
    indirect=True,
)
@pytest.mark.parametrize("option", ["working_dir", "py_modules"])
def test_task_level_gc(runtime_env_disable_URI_cache, ray_start_cluster, option):
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
        obj_ref = f.options(runtime_env=runtime_env).remote()
        with pytest.raises(GetTimeoutError):
            ray.get(obj_ref, timeout=get_timeout)
        ray.cancel(obj_ref)
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
    else:
        ray.get(actor.check.remote())

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
        obj_ref = f.options(runtime_env=runtime_env).remote()
        with pytest.raises(GetTimeoutError):
            ray.get(obj_ref, timeout=get_timeout)
        ray.cancel(obj_ref)
    else:
        ray.get(f.options(runtime_env=runtime_env).remote())
    if soft_limit_zero or worker_register_timeout:
        # Wait for worker exited and local files gced
        wait_for_condition(lambda: check_local_files_gced(cluster))
    else:
        # Local files should not be gced because of an enough soft limit.
        assert not check_local_files_gced(cluster)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
