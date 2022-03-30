from pathlib import Path
import os
import sys
import time
import tempfile

import pytest
from pytest_lazyfixture import lazy_fixture

import ray
from ray.exceptions import GetTimeoutError
from ray._private.test_utils import wait_for_condition, chdir, check_local_files_gced
import ray.experimental.internal_kv as kv
from ray._private.utils import get_directory_size_bytes


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


def check_internal_kv_gced():
    return len(kv._internal_kv_list("gcs://")) == 0


class TestGC:
    @pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
    @pytest.mark.parametrize("option", ["working_dir", "py_modules"])
    @pytest.mark.parametrize(
        "source", [S3_PACKAGE_URI, lazy_fixture("tmp_working_dir")]
    )
    def test_job_level_gc(
        self, start_cluster, runtime_env_disable_URI_cache, option: str, source: str
    ):
        """Tests that job-level working_dir is GC'd when the job exits."""
        NUM_NODES = 3
        cluster, address = start_cluster
        for i in range(NUM_NODES - 1):  # Head node already added.
            cluster.add_node(
                num_cpus=1, runtime_env_dir_name=f"node_{i}_runtime_resources"
            )

        if option == "working_dir":
            ray.init(address, runtime_env={"working_dir": source})
        elif option == "py_modules":
            if source != S3_PACKAGE_URI:
                source = str(Path(source) / "test_module")
            ray.init(
                address,
                runtime_env={
                    "py_modules": [
                        source,
                        Path(os.path.dirname(__file__))
                        / "pip_install_test-0.5-py3-none-any.whl",
                    ]
                },
            )

        # For a local directory, the package should be in the GCS.
        # For an S3 URI, there should be nothing in the GCS because
        # it will be downloaded from S3 directly on each node.
        # In the "py_modules" case, we have specified a local wheel
        # file to be uploaded to the GCS, so we do not expect the
        # internal KV to be empty.
        if source == S3_PACKAGE_URI and option != "py_modules":
            assert check_internal_kv_gced()
        else:
            assert not check_internal_kv_gced()

        @ray.remote(num_cpus=1)
        class A:
            def test_import(self):
                import test_module

                if option == "py_modules":
                    import pip_install_test  # noqa: F401
                test_module.one()

        num_cpus = int(ray.available_resources()["CPU"])
        actors = [A.remote() for _ in range(num_cpus)]
        ray.get([a.test_import.remote() for a in actors])

        if source == S3_PACKAGE_URI and option != "py_modules":
            assert check_internal_kv_gced()
        else:
            assert not check_internal_kv_gced()
        assert not check_local_files_gced(cluster)

        ray.shutdown()

        # Need to re-connect to use internal_kv.
        ray.init(address=address)
        wait_for_condition(check_internal_kv_gced)
        wait_for_condition(lambda: check_local_files_gced(cluster))

    @pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
    @pytest.mark.parametrize("option", ["working_dir", "py_modules"])
    def test_actor_level_gc(
        self, start_cluster, runtime_env_disable_URI_cache, option: str
    ):
        """Tests that actor-level working_dir is GC'd when the actor exits."""
        NUM_NODES = 5
        cluster, address = start_cluster
        for i in range(NUM_NODES - 1):  # Head node already added.
            cluster.add_node(
                num_cpus=1, runtime_env_dir_name=f"node_{i}_runtime_resources"
            )

        ray.init(address)

        @ray.remote(num_cpus=1)
        class A:
            def check(self):
                import test_module

                test_module.one()

        if option == "working_dir":
            A = A.options(runtime_env={"working_dir": S3_PACKAGE_URI})
        else:
            A = A.options(
                runtime_env={
                    "py_modules": [
                        S3_PACKAGE_URI,
                    ]
                }
            )

        num_cpus = int(ray.available_resources()["CPU"])
        actors = [A.remote() for _ in range(num_cpus)]
        ray.get([a.check.remote() for a in actors])
        for i in range(num_cpus):
            assert not check_local_files_gced(cluster)
            ray.kill(actors[i])
        wait_for_condition(lambda: check_local_files_gced(cluster))

    @pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
    @pytest.mark.parametrize("option", ["working_dir", "py_modules"])
    @pytest.mark.parametrize(
        "source", [S3_PACKAGE_URI, lazy_fixture("tmp_working_dir")]
    )
    def test_detached_actor_gc(
        self, start_cluster, runtime_env_disable_URI_cache, option: str, source: str
    ):
        """Tests that URIs for detached actors are GC'd only when they exit."""
        cluster, address = start_cluster

        if option == "working_dir":
            ray.init(address, namespace="test", runtime_env={"working_dir": source})
        elif option == "py_modules":
            if source != S3_PACKAGE_URI:
                source = str(Path(source) / "test_module")
            ray.init(
                address,
                namespace="test",
                runtime_env={
                    "py_modules": [
                        source,
                        Path(os.path.dirname(__file__))
                        / "pip_install_test-0.5-py3-none-any.whl",
                    ]
                },
            )

        # For a local directory, the package should be in the GCS.
        # For an S3 URI, there should be nothing in the GCS because
        # it will be downloaded from S3 directly on each node.
        # In the "py_modules" case, a local wheel file will be in the GCS.
        if source == S3_PACKAGE_URI and option != "py_modules":
            assert check_internal_kv_gced()
        else:
            assert not check_internal_kv_gced()

        @ray.remote
        class A:
            def test_import(self):
                import test_module

                if option == "py_modules":
                    import pip_install_test  # noqa: F401
                test_module.one()

        a = A.options(name="test", lifetime="detached").remote()
        ray.get(a.test_import.remote())

        if source == S3_PACKAGE_URI and option != "py_modules":
            assert check_internal_kv_gced()
        else:
            assert not check_internal_kv_gced()
        assert not check_local_files_gced(cluster)

        ray.shutdown()

        ray.init(address, namespace="test")

        if source == S3_PACKAGE_URI and option != "py_modules":
            assert check_internal_kv_gced()
        else:
            assert not check_internal_kv_gced()
        assert not check_local_files_gced(cluster)

        a = ray.get_actor("test")
        ray.get(a.test_import.remote())

        ray.kill(a)
        wait_for_condition(check_internal_kv_gced)
        wait_for_condition(lambda: check_local_files_gced(cluster))

    @pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
    def test_hit_cache_size_limit(self, start_cluster, URI_cache_10_MB):
        """Test eviction happens when we exceed a nonzero (10MB) cache size."""
        NUM_NODES = 3
        cluster, address = start_cluster
        for i in range(NUM_NODES - 1):  # Head node already added.
            cluster.add_node(
                num_cpus=1, runtime_env_dir_name=f"node_{i}_runtime_resources"
            )
        with tempfile.TemporaryDirectory() as tmp_dir, chdir(tmp_dir):
            with open("test_file_1", "wb") as f:
                f.write(os.urandom(8 * 1024 * 1024))  # 8 MiB

            ray.init(address, runtime_env={"working_dir": tmp_dir})

            @ray.remote
            def f():
                pass

            ray.get(f.remote())
            ray.shutdown()

            with open("test_file_2", "wb") as f:
                f.write(os.urandom(4 * 1024 * 1024))
            os.remove("test_file_1")

            ray.init(address, runtime_env={"working_dir": tmp_dir})
            # Without the cache size limit, we would expect the local dir to be
            # 12 MB.  Since we do have a size limit, the first package must be
            # GC'ed, leaving us with 4 MB.  Sleep to give time for deletion.
            time.sleep(5)
            for node in cluster.list_all_nodes():
                local_dir = os.path.join(
                    node.get_runtime_env_dir_path(), "working_dir_files"
                )
                assert 3 < get_directory_size_bytes(local_dir) / (1024 ** 2) < 5


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
