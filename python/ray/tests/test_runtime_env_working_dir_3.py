from pathlib import Path
import os
import sys
import time
import tempfile

import pytest
from pytest_lazyfixture import lazy_fixture
from unittest import mock

import ray
from ray._private.test_utils import wait_for_condition, chdir, check_local_files_gced
import ray.experimental.internal_kv as kv
from ray._private.utils import get_directory_size_bytes


# This test requires you have AWS credentials set up (any AWS credentials will
# do, this test only accesses a public bucket).

# This package contains a subdirectory called `test_module`.
# Calling `test_module.one()` should return `2`.
# If you find that confusing, take it up with @jiaodong...
S3_PACKAGE_URI = "s3://runtime-env-test/test_runtime_env.zip"


# Set scope to "class" to force this to run before start_cluster, whose scope
# is "function".  We need these env vars to be set before Ray is started.
@pytest.fixture(scope="class")
def working_dir_and_pymodules_disable_URI_cache():
    with mock.patch.dict(
        os.environ,
        {
            "RAY_RUNTIME_ENV_WORKING_DIR_CACHE_SIZE_GB": "0",
            "RAY_RUNTIME_ENV_PY_MODULES_CACHE_SIZE_GB": "0",
        },
    ):
        print("URI caching disabled (cache size set to 0).")
        yield


@pytest.fixture(scope="class")
def URI_cache_10_MB():
    with mock.patch.dict(
        os.environ,
        {
            "RAY_RUNTIME_ENV_WORKING_DIR_CACHE_SIZE_GB": "0.01",
            "RAY_RUNTIME_ENV_PY_MODULES_CACHE_SIZE_GB": "0.01",
        },
    ):
        print("URI cache size set to 0.01 GB.")
        yield


def check_internal_kv_gced():
    return len(kv._internal_kv_list("gcs://")) == 0


class TestGC:
    @pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
    @pytest.mark.parametrize("option", ["working_dir", "py_modules"])
    @pytest.mark.parametrize(
        "source", [S3_PACKAGE_URI, lazy_fixture("tmp_working_dir")]
    )
    def test_job_level_gc(
        self,
        start_cluster,
        working_dir_and_pymodules_disable_URI_cache,
        option: str,
        source: str,
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
        self, start_cluster, working_dir_and_pymodules_disable_URI_cache, option: str
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
        self,
        start_cluster,
        working_dir_and_pymodules_disable_URI_cache,
        option: str,
        source: str,
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


@pytest.mark.skip("#23617 must be resolved for skip_local_gc to work.")
class TestSkipLocalGC:
    @pytest.mark.parametrize("source", [lazy_fixture("tmp_working_dir")])
    def test_skip_local_gc_env_var(
        self,
        skip_local_gc,
        start_cluster,
        working_dir_and_pymodules_disable_URI_cache,
        source,
    ):
        cluster, address = start_cluster
        ray.init(address, namespace="test", runtime_env={"working_dir": source})

        @ray.remote
        class A:
            def test_import(self):
                import test_module

                test_module.one()

        a = A.remote()
        ray.get(a.test_import.remote())  # Check working_dir was downloaded

        ray.shutdown()

        time.sleep(1)  # Give time for GC to potentially happen
        assert not check_local_files_gced(cluster)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
