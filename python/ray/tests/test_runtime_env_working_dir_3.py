import os
import sys
import tempfile
import time
from pathlib import Path
from unittest import mock

import pytest

import ray
import ray.experimental.internal_kv as kv
from ray._common.network_utils import find_free_port
from ray._common.test_utils import wait_for_condition
from ray._private.ray_constants import RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_ENV_VAR
from ray._private.test_utils import (
    chdir,
    check_local_files_gced,
)
from ray._private.utils import get_directory_size_bytes

# This test requires you have AWS credentials set up (any AWS credentials will
# do, this test only accesses a public bucket).

# This package contains a subdirectory called `test_module`.
# Calling `test_module.one()` should return `2`.
# If you find that confusing, take it up with @jiaodong...
S3_PACKAGE_URI = "s3://runtime-env-test/test_runtime_env.zip"

# Time to set for temporary URI before deletion.
# Set to 40s on windows and 20s on other platforms to avoid flakiness.
TEMP_URI_EXPIRATION_S = 40 if sys.platform == "win32" else 20


# Set scope to "class" to force this to run before start_cluster, whose scope
# is "function".  We need these env vars to be set before Ray is started.
@pytest.fixture(scope="class")
def working_dir_and_pymodules_disable_URI_cache():
    with mock.patch.dict(
        os.environ,
        {
            "RAY_RUNTIME_ENV_WORKING_DIR_CACHE_SIZE_GB": "0",
            "RAY_RUNTIME_ENV_PY_MODULES_CACHE_SIZE_GB": "0",
            RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_ENV_VAR: "0",
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
            RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_ENV_VAR: "0",
        },
    ):
        print("URI cache size set to 0.01 GB.")
        yield


@pytest.fixture(scope="class")
def disable_temporary_uri_pinning():
    with mock.patch.dict(
        os.environ,
        {
            RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_ENV_VAR: "0",
        },
    ):
        print("temporary URI pinning disabled.")
        yield


def check_internal_kv_gced():
    return len(kv._internal_kv_list("gcs://")) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
class TestGC:
    def test_job_level_gc(
        self,
        start_cluster,
        working_dir_and_pymodules_disable_URI_cache,
        disable_temporary_uri_pinning,
        tmp_working_dir,
    ):
        """Tests that job-level working_dir is GC'd when the job exits."""
        cluster, address = start_cluster
        cluster.add_node(
            num_cpus=1,
            runtime_env_dir_name="worker_node_runtime_resources",
            dashboard_agent_listen_port=find_free_port(),
        )

        ray.init(
            address,
            runtime_env={
                "working_dir": tmp_working_dir,
                "py_modules": [
                    S3_PACKAGE_URI,
                    str(
                        Path(os.path.dirname(__file__))
                        / "pip_install_test-0.5-py3-none-any.whl"
                    ),
                ],
            },
        )
        assert not check_internal_kv_gced()
        print("kv check 1 passed.")

        @ray.remote(num_cpus=1)
        class A:
            def test_import(self):
                import pip_install_test  # noqa: F401
                import test_module

                test_module.one()

        num_cpus = int(ray.available_resources()["CPU"])
        print(f"{num_cpus} cpus available.")

        actors = [A.remote() for _ in range(num_cpus)]
        print(f"Created {len(actors)} actors.")

        ray.get([a.test_import.remote() for a in actors])
        print("Got responses from all actors.")

        assert not check_internal_kv_gced()
        print("kv check 2 passed.")

        assert not check_local_files_gced(cluster)
        print("check_local_files_gced() check passed.")

        ray.shutdown()
        print("Ray has been shut down.")

        # Need to re-connect to use internal_kv.
        ray.init(address=address)
        print(f'Reconnected to Ray at address "{address}".')

        wait_for_condition(check_internal_kv_gced)
        print("check_internal_kv_gced passed wait_for_condition block.")

        wait_for_condition(lambda: check_local_files_gced(cluster))
        print("check_local_files_gced passed wait_for_condition block.")

    # NOTE(edoakes): I tried removing the parametrization here and setting working_dir
    # and py_modules to the same thing, but the test fails. This appears to be due to a
    # bug in the reference counting logic: we key the reference table on URI only, not
    # (option, URI), so there is a collision when both options have the same value.
    # See: https://github.com/ray-project/ray/issues/52578.
    @pytest.mark.parametrize("option", ["working_dir", "py_modules"])
    def test_actor_level_gc(
        self,
        start_cluster,
        working_dir_and_pymodules_disable_URI_cache,
        option: str,
    ):
        """Tests that actor-level working_dir is GC'd when the actor exits."""
        cluster, address = start_cluster
        cluster.add_node(
            num_cpus=1,
            runtime_env_dir_name="worker_node_runtime_resources",
            dashboard_agent_listen_port=find_free_port(),
        )
        ray.init(address)

        @ray.remote(num_cpus=1)
        class A:
            def check(self):
                import test_module

                test_module.one()

        A = A.options(
            runtime_env={
                "working_dir": S3_PACKAGE_URI,
            }
            if option == "working_dir"
            else {
                "py_modules": [
                    S3_PACKAGE_URI,
                ],
            }
        )

        num_cpus = int(ray.available_resources()["CPU"])
        print(f"{num_cpus} cpus available.")

        actors = [A.remote() for _ in range(num_cpus)]
        print(f"Created {len(actors)} actors.")

        ray.get([a.check.remote() for a in actors])
        print("Got responses from all actors.")

        for i in range(num_cpus):
            assert not check_local_files_gced(cluster)
            print(f"check_local_files_gced assertion passed for cpu {i}.")

            ray.kill(actors[i])
            print(f"Issued ray.kill for actor {i}.")

        wait_for_condition(lambda: check_local_files_gced(cluster))
        print("check_local_files_gced passed wait_for_condition block.")

    def test_detached_actor_gc(
        self,
        start_cluster,
        working_dir_and_pymodules_disable_URI_cache,
        disable_temporary_uri_pinning,
        tmp_working_dir,
    ):
        """Tests that URIs for detached actors are GC'd only when they exit."""
        cluster, address = start_cluster
        # Wait until agent is ready.
        # TODO(sang): There's a bug where is runtime env creation request is
        # sent before agent is ready, it fails. We will fix this issue soon.
        time.sleep(2)

        ray.init(
            address,
            namespace="test",
            runtime_env={
                "working_dir": tmp_working_dir,
                "py_modules": [
                    S3_PACKAGE_URI,
                    str(
                        Path(os.path.dirname(__file__))
                        / "pip_install_test-0.5-py3-none-any.whl"
                    ),
                ],
            },
        )

        assert not check_internal_kv_gced()

        @ray.remote
        class A:
            def test_import(self):
                import pip_install_test  # noqa: F401
                import test_module

                test_module.one()

        a = A.options(name="test", lifetime="detached").remote()
        print('Created detached actor with name "test".')

        ray.get(a.test_import.remote())
        print('Got response from "test" actor.')

        assert not check_internal_kv_gced()
        print("kv check 2 passed.")

        assert not check_local_files_gced(cluster)
        print("check_local_files_gced() check passed.")

        ray.shutdown()
        print("Ray has been shut down.")

        ray.init(address, namespace="test")
        print(f'Reconnected to Ray at address "{address}" and namespace "test".')

        assert not check_internal_kv_gced()
        print("kv check 3 passed.")

        assert not check_local_files_gced(cluster)
        print("check_local_files_gced() check passed.")

        a = ray.get_actor("test")
        print('Got "test" actor.')

        ray.get(a.test_import.remote())
        print('Got response from "test" actor.')

        ray.kill(a)
        print('Issued ray.kill() request to "test" actor.')

        wait_for_condition(check_internal_kv_gced)
        print("check_internal_kv_gced passed wait_for_condition block.")

        wait_for_condition(lambda: check_local_files_gced(cluster))
        print("check_local_files_gced passed wait_for_condition block.")

    def test_hit_cache_size_limit(
        self, start_cluster, URI_cache_10_MB, disable_temporary_uri_pinning
    ):
        """Test eviction happens when we exceed a nonzero (10MB) cache size."""
        cluster, address = start_cluster
        cluster.add_node(
            num_cpus=1, runtime_env_dir_name="worker_node_runtime_resources"
        )

        with tempfile.TemporaryDirectory() as tmp_dir, chdir(tmp_dir):
            print("Entered tempfile context manager.")

            with open("test_file_1", "wb") as f:
                f.write(os.urandom(8 * 1024 * 1024))  # 8 MiB
            print('Wrote random bytes to "test_file_1" file.')

            ray.init(address, runtime_env={"working_dir": tmp_dir})
            print(f'Initialized Ray at "{address}" with working_dir.')

            @ray.remote
            def f():
                pass

            ray.get(f.remote())
            print('Created and received response from task "f".')

            ray.shutdown()
            print("Ray has been shut down.")

            with open("test_file_2", "wb") as f:
                f.write(os.urandom(4 * 1024 * 1024))
            print('Wrote random bytes to "test_file_2".')

            os.remove("test_file_1")
            print('Removed "test_file_1".')

            ray.init(address, runtime_env={"working_dir": tmp_dir})
            print(f'Reinitialized Ray at address "{address}" with working_dir.')

            # Without the cache size limit, we would expect the local dir to be
            # 12 MB.  Since we do have a size limit, the first package must be
            # GC'ed, leaving us with 4 MB.

            for idx, node in enumerate(cluster.list_all_nodes()):
                local_dir = os.path.join(
                    node.get_runtime_env_dir_path(), "working_dir_files"
                )
                print("Created local_dir path.")

                def local_dir_size_near_4mb():
                    return 3 < get_directory_size_bytes(local_dir) / (1024**2) < 5

                wait_for_condition(local_dir_size_near_4mb)

                print(f"get_directory_size_bytes assertion {idx} passed.")


@pytest.mark.parametrize("expiration_s", [0, TEMP_URI_EXPIRATION_S])
def test_pin_runtime_env_uri(start_cluster, tmp_working_dir, expiration_s, monkeypatch):
    """Test that temporary GCS URI references are deleted after expiration_s."""
    monkeypatch.setenv(RAY_RUNTIME_ENV_URI_PIN_EXPIRATION_S_ENV_VAR, str(expiration_s))

    cluster, address = start_cluster

    start = time.time()
    ray.init(address, namespace="test", runtime_env={"working_dir": tmp_working_dir})

    @ray.remote
    def f():
        pass

    # Wait for runtime env to be set up. This can be accomplished by getting
    # the result of a task that depends on it.
    ray.get(f.remote())
    ray.shutdown()

    # Need to re-connect to use internal_kv.
    ray.init(address=address)

    time_until_first_check = time.time() - start
    print("Starting Internal KV checks at time ", time_until_first_check)
    assert (
        time_until_first_check < TEMP_URI_EXPIRATION_S
    ), "URI expired before we could check it. Try bumping the expiration time."
    if expiration_s > 0:
        assert not check_internal_kv_gced()
        wait_for_condition(check_internal_kv_gced, timeout=4 * expiration_s)
        time_until_gc = time.time() - start
        assert expiration_s < time_until_gc < 4 * expiration_s
        print("Internal KV was GC'ed at time ", time_until_gc)
    else:
        wait_for_condition(check_internal_kv_gced)
        print("Internal KV was GC'ed at time ", time.time() - start)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
