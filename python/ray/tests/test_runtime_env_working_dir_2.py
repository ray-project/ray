import os
from pathlib import Path
import sys
import time
import tempfile

import pytest
from pytest_lazyfixture import lazy_fixture
from unittest import mock
from ray._private.test_utils import run_string_as_driver

import ray
import ray.experimental.internal_kv as kv
from ray._private.test_utils import (wait_for_condition, chdir,
                                     check_local_files_gced)
from ray._private.runtime_env import RAY_WORKER_DEV_EXCLUDES
from ray._private.runtime_env.packaging import GCS_STORAGE_MAX_SIZE
# This test requires you have AWS credentials set up (any AWS credentials will
# do, this test only accesses a public bucket).

# This package contains a subdirectory called `test_module`.
# Calling `test_module.one()` should return `2`.
# If you find that confusing, take it up with @jiaodong...
S3_PACKAGE_URI = "s3://runtime-env-test/test_runtime_env.zip"


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("option", ["working_dir", "py_modules"])
def test_inheritance(start_cluster, option: str):
    """Tests that child tasks/actors inherit URIs properly."""
    cluster, address = start_cluster
    with tempfile.TemporaryDirectory() as tmpdir, chdir(tmpdir):
        with open("hello", "w") as f:
            f.write("world")

        if option == "working_dir":
            ray.init(address, runtime_env={"working_dir": "."})
        elif option == "py_modules":
            ray.init(address, runtime_env={"py_modules": ["."]})

        @ray.remote
        def get_env():
            return ray.get_runtime_context().runtime_env

        @ray.remote
        class EnvGetter:
            def get(self):
                return ray.get_runtime_context().runtime_env

        job_env = ray.get_runtime_context().runtime_env
        assert ray.get(get_env.remote()) == job_env
        eg = EnvGetter.remote()
        assert ray.get(eg.get.remote()) == job_env

        # Passing a new URI should work.
        if option == "working_dir":
            env = {"working_dir": S3_PACKAGE_URI}
        elif option == "py_modules":
            env = {"py_modules": [S3_PACKAGE_URI]}

        new_env = ray.get(get_env.options(runtime_env=env).remote())
        assert new_env != job_env
        eg = EnvGetter.options(runtime_env=env).remote()
        assert ray.get(eg.get.remote()) != job_env

        # Passing a local directory should not work.
        if option == "working_dir":
            env = {"working_dir": "."}
        elif option == "py_modules":
            env = {"py_modules": ["."]}
        with pytest.raises(ValueError):
            get_env.options(runtime_env=env).remote()
        with pytest.raises(ValueError):
            EnvGetter.options(runtime_env=env).remote()


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("option", ["working_dir", "py_modules"])
def test_large_file_boundary(shutdown_only, option: str):
    """Check that packages just under the max size work as expected."""
    with tempfile.TemporaryDirectory() as tmp_dir, chdir(tmp_dir):
        size = GCS_STORAGE_MAX_SIZE - 1024 * 1024
        with open("test_file", "wb") as f:
            f.write(os.urandom(size))

        if option == "working_dir":
            ray.init(runtime_env={"working_dir": "."})
        else:
            ray.init(runtime_env={"py_modules": ["."]})

        @ray.remote
        class Test:
            def get_size(self):
                with open("test_file", "rb") as f:
                    return len(f.read())

        t = Test.remote()
        assert ray.get(t.get_size.remote()) == size


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("option", ["working_dir", "py_modules"])
def test_large_file_error(shutdown_only, option: str):
    with tempfile.TemporaryDirectory() as tmp_dir, chdir(tmp_dir):
        # Write to two separate files, each of which is below the threshold to
        # make sure the error is for the full package size.
        size = GCS_STORAGE_MAX_SIZE // 2 + 1
        with open("test_file_1", "wb") as f:
            f.write(os.urandom(size))

        with open("test_file_2", "wb") as f:
            f.write(os.urandom(size))

        with pytest.raises(RuntimeError):
            if option == "working_dir":
                ray.init(runtime_env={"working_dir": "."})
            else:
                ray.init(runtime_env={"py_modules": ["."]})


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("option", ["working_dir", "py_modules"])
def test_large_dir_upload_message(start_cluster, option):
    cluster, address = start_cluster
    with tempfile.TemporaryDirectory() as tmp_dir:
        filepath = os.path.join(tmp_dir, "test_file.txt")
        if option == "working_dir":
            driver_script = f"""
import ray
ray.init("{address}", runtime_env={{"working_dir": "{tmp_dir}"}})
"""
        else:
            driver_script = f"""
import ray
ray.init("{address}", runtime_env={{"py_modules": ["{tmp_dir}"]}})
"""

        with open(filepath, "w") as f:
            f.write("Hi")

        output = run_string_as_driver(driver_script)
        assert "Pushing file package" in output
        assert "Successfully pushed file package" in output
        assert "warning" not in output.lower()


@pytest.mark.skipif(
    sys.platform != "darwin", reason="Package exceeds max size.")
def test_ray_worker_dev_flow(start_cluster):
    cluster, address = start_cluster
    ray.init(
        address,
        runtime_env={
            "py_modules": [ray],
            "excludes": RAY_WORKER_DEV_EXCLUDES
        })

    @ray.remote
    def get_captured_ray_path():
        return [ray.__path__]

    @ray.remote
    def get_lazy_ray_path():
        import ray
        return [ray.__path__]

    captured_path = ray.get(get_captured_ray_path.remote())
    lazy_path = ray.get(get_lazy_ray_path.remote())
    assert captured_path == lazy_path
    assert captured_path != ray.__path__[0]

    @ray.remote
    def test_recursive_task():
        @ray.remote
        def inner():
            return [ray.__path__]

        return ray.get(inner.remote())

    assert ray.get(test_recursive_task.remote()) == captured_path

    @ray.remote
    def test_recursive_actor():
        @ray.remote
        class A:
            def get(self):
                return [ray.__path__]

        a = A.remote()
        return ray.get(a.get.remote())

    assert ray.get(test_recursive_actor.remote()) == captured_path

    from ray import serve

    @ray.remote
    def test_serve():
        serve.start()

        @serve.deployment
        def f():
            return "hi"

        f.deploy()
        h = f.get_handle()

        assert ray.get(h.remote()) == "hi"

        f.delete()
        return [serve.__path__]

    assert ray.get(test_serve.remote()) != serve.__path__[0]

    from ray import tune

    @ray.remote
    def test_tune():
        def objective(step, alpha, beta):
            return (0.1 + alpha * step / 100)**(-1) + beta * 0.1

        def training_function(config):
            # Hyperparameters
            alpha, beta = config["alpha"], config["beta"]
            for step in range(10):
                intermediate_score = objective(step, alpha, beta)
                tune.report(mean_loss=intermediate_score)

        analysis = tune.run(
            training_function,
            config={
                "alpha": tune.grid_search([0.001, 0.01, 0.1]),
                "beta": tune.choice([1, 2, 3])
            })

        print("Best config: ",
              analysis.get_best_config(metric="mean_loss", mode="min"))

    assert ray.get(test_tune.remote()) != serve.__path__[0]


def check_internal_kv_gced():
    return len(kv._internal_kv_list("gcs://")) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("option", ["working_dir", "py_modules"])
@pytest.mark.parametrize(
    "source", [S3_PACKAGE_URI, lazy_fixture("tmp_working_dir")])
def test_job_level_gc(start_cluster, option: str, source: str):
    """Tests that job-level working_dir is GC'd when the job exits."""
    NUM_NODES = 3
    cluster, address = start_cluster
    for i in range(NUM_NODES - 1):  # Head node already added.
        cluster.add_node(
            num_cpus=1, runtime_env_dir_name=f"node_{i}_runtime_resources")

    if option == "working_dir":
        ray.init(address, runtime_env={"working_dir": source})
    elif option == "py_modules":
        if source != S3_PACKAGE_URI:
            source = str(Path(source) / "test_module")
        ray.init(address, runtime_env={"py_modules": [source]})

    # For a local directory, the package should be in the GCS.
    # For an S3 URI, there should be nothing in the GCS because
    # it will be downloaded from S3 directly on each node.
    if source == S3_PACKAGE_URI:
        assert check_internal_kv_gced()
    else:
        assert not check_internal_kv_gced()

    @ray.remote(num_cpus=1)
    class A:
        def test_import(self):
            import test_module
            test_module.one()

    num_cpus = int(ray.available_resources()["CPU"])
    actors = [A.remote() for _ in range(num_cpus)]
    ray.get([a.test_import.remote() for a in actors])

    if source == S3_PACKAGE_URI:
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
def test_actor_level_gc(start_cluster, option: str):
    """Tests that actor-level working_dir is GC'd when the actor exits."""
    NUM_NODES = 5
    cluster, address = start_cluster
    for i in range(NUM_NODES - 1):  # Head node already added.
        cluster.add_node(
            num_cpus=1, runtime_env_dir_name=f"node_{i}_runtime_resources")

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
    "source", [S3_PACKAGE_URI, lazy_fixture("tmp_working_dir")])
def test_detached_actor_gc(start_cluster, option: str, source: str):
    """Tests that URIs for detached actors are GC'd only when they exit."""
    cluster, address = start_cluster

    if option == "working_dir":
        ray.init(
            address, namespace="test", runtime_env={"working_dir": source})
    elif option == "py_modules":
        if source != S3_PACKAGE_URI:
            source = str(Path(source) / "test_module")
        ray.init(
            address, namespace="test", runtime_env={"py_modules": [source]})

    # For a local directory, the package should be in the GCS.
    # For an S3 URI, there should be nothing in the GCS because
    # it will be downloaded from S3 directly on each node.
    if source == S3_PACKAGE_URI:
        assert check_internal_kv_gced()
    else:
        assert not check_internal_kv_gced()

    @ray.remote
    class A:
        def test_import(self):
            import test_module
            test_module.one()

    a = A.options(name="test", lifetime="detached").remote()
    ray.get(a.test_import.remote())

    if source == S3_PACKAGE_URI:
        assert check_internal_kv_gced()
    else:
        assert not check_internal_kv_gced()
    assert not check_local_files_gced(cluster)

    ray.shutdown()

    ray.init(address, namespace="test")

    if source == S3_PACKAGE_URI:
        assert check_internal_kv_gced()
    else:
        assert not check_internal_kv_gced()
    assert not check_local_files_gced(cluster)

    a = ray.get_actor("test")
    ray.get(a.test_import.remote())

    ray.kill(a)
    wait_for_condition(check_internal_kv_gced)
    wait_for_condition(lambda: check_local_files_gced(cluster))


# Set scope to "class" to force this to run before start_cluster, whose scope
# is "function".  We need these env vars to be set before Ray is started.
@pytest.fixture(scope="class")
def skip_local_gc():
    with mock.patch.dict(os.environ, {
            "RAY_runtime_env_skip_local_gc": "1",
    }):
        print("RAY_runtime_env_skip_local_gc enabled.")
        yield


class TestSkipLocalGC:
    @pytest.mark.parametrize("source", [lazy_fixture("tmp_working_dir")])
    def test_skip_local_gc_env_var(self, skip_local_gc, start_cluster, source):
        cluster, address = start_cluster
        ray.init(
            address, namespace="test", runtime_env={"working_dir": source})

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
