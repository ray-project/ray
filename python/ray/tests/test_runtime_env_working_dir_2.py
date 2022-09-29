import os
from pathlib import Path
import sys
import tempfile

import pytest
from pytest_lazyfixture import lazy_fixture
from ray._private.test_utils import run_string_as_driver

import ray
from ray._private.test_utils import wait_for_condition, chdir, check_local_files_gced
from ray._private.runtime_env import RAY_WORKER_DEV_EXCLUDES
from ray._private.runtime_env.packaging import GCS_STORAGE_MAX_SIZE
from ray.exceptions import GetTimeoutError, RuntimeEnvSetupError

# This test requires you have AWS credentials set up (any AWS credentials will
# do, this test only accesses a public bucket).

# This package contains a subdirectory called `test_module`.
# Calling `test_module.one()` should return `2`.
# If you find that confusing, take it up with @jiaodong...
S3_PACKAGE_URI = "s3://runtime-env-test/test_runtime_env.zip"


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


@pytest.mark.parametrize("option", ["working_dir", "py_modules"])
def test_large_file_boundary(shutdown_only, tmp_path, option: str):
    """Check that packages just under the max size work as expected."""
    # To support Windows we do not use 'with tempfile' as that results in a recursion
    # error (See: https://bugs.python.org/issue42796). Instead we use the tmp_path
    # fixture that will clean up the files at a later time.
    with chdir(tmp_path):
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


@pytest.mark.parametrize("option", ["working_dir", "py_modules"])
def test_large_file_error(shutdown_only, tmp_path, option: str):
    with chdir(tmp_path):
        # Write to two separate files, each of which is below the threshold to
        # make sure the error is for the full package size.
        size = GCS_STORAGE_MAX_SIZE // 2 + 1
        with open("test_file_1", "wb") as f:
            f.write(os.urandom(size))

        with open("test_file_2", "wb") as f:
            f.write(os.urandom(size))

        with pytest.raises(RuntimeEnvSetupError):
            if option == "working_dir":
                ray.init(runtime_env={"working_dir": "."})
            else:
                ray.init(runtime_env={"py_modules": ["."]})


@pytest.mark.parametrize("option", ["working_dir", "py_modules"])
def test_large_dir_upload_message(start_cluster, option):
    cluster, address = start_cluster
    with tempfile.TemporaryDirectory() as tmp_dir:
        filepath = os.path.join(tmp_dir, "test_file.txt")

        # Ensure forward slashes to prevent escapes in the text passed
        # to stdin (for Windows tests this matters)
        tmp_dir = str(Path(tmp_dir).as_posix())

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


# TODO(architkulkarni): Deflake and reenable this test.
@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on Mac. Issue #27562")
@pytest.mark.skipif(sys.platform != "darwin", reason="Package exceeds max size.")
def test_ray_worker_dev_flow(start_cluster):
    cluster, address = start_cluster
    ray.init(
        address, runtime_env={"py_modules": [ray], "excludes": RAY_WORKER_DEV_EXCLUDES}
    )

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
            return (0.1 + alpha * step / 100) ** (-1) + beta * 0.1

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
                "beta": tune.choice([1, 2, 3]),
            },
        )

        print("Best config: ", analysis.get_best_config(metric="mean_loss", mode="min"))

    assert ray.get(test_tune.remote()) != serve.__path__[0]


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
# TODO(architkulkarni): Deflake and reenable this test.
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
