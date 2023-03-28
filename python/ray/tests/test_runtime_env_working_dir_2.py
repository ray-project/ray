import os
from pathlib import Path
import sys
import tempfile

import pytest
from ray._private.test_utils import run_string_as_driver

import ray
from ray._private.test_utils import chdir
from ray._private.runtime_env import RAY_WORKER_DEV_EXCLUDES
from ray._private.runtime_env.packaging import GCS_STORAGE_MAX_SIZE
from ray.exceptions import RuntimeEnvSetupError
from ray._private.runtime_env.packaging import (
    get_uri_for_directory,
    upload_package_if_needed,
)
from ray._private.utils import get_directory_size_bytes

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


def test_concurrent_downloads(shutdown_only):
    # https://github.com/ray-project/ray/issues/30369
    def upload_dir(tmpdir):
        # Create an arbitrary nonempty directory to upload.
        path = Path(tmpdir)
        dir_to_upload = path / "dir_to_upload"
        dir_to_upload.mkdir(parents=True)
        filepath = dir_to_upload / "file"
        with filepath.open("w") as file:
            file.write("F" * 100)

        uri = get_uri_for_directory(dir_to_upload)
        assert get_directory_size_bytes(dir_to_upload) > 0

        uploaded = upload_package_if_needed(uri, tmpdir, dir_to_upload)
        assert uploaded
        return uri

    ray.init()

    tmpdir = tempfile.mkdtemp()
    uri = upload_dir(tmpdir)

    @ray.remote(runtime_env={"working_dir": uri, "env_vars": {"A": "B"}})
    def f():
        print("f")

    @ray.remote(runtime_env={"working_dir": uri})
    def g():
        print("g")

    refs = [f.remote(), g.remote()]
    ray.get(refs)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
