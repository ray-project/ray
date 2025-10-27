import os
import sys
import tempfile
from pathlib import Path

import pytest

import ray
from ray._private.runtime_env.packaging import (
    GCS_STORAGE_MAX_SIZE,
    get_uri_for_directory,
    upload_package_if_needed,
)
from ray._private.test_utils import (
    chdir,
    run_string_as_driver,
)
from ray._private.utils import get_directory_size_bytes
from ray.exceptions import RuntimeEnvSetupError

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


def test_file_created_before_1980(shutdown_only, tmp_working_dir):
    # Make sure working_dir supports file created before 1980
    # https://github.com/ray-project/ray/issues/46379
    working_path = Path(tmp_working_dir)
    file_1970 = working_path / "1970"
    with file_1970.open(mode="w") as f:
        f.write("1970")
    os.utime(
        file_1970,
        (0, 0),
    )

    ray.init(runtime_env={"working_dir": tmp_working_dir})

    @ray.remote
    def task():
        with open("1970") as f:
            assert f.read() == "1970"

    ray.get(task.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
