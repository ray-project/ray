from pathlib import Path
import sys

import pytest
from pytest_lazyfixture import lazy_fixture

import ray

# This test requires you have AWS credentials set up (any AWS credentials will
# do, this test only accesses a public bucket).

# This package contains a subdirectory called `test_module`.
# Calling `test_module.one()` should return `2`.
# If you find that confusing, take it up with @jiaodong...
HTTPS_PACKAGE_URI = "https://github.com/shrekris-anyscale/test_module/archive/HEAD.zip"
S3_PACKAGE_URI = "s3://runtime-env-test/test_runtime_env.zip"
GS_PACKAGE_URI = "gs://public-runtime-env-test/test_module.zip"
REMOTE_URIS = [HTTPS_PACKAGE_URI, S3_PACKAGE_URI]


@pytest.mark.skipif(sys.platform == "win32", reason="Fail to create temp dir.")
@pytest.mark.parametrize("remote_uri", REMOTE_URIS)
@pytest.mark.parametrize("option", ["failure", "working_dir", "py_modules"])
@pytest.mark.parametrize("per_task_actor", [True, False])
def test_remote_package_uri(start_cluster, remote_uri, option, per_task_actor):
    """Tests the case where we lazily read files or import inside a task/actor.

    In this case, the files come from a remote location.

    This tests both that this fails *without* the working_dir and that it
    passes with it.
    """
    cluster, address = start_cluster

    if option == "working_dir":
        env = {"working_dir": remote_uri}
    elif option == "py_modules":
        env = {"py_modules": [remote_uri]}

    if option == "failure" or per_task_actor:
        ray.init(address)
    else:
        ray.init(address, runtime_env=env)

    @ray.remote
    def test_import():
        import test_module

        return test_module.one()

    if option != "failure" and per_task_actor:
        test_import = test_import.options(runtime_env=env)

    if option == "failure":
        with pytest.raises(ImportError):
            ray.get(test_import.remote())
    else:
        assert ray.get(test_import.remote()) == 2

    @ray.remote
    class Actor:
        def test_import(self):
            import test_module

            return test_module.one()

    if option != "failure" and per_task_actor:
        Actor = Actor.options(runtime_env=env)

    a = Actor.remote()
    if option == "failure":
        with pytest.raises(ImportError):
            assert ray.get(a.test_import.remote()) == 2
    else:
        assert ray.get(a.test_import.remote()) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
