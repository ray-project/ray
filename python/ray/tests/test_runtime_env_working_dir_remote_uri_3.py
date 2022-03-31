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
@pytest.mark.parametrize("working_dir", [*REMOTE_URIS, lazy_fixture("tmp_working_dir")])
def test_runtime_context(start_cluster, working_dir):
    """Tests that the working_dir is propagated in the runtime_context."""
    cluster, address = start_cluster
    ray.init(runtime_env={"working_dir": working_dir})

    def check():
        wd = ray.get_runtime_context().runtime_env["working_dir"]
        if working_dir in REMOTE_URIS:
            assert wd == working_dir
        else:
            assert wd.startswith("gcs://_ray_pkg_")

    check()

    @ray.remote
    def task():
        check()

    ray.get(task.remote())

    @ray.remote
    class Actor:
        def check(self):
            check()

    a = Actor.remote()
    ray.get(a.check.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
