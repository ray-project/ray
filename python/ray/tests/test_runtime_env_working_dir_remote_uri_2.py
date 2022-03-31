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
@pytest.mark.parametrize("option", ["working_dir", "py_modules"])
@pytest.mark.parametrize("source", [*REMOTE_URIS, lazy_fixture("tmp_working_dir")])
def test_multi_node(start_cluster, option: str, source: str):
    """Tests that the working_dir is propagated across multi-node clusters."""
    NUM_NODES = 3
    cluster, address = start_cluster
    for i in range(NUM_NODES - 1):  # Head node already added.
        cluster.add_node(num_cpus=1, runtime_env_dir_name=f"node_{i}_runtime_resources")

    if option == "working_dir":
        ray.init(address, runtime_env={"working_dir": source})
    elif option == "py_modules":
        if source not in REMOTE_URIS:
            source = str(Path(source) / "test_module")
        ray.init(address, runtime_env={"py_modules": [source]})

    @ray.remote(num_cpus=1)
    class A:
        def check_and_get_node_id(self):
            import test_module

            test_module.one()
            return ray.get_runtime_context().node_id

    num_cpus = int(ray.available_resources()["CPU"])
    actors = [A.remote() for _ in range(num_cpus)]
    object_refs = [a.check_and_get_node_id.remote() for a in actors]
    assert len(set(ray.get(object_refs))) == NUM_NODES


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
