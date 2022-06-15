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
