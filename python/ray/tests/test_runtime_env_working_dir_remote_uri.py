import sys
from typing import Dict, Optional, Tuple

import pytest

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

# This test requires you have AWS credentials set up (any AWS credentials will
# do, this test only accesses a public bucket).

# This package contains a subdirectory called `test_module`.
# Calling `test_module.one()` should return `2`.
HTTPS_PACKAGE_URI = "https://github.com/shrekris-anyscale/test_module/archive/a885b80879665a49d5cd4c3ebd33bb6f865644e5.zip"
S3_PACKAGE_URI = "s3://runtime-env-test/test_runtime_env.zip"
S3_WHL_PACKAGE_URI = "s3://runtime-env-test/test_module-0.0.1-py3-none-any.whl"


@pytest.fixture(scope="module")
def _start_cluster_shared_two_nodes(_start_cluster_shared):
    cluster, address = _start_cluster_shared
    cluster.add_node(num_cpus=1, runtime_env_dir_name="worker_node_runtime_resources")
    yield cluster, address


@pytest.fixture
def start_cluster_shared_two_nodes(_start_cluster_shared_two_nodes):
    """Shares a two-node cluster instance across all tests in the module.

    Shuts down Ray between test cases.
    """
    yield _start_cluster_shared_two_nodes
    ray.shutdown()


def make_task_actor(*, runtime_env: Optional[Dict]) -> Tuple:
    def _test() -> Tuple[str, Dict]:
        import test_module

        assert test_module.one() == 2

        ctx = ray.get_runtime_context()
        return ctx.get_node_id(), ctx.runtime_env

    @ray.remote(runtime_env=runtime_env)
    def test_import_task() -> Tuple[str, Dict]:
        return _test()

    @ray.remote(runtime_env=runtime_env)
    class TestImportActor:
        def test_import(self) -> Tuple[str, Dict]:
            return _test()

    return test_import_task, TestImportActor


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
def test_failure_without_runtime_env(start_cluster_shared_two_nodes):
    """Sanity checks that the test task & actor fail without a runtime_env."""
    cluster, address = start_cluster_shared_two_nodes

    task, actor = make_task_actor(runtime_env=None)
    task_obj_ref = task.remote()
    a = actor.remote()
    actor_obj_ref = a.test_import.remote()

    with pytest.raises(ModuleNotFoundError):
        ray.get(task_obj_ref)
    with pytest.raises(ModuleNotFoundError):
        ray.get(actor_obj_ref)


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
@pytest.mark.parametrize("option", ["working_dir", "py_modules"])
@pytest.mark.parametrize(
    "remote_uri",
    [HTTPS_PACKAGE_URI, S3_PACKAGE_URI, S3_WHL_PACKAGE_URI],
    ids=["https", "s3", "whl"],
)
@pytest.mark.parametrize("per_task_actor", [True, False])
def test_remote_package_uri_multi_node(
    start_cluster_shared_two_nodes, option, remote_uri, per_task_actor
):
    """Test the case where we lazily import inside a task/actor."""
    cluster, address = start_cluster_shared_two_nodes

    if option == "working_dir":
        if remote_uri.endswith(".whl"):
            pytest.skip(".whl working dir is not supported")
        env = {"working_dir": remote_uri}
    elif option == "py_modules":
        env = {"py_modules": [remote_uri]}

    if per_task_actor:
        ray.init(address)
    else:
        ray.init(address, runtime_env=env)

    node_ids = [n["NodeID"] for n in ray.nodes()]
    task, actor = make_task_actor(runtime_env=env if per_task_actor else None)

    # Run one task and one actor task pinned to each node in the cluster and verify:
    # 1) The task succeeded because the runtime_env was set up correctly.
    # 2) The task was placed on the correct node.
    # 3) The Ray runtime_context was populated with the configured runtime_env.
    task_refs = [
        task.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False)
        ).remote()
        for node_id in node_ids
    ]
    for i, task_ref in enumerate(task_refs):
        node_id, env_in_task = ray.get(task_ref)
        assert node_id == node_ids[i]
        assert env_in_task == env

    actors = [
        actor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False)
        ).remote()
        for node_id in node_ids
    ]
    actor_task_refs = [a.test_import.remote() for a in actors]
    for i, actor_task_ref in enumerate(actor_task_refs):
        node_id, env_in_task = ray.get(actor_task_ref)
        assert node_id == node_ids[i]
        assert env_in_task == env


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
