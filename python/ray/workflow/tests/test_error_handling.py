import pytest

import ray
from ray import workflow

from ray.tests.conftest import *  # noqa


def test_task_failure(workflow_start_regular_shared, tmp_path):
    @ray.remote(max_retries=10, retry_exceptions=True)
    def unstable_task_exception(n):
        v = int((tmp_path / "test").read_text())
        (tmp_path / "test").write_text(f"{v + 1}")
        if v < n:
            raise ValueError("Invalid")
        return v

    @ray.remote(max_retries=10)
    def unstable_task_crash(n):
        v = int((tmp_path / "test").read_text())
        (tmp_path / "test").write_text(f"{v + 1}")
        if v < n:
            import os

            os.kill(os.getpid(), 9)
        return v

    @ray.remote(max_retries=10, retry_exceptions=True)
    def unstable_task_crash_then_exception(n):
        v = int((tmp_path / "test").read_text())
        (tmp_path / "test").write_text(f"{v + 1}")
        if v < n / 2:
            import os

            os.kill(os.getpid(), 9)
        elif v < n:
            raise ValueError("Invalid")
        return v

    with pytest.raises(Exception):
        unstable_task_exception.options(max_retries=-2)

    for task in (
        unstable_task_exception,
        unstable_task_crash,
        unstable_task_crash_then_exception,
    ):
        (tmp_path / "test").write_text("0")
        assert workflow.run(task.bind(10)) == 10

        (tmp_path / "test").write_text("0")
        with pytest.raises(workflow.WorkflowExecutionError):
            workflow.run(task.bind(11))

    # TODO(suquark): catch crash as an exception
    for task in (unstable_task_exception, unstable_task_crash_then_exception):
        (tmp_path / "test").write_text("0")
        (ret, err) = workflow.run(
            task.options(**workflow.options(catch_exceptions=True)).bind(10)
        )
        assert ret == 10
        assert err is None

        (tmp_path / "test").write_text("0")
        (ret, err) = workflow.run(
            task.options(**workflow.options(catch_exceptions=True)).bind(11)
        )
        assert ret is None
        assert err is not None

    (tmp_path / "test").write_text("0")
    with pytest.raises(workflow.WorkflowExecutionError):
        workflow.run(unstable_task_exception.options(retry_exceptions=False).bind(10))

    (tmp_path / "test").write_text("0")
    workflow.run(unstable_task_crash.options(retry_exceptions=False).bind(10))

    (tmp_path / "test").write_text("0")
    with pytest.raises(workflow.WorkflowExecutionError):
        workflow.run(
            unstable_task_crash_then_exception.options(retry_exceptions=False).bind(10)
        )


def test_nested_catch_exception(workflow_start_regular_shared):
    @ray.remote
    def f2():
        return 10

    @ray.remote
    def f1():
        return workflow.continuation(f2.bind())

    assert (10, None) == workflow.run(
        f1.options(**workflow.options(catch_exceptions=True)).bind()
    )


def test_nested_catch_exception_2(workflow_start_regular_shared):
    @ray.remote
    def f1(n):
        if n == 0:
            raise ValueError()
        else:
            return workflow.continuation(f1.bind(n - 1))

    ret, err = workflow.run(
        f1.options(**workflow.options(catch_exceptions=True)).bind(5)
    )
    assert ret is None
    assert isinstance(err, ValueError)


def test_nested_catch_exception_3(workflow_start_regular_shared, tmp_path):
    """Test the case where the exception is not raised by the output task of
    a nested DAG."""

    @ray.remote
    def f3():
        return 10

    @ray.remote
    def f3_exc():
        raise ValueError()

    @ray.remote
    def f2(x):
        return x

    @ray.remote
    def f1(exc):
        if exc:
            return workflow.continuation(f2.bind(f3_exc.bind()))
        else:
            return workflow.continuation(f2.bind(f3.bind()))

    ret, err = workflow.run(
        f1.options(**workflow.options(catch_exceptions=True)).bind(True)
    )
    assert ret is None
    assert isinstance(err, ValueError)

    assert (10, None) == workflow.run(
        f1.options(**workflow.options(catch_exceptions=True)).bind(False)
    )


@pytest.mark.skip(
    reason="Workflow does not support 'scheduling_strategy' that is not"
    "json-serializable as Ray task options."
)
def test_disable_auto_lineage_reconstruction(ray_start_cluster, tmp_path):
    """This test makes sure that workflow tasks will not be recovered automatically
    with lineage reconstruction."""
    import time

    from filelock import FileLock

    from ray.cluster_utils import Cluster
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    cluster: Cluster = ray_start_cluster
    cluster.add_node(num_cpus=2, resources={"head": 1}, storage=str(tmp_path))
    ray.init(address=cluster.address)

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().node_id

    lock_path = str(tmp_path / "lock")

    @ray.remote
    def f1():
        v = int((tmp_path / "num_executed").read_text())
        (tmp_path / "num_executed").write_text(str(v + 1))
        import numpy as np

        return np.ones(10 ** 6)

    @ray.remote
    def f2(x):
        (tmp_path / "f2").touch()
        with FileLock(lock_path):
            return x

    def _trigger_lineage_reconstruction(with_workflow):
        (tmp_path / "f2").unlink(missing_ok=True)
        (tmp_path / "num_executed").write_text("0")

        worker_node_1 = cluster.add_node(
            num_cpus=2, resources={"worker_1": 1}, storage=str(tmp_path)
        )
        worker_node_2 = cluster.add_node(
            num_cpus=2, resources={"worker_2": 1}, storage=str(tmp_path)
        )
        worker_node_id_1 = ray.get(
            get_node_id.options(num_cpus=0, resources={"worker_1": 1}).remote()
        )
        worker_node_id_2 = ray.get(
            get_node_id.options(num_cpus=0, resources={"worker_2": 1}).remote()
        )
        dag = f2.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                worker_node_id_2, soft=True
            )
        ).bind(
            f1.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    worker_node_id_1, soft=True
                )
            ).bind()
        )

        with FileLock(lock_path):
            if with_workflow:
                ref = workflow.run_async(dag)
            else:
                ref = dag.execute()
            while not (tmp_path / "f2").exists():
                time.sleep(0.1)
            cluster.remove_node(worker_node_1, allow_graceful=False)
            cluster.remove_node(worker_node_2, allow_graceful=False)
        return ray.get(ref).sum()

    assert _trigger_lineage_reconstruction(with_workflow=False) == 10 ** 6
    assert int((tmp_path / "num_executed").read_text()) == 2

    assert _trigger_lineage_reconstruction(with_workflow=True) == 10 ** 6
    assert int((tmp_path / "num_executed").read_text()) == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
