import pytest
import ray
from ray import workflow
from ray.tests.conftest import *  # noqa


def test_workflow_concurrency_limit_argument(shutdown_only):
    with pytest.raises(TypeError):
        workflow.init(1, 2)

    with pytest.raises(TypeError):
        workflow.init(max_running_workflows=1.7)

    with pytest.raises(TypeError):
        workflow.init(max_pending_workflows=1.7)

    with pytest.raises(ValueError):
        workflow.init(max_running_workflows=-2)

    with pytest.raises(ValueError):
        workflow.init(max_pending_workflows=-2)

    with pytest.raises(ValueError):
        workflow.init(max_running_workflows=0)


def test_workflow_concurrency_limit_reinit(shutdown_only):
    workflow.init(max_running_workflows=5, max_pending_workflows=6)
    workflow.init(max_running_workflows=5, max_pending_workflows=6)
    with pytest.raises(ValueError):
        workflow.init(max_running_workflows=7, max_pending_workflows=8)
    workflow.init()
    workflow.init(max_running_workflows=None, max_pending_workflows=None)


def test_workflow_queuing_1(shutdown_only, tmp_path):
    ray.init(storage=str(tmp_path))
    workflow.init(max_running_workflows=2, max_pending_workflows=2)

    import queue
    import filelock

    lock_path = str(tmp_path / ".lock")

    @ray.remote
    def long_running(x):
        with filelock.FileLock(lock_path):
            return x

    wfs = [long_running.bind(i) for i in range(5)]

    with filelock.FileLock(lock_path):
        refs = [
            workflow.run_async(wfs[i], workflow_id=f"workflow_{i}") for i in range(4)
        ]

        assert sorted(x[0] for x in workflow.list_all({workflow.RUNNING})) == [
            "workflow_0",
            "workflow_1",
        ]
        assert sorted(x[0] for x in workflow.list_all({workflow.PENDING})) == [
            "workflow_2",
            "workflow_3",
        ]

        with pytest.raises(queue.Full, match="Workflow queue has been full"):
            workflow.run(wfs[4], workflow_id="workflow_4")

    assert ray.get(refs) == [0, 1, 2, 3]
    assert workflow.run(wfs[4], workflow_id="workflow_4") == 4
    assert sorted(x[0] for x in workflow.list_all({workflow.SUCCESSFUL})) == [
        "workflow_0",
        "workflow_1",
        "workflow_2",
        "workflow_3",
        "workflow_4",
    ]
    for i in range(5):
        assert workflow.get_output(f"workflow_{i}") == i


def test_workflow_queuing_2(shutdown_only, tmp_path):
    ray.init(storage=str(tmp_path))
    workflow.init(max_running_workflows=2, max_pending_workflows=2)

    @ray.remote
    def short_running(x):
        return x

    wfs = [short_running.bind(i) for i in range(5)]
    refs = [workflow.run_async(wfs[i], workflow_id=f"workflow_{i}") for i in range(4)]
    for i in range(4):
        assert workflow.get_output(f"workflow_{i}") == i
    assert ray.get(refs) == [0, 1, 2, 3]
    assert workflow.run(wfs[4], workflow_id="workflow_4") == 4
    assert sorted(x[0] for x in workflow.list_all({workflow.SUCCESSFUL})) == [
        "workflow_0",
        "workflow_1",
        "workflow_2",
        "workflow_3",
        "workflow_4",
    ]


def test_workflow_queuing_3(shutdown_only, tmp_path):
    """This test ensures the queuing workflow is indeed pending."""
    ray.init(storage=str(tmp_path))
    workflow.init(max_running_workflows=1, max_pending_workflows=1)

    import time
    import filelock
    from ray.exceptions import GetTimeoutError

    lock_path = str(tmp_path / ".lock")

    @ray.remote
    def long_running(x):
        (tmp_path / str(x)).write_text(str(x))
        with filelock.FileLock(lock_path):
            return x

    workflow_id = "test_workflow_queuing_3"

    with filelock.FileLock(lock_path):
        wf_1 = workflow.run_async(long_running.bind(1), workflow_id=f"{workflow_id}_1")
        wf_2 = workflow.run_async(long_running.bind(2), workflow_id=f"{workflow_id}_2")
        time.sleep(5)
        assert (tmp_path / str(1)).exists()
        assert not (tmp_path / str(2)).exists()
        assert workflow.get_status(workflow_id=f"{workflow_id}_1") == workflow.RUNNING
        assert workflow.get_status(workflow_id=f"{workflow_id}_2") == workflow.PENDING
        with pytest.raises(GetTimeoutError):
            ray.get(wf_2, timeout=5)

    assert ray.get([wf_1, wf_2]) == [1, 2]


def test_workflow_queuing_resume_all(shutdown_only, tmp_path):
    ray.init(storage=str(tmp_path))
    workflow.init(max_running_workflows=2, max_pending_workflows=2)

    import queue
    import filelock

    lock_path = str(tmp_path / ".lock")

    @ray.remote
    def long_running(x):
        with filelock.FileLock(lock_path):
            return x

    wfs = [long_running.bind(i) for i in range(5)]

    with filelock.FileLock(lock_path):
        _refs = [  # noqa: F841
            workflow.run_async(wfs[i], workflow_id=f"workflow_{i}") for i in range(4)
        ]

        assert sorted(x[0] for x in workflow.list_all({workflow.RUNNING})) == [
            "workflow_0",
            "workflow_1",
        ]
        assert sorted(x[0] for x in workflow.list_all({workflow.PENDING})) == [
            "workflow_2",
            "workflow_3",
        ]

        with pytest.raises(queue.Full, match="Workflow queue has been full"):
            workflow.run(wfs[4], workflow_id="workflow_4")

        # kill all workflows
        ray.shutdown()

    ray.init(storage=str(tmp_path))
    workflow.init(max_running_workflows=2, max_pending_workflows=2)

    with filelock.FileLock(lock_path):
        workflow_ids, outputs = zip(*sorted(workflow.resume_all()))
        # We should have the same running and pending workflows, because when
        # resume_all(), running workflows have higher priority.
        assert sorted(x[0] for x in workflow.list_all({workflow.RUNNING})) == [
            "workflow_0",
            "workflow_1",
        ]
        assert sorted(x[0] for x in workflow.list_all({workflow.PENDING})) == [
            "workflow_2",
            "workflow_3",
        ]

    assert workflow_ids == (
        "workflow_0",
        "workflow_1",
        "workflow_2",
        "workflow_3",
    )

    assert ray.get(list(outputs)) == [0, 1, 2, 3]
    assert workflow.run(wfs[4], workflow_id="workflow_4") == 4
    assert sorted(x[0] for x in workflow.list_all({workflow.SUCCESSFUL})) == [
        "workflow_0",
        "workflow_1",
        "workflow_2",
        "workflow_3",
        "workflow_4",
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
