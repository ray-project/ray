import time

import pytest
import ray
from ray import workflow
from filelock import FileLock


def test_workflow_manager_simple(workflow_start_regular):
    assert [] == workflow.list_all()
    with pytest.raises(workflow.common.WorkflowNotFoundError):
        workflow.get_status("X")


def test_workflow_manager(workflow_start_regular, tmp_path):
    # For sync between jobs
    tmp_file = str(tmp_path / "lock")
    lock = FileLock(tmp_file)
    lock.acquire()

    # For sync between jobs
    flag_file = tmp_path / "flag"
    flag_file.touch()

    @workflow.step
    def long_running(i):
        lock = FileLock(tmp_file)
        with lock.acquire():
            pass

        if i % 2 == 0:
            if flag_file.exists():
                raise ValueError()
        return 100

    outputs = [long_running.step(i).run_async(workflow_id=str(i)) for i in range(100)]
    # Test list all, it should list all jobs running
    all_tasks = workflow.list_all()
    assert len(all_tasks) == 100
    all_tasks_running = workflow.list_all(workflow.RUNNING)
    assert dict(all_tasks) == dict(all_tasks_running)
    assert workflow.get_status("0") == "RUNNING"

    # Release lock and make sure all tasks finished
    lock.release()
    for o in outputs:
        try:
            r = ray.get(o)
        except Exception:
            continue
        assert 100 == r
    all_tasks_running = workflow.list_all(workflow.WorkflowStatus.RUNNING)
    assert len(all_tasks_running) == 0
    # Half of them failed and half succeed
    failed_jobs = workflow.list_all("FAILED")
    assert len(failed_jobs) == 50
    finished_jobs = workflow.list_all("SUCCESSFUL")
    assert len(finished_jobs) == 50

    all_tasks_status = workflow.list_all(
        {
            workflow.WorkflowStatus.SUCCESSFUL,
            workflow.WorkflowStatus.FAILED,
            workflow.WorkflowStatus.RUNNING,
        }
    )
    assert len(all_tasks_status) == 100
    assert failed_jobs == [
        (k, v) for (k, v) in all_tasks_status if v == workflow.WorkflowStatus.FAILED
    ]
    assert finished_jobs == [
        (k, v) for (k, v) in all_tasks_status if v == workflow.WorkflowStatus.SUCCESSFUL
    ]

    # Test get_status
    assert workflow.get_status("0") == "FAILED"
    assert workflow.get_status("1") == "SUCCESSFUL"
    lock.acquire()
    r = workflow.resume("0")
    assert workflow.get_status("0") == workflow.RUNNING
    flag_file.unlink()
    lock.release()
    assert 100 == ray.get(r)
    assert workflow.get_status("0") == workflow.SUCCESSFUL

    # Test cancel
    lock.acquire()
    workflow.resume("2")
    assert workflow.get_status("2") == workflow.RUNNING
    workflow.cancel("2")
    assert workflow.get_status("2") == workflow.CANCELED

    # Now resume_all
    resumed = workflow.resume_all(include_failed=True)
    assert len(resumed) == 48
    lock.release()
    assert [ray.get(o) for (_, o) in resumed] == [100] * 48


@pytest.mark.parametrize("workflow_start_regular", [{"num_cpus": 4}], indirect=True)
def test_actor_manager(workflow_start_regular, tmp_path):
    lock_file = tmp_path / "lock"

    @workflow.virtual_actor
    class LockCounter:
        def __init__(self, lck):
            self.counter = 0
            self.lck = lck

        @workflow.virtual_actor.readonly
        def val(self):
            with FileLock(self.lck):
                return self.counter

        def incr(self):
            with FileLock(self.lck):
                self.counter += 1
                return self.counter

        def __getstate__(self):
            return (self.lck, self.counter)

        def __setstate__(self, state):
            self.lck, self.counter = state

    actor = LockCounter.get_or_create("counter", str(lock_file))
    ray.get(actor.ready())

    lock = FileLock(lock_file)
    lock.acquire()

    assert [("counter", workflow.SUCCESSFUL)] == workflow.list_all()

    v = actor.val.run_async()
    # Readonly function won't make the workflow running
    assert [("counter", workflow.SUCCESSFUL)] == workflow.list_all()
    lock.release()
    assert ray.get(v) == 0

    # Writer function would make the workflow running
    lock.acquire()
    v = actor.incr.run_async()
    time.sleep(2)
    assert [("counter", workflow.RUNNING)] == workflow.list_all()
    lock.release()
    assert ray.get(v) == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
