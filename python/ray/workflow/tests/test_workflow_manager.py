import time

import pytest
import ray
from ray import workflow
from filelock import FileLock, Timeout


def ensure_lock(lock_file):
    lock = FileLock(lock_file)

    while True:
        try:
            lock.acquire(1)
            lock.release()
            time.sleep(1)
        except Timeout:
            break


def test_workflow_manager_simple(workflow_start_regular):
    assert [] == workflow.list_all()
    with pytest.raises(ValueError):
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

    outputs = [
        long_running.step(i).run_async(workflow_id=str(i)) for i in range(100)
    ]
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

    all_tasks_status = workflow.list_all({
        workflow.WorkflowStatus.SUCCESSFUL, workflow.WorkflowStatus.FAILED,
        workflow.WorkflowStatus.RUNNING
    })
    assert len(all_tasks_status) == 100
    assert failed_jobs == [(k, v) for (k, v) in all_tasks_status
                           if v == workflow.WorkflowStatus.FAILED]
    assert finished_jobs == [(k, v) for (k, v) in all_tasks_status
                             if v == workflow.WorkflowStatus.SUCCESSFUL]

    remaining = 50
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
    remaining -= 1

    lock.acquire()
    # Test cancel
    workflow.resume("2")
    assert workflow.get_status("2") == workflow.RUNNING
    workflow.cancel("2")
    assert workflow.get_status("2") == workflow.CANCELED
    remaining -= 1

    # Now resume_all
    resumed = workflow.resume_all(include_failed=True)
    assert len(resumed) == remaining
    lock.release()
    assert [ray.get(o) for (_, o) in resumed] == [100] * remaining


@pytest.mark.parametrize(
    "workflow_start_regular", [{
        "num_cpus": 4
    }], indirect=True)
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


@pytest.mark.parametrize(
    "workflow_start_regular", [{
        "num_cpus": 1,
    }], indirect=True)
def test_workflow_cancel_simple(workflow_start_regular, tmp_path):
    tmp_file = str(tmp_path / "lock")

    # A step that will run forever
    @workflow.step
    def inf_step():
        with FileLock(tmp_file):
            while True:
                time.sleep(1)

    @workflow.step
    def a_step():
        with FileLock(tmp_file):
            return 1

    # start job 1 first and then make sure it's holding the lock
    job_1 = inf_step.step().run_async("job_1")
    ensure_lock(tmp_file)
    job_2 = a_step.step().run_async("job_2")

    assert workflow.get_status("job_1") == workflow.RUNNING
    assert workflow.get_status("job_2") == workflow.RUNNING
    # Canceled one will be turned into cancelled status
    workflow.cancel("job_1")
    assert workflow.get_status("job_1") == workflow.CANCELED
    # job_2 will hold the lock and make progress since job_1 is cancelled
    assert 1 == ray.get(job_2)
    del job_1


@pytest.mark.parametrize(
    "workflow_start_regular", [{
        "num_cpus": 1,
    }], indirect=True)
def test_workflow_cancel_nested(workflow_start_regular, tmp_path):
    tmp_file = str(tmp_path / "lock")

    # A step that will run forever
    @workflow.step
    def inf_step():
        with FileLock(tmp_file):
            while True:
                time.sleep(1)

    @workflow.step
    def a_step():
        with FileLock(tmp_file):
            return 1

    @workflow.step
    def outer_step():
        return inf_step.step()

    # start job 1 first and then make sure it's holding the lock
    job_1 = outer_step.step().run_async("job_1")
    ensure_lock(tmp_file)
    job_2 = a_step.step().run_async("job_2")

    assert workflow.get_status("job_1") == workflow.RUNNING
    assert workflow.get_status("job_2") == workflow.RUNNING
    # Canceled one will be turned into cancelled status
    workflow.cancel("job_1")
    assert workflow.get_status("job_1") == workflow.CANCELED
    # job_2 will hold the lock and make progress since job_1 is cancelled
    assert 1 == ray.get(job_2)
    del job_1


def test_workflow_cancel_parallel(workflow_start_regular, tmp_path):
    lock_files = [str(tmp_path / f"lock.{i}") for i in range(10)]

    @workflow.step(num_cpus=0)
    def inf_step(lock_file):
        with FileLock(lock_file):
            while True:
                time.sleep(1)

    @workflow.step
    def join_steps(*args):
        return

    @workflow.step
    def a_step():
        locks = [FileLock(f) for f in lock_files]
        for lock in locks:
            lock.acquire()
            lock.release()
        return 1

    input_steps = [inf_step.step(f) for f in lock_files]
    job_1 = join_steps.step(*input_steps).run_async("job_1")
    for f in lock_files:
        ensure_lock(f)

    job_2 = a_step.step().run_async("job_2")
    assert workflow.get_status("job_1") == workflow.RUNNING
    assert workflow.get_status("job_2") == workflow.RUNNING

    # Canceled one will be turned into cancelled status
    workflow.cancel("job_1")
    assert workflow.get_status("job_1") == workflow.CANCELED
    # job_2 will hold the lock and make progress since job_1 is cancelled
    assert 1 == ray.get(job_2)
    del job_1


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
