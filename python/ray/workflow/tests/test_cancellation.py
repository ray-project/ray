import filelock
import pytest

import ray
from ray import workflow
from ray.exceptions import GetTimeoutError
from ray.workflow import WorkflowStatus


def test_cancellation(tmp_path, workflow_start_regular):
    lock_a = tmp_path / "lock_a"
    lock_b = tmp_path / "lock_b"

    @ray.remote
    def simple():
        with filelock.FileLock(lock_a):
            with filelock.FileLock(lock_b):
                pass

    workflow_id = "test_cancellation"

    with filelock.FileLock(lock_b):
        r = workflow.run_async(simple.bind(), workflow_id=workflow_id)
        try:
            ray.get(r, timeout=5)
        except GetTimeoutError:
            pass
        else:
            assert False

        assert workflow.get_status(workflow_id) == WorkflowStatus.RUNNING

        workflow.cancel(workflow_id)
        with pytest.raises(workflow.WorkflowCancellationError):
            ray.get(r)
        lock = filelock.FileLock(lock_a)
        lock.acquire(timeout=5)

        assert workflow.get_status(workflow_id) == WorkflowStatus.CANCELED


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
