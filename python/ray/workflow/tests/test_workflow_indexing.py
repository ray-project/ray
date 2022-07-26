import pytest

from ray._private.client_mode_hook import client_mode_wrap
from ray.workflow.common import WorkflowStatus
from ray.workflow.workflow_storage import WorkflowIndexingStorage


def test_workflow_status_update(workflow_start_regular):
    # Test workflow status update is working.
    @client_mode_wrap
    def check():
        store = WorkflowIndexingStorage()
        assert not store.list_workflow()
        for i in range(100):
            assert store.load_workflow_status(workflow_id=str(i)) == WorkflowStatus.NONE

        for i in range(100):
            store.update_workflow_status(str(i), WorkflowStatus.RUNNING)

        assert sorted(store.list_workflow()) == sorted(
            [(str(i), WorkflowStatus.RUNNING) for i in range(100)]
        )

        assert sorted(store.list_workflow({WorkflowStatus.RUNNING})) == sorted(
            [(str(i), WorkflowStatus.RUNNING) for i in range(100)]
        )

        assert sorted(store.list_workflow({WorkflowStatus.RESUMABLE})) == []

        for i in range(100):
            store.update_workflow_status(str(i), WorkflowStatus.RESUMABLE)

        assert sorted(store.list_workflow({WorkflowStatus.RESUMABLE})) == sorted(
            [(str(i), WorkflowStatus.RESUMABLE) for i in range(100)]
        )

        assert sorted(store.list_workflow({WorkflowStatus.FAILED})) == []

        for i in range(100):
            store.update_workflow_status(str(i), WorkflowStatus.FAILED)

        assert sorted(store.list_workflow()) == sorted(
            [(str(i), WorkflowStatus.FAILED) for i in range(100)]
        )

        assert sorted(store.list_workflow({WorkflowStatus.FAILED})) == sorted(
            [(str(i), WorkflowStatus.FAILED) for i in range(100)]
        )

        assert sorted(store.list_workflow({WorkflowStatus.RUNNING})) == []

    check()


def test_workflow_auto_fix_status(workflow_start_regular):
    # Test workflow can recovery from corrupted status updating.
    @client_mode_wrap
    def check():
        store = WorkflowIndexingStorage()
        assert not store.list_workflow()
        # this is a hack to crash status updating
        _key_workflow_with_status = store._key_workflow_with_status
        store._key_workflow_with_status = None
        for i in range(100):
            try:
                store.update_workflow_status(str(i), WorkflowStatus.RUNNING)
            except TypeError:
                pass

        store._key_workflow_with_status = _key_workflow_with_status

        assert sorted(store.list_workflow()) == sorted(
            [(str(i), WorkflowStatus.RUNNING) for i in range(100)]
        )

        for i in range(100):
            try:
                # when update workflow, we fix failed status
                store.update_workflow_status(str(i), WorkflowStatus.RESUMABLE)
            except TypeError:
                pass

        for i in range(100):
            assert store.load_workflow_status(str(i)) == WorkflowStatus.RESUMABLE

    check()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
