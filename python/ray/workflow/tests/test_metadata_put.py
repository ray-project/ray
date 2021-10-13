import asyncio

from ray import workflow
from ray.tests.conftest import *  # noqa
from ray.workflow import workflow_storage
from ray.workflow.storage import get_global_storage

import pytest


def get_metadata(paths, is_json=True):
    store = get_global_storage()
    key = store.make_key(*paths)
    return asyncio.get_event_loop().run_until_complete(store.get(key, is_json))


def test_step_user_metadata(workflow_start_regular):

    metadata = {"k1": "v1"}
    step_name = "simple_step"
    workflow_id = "simple"

    @workflow.step(name=step_name, metadata=metadata)
    def simple():
        return 0

    simple.step().run(workflow_id)

    checkpointed_metadata = get_metadata(
        [workflow_id, "steps", step_name, workflow_storage.STEP_USER_METADATA])
    assert metadata == checkpointed_metadata


def test_step_runtime_metadata(workflow_start_regular):

    step_name = "simple_step"
    workflow_id = "simple"

    @workflow.step(name=step_name)
    def simple():
        return 0

    simple.step().run(workflow_id)

    prerun_meta = get_metadata([
        workflow_id, "steps", step_name, workflow_storage.STEP_PRERUN_METADATA
    ])
    postrun_meta = get_metadata([
        workflow_id, "steps", step_name, workflow_storage.STEP_POSTRUN_METADATA
    ])
    assert "start_time" in prerun_meta
    assert "end_time" in postrun_meta


def test_workflow_user_metadata(workflow_start_regular):

    metadata = {"k1": "v1"}
    workflow_id = "simple"

    @workflow.step
    def simple():
        return 0

    simple.step().run(workflow_id, metadata=metadata)

    checkpointed_metadata = get_metadata(
        [workflow_id, workflow_storage.WORKFLOW_USER_METADATA])
    assert metadata == checkpointed_metadata


def test_workflow_runtime_metadata(workflow_start_regular):

    workflow_id = "simple"

    @workflow.step
    def simple():
        return 0

    simple.step().run(workflow_id)

    prerun_meta = get_metadata(
        [workflow_id, workflow_storage.WORKFLOW_PRERUN_METADATA])
    postrun_meta = get_metadata(
        [workflow_id, workflow_storage.WORKFLOW_POSTRUN_METADATA])
    assert "start_time" in prerun_meta
    assert "end_time" in postrun_meta


def test_all_metadata(workflow_start_regular):

    user_step_metadata = {"k1": "v1"}
    user_run_metadata = {"k2": "v2"}
    step_name = "simple_step"
    workflow_id = "simple"

    @workflow.step
    def simple():
        return 0

    simple.options(
        name=step_name, metadata=user_step_metadata).step().run(
            workflow_id, metadata=user_run_metadata)

    checkpointed_user_step_metadata = get_metadata(
        [workflow_id, "steps", step_name, workflow_storage.STEP_USER_METADATA])
    checkpointed_user_run_metadata = get_metadata(
        [workflow_id, workflow_storage.WORKFLOW_USER_METADATA])
    checkpointed_pre_step_meta = get_metadata([
        workflow_id, "steps", step_name, workflow_storage.STEP_PRERUN_METADATA
    ])
    checkpointed_post_step_meta = get_metadata([
        workflow_id, "steps", step_name, workflow_storage.STEP_POSTRUN_METADATA
    ])
    checkpointed_pre_run_meta = get_metadata(
        [workflow_id, workflow_storage.WORKFLOW_PRERUN_METADATA])
    checkpointed_post_run_meta = get_metadata(
        [workflow_id, workflow_storage.WORKFLOW_POSTRUN_METADATA])
    assert user_step_metadata == checkpointed_user_step_metadata
    assert user_run_metadata == checkpointed_user_run_metadata
    assert "start_time" in checkpointed_pre_step_meta
    assert "start_time" in checkpointed_pre_run_meta
    assert "end_time" in checkpointed_post_step_meta
    assert "end_time" in checkpointed_post_run_meta


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
