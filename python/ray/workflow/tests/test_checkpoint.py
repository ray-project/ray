import ray
import pytest
from ray.tests.conftest import *  # noqa

import numpy as np
from ray import workflow
from ray._private.client_mode_hook import client_mode_wrap
from ray.workflow import workflow_storage
from ray.workflow.tests.utils import assert_task_checkpoints


@ray.remote
def checkpoint_dag(checkpoint):
    @ray.remote
    def large_input():
        return np.arange(2**24)

    @ray.remote
    def identity(x):
        return x

    @ray.remote
    def average(x):
        return np.mean(x)

    x = large_input.options(
        **workflow.options(task_id="large_input", checkpoint=checkpoint)
    ).bind()
    y = identity.options(
        **workflow.options(task_id="identity", checkpoint=checkpoint)
    ).bind(x)
    return workflow.continuation(
        average.options(**workflow.options(task_id="average")).bind(y)
    )


def test_checkpoint_dag_skip_all(workflow_start_regular_shared):
    outputs = workflow.run(
        checkpoint_dag.options(
            **workflow.options(task_id="checkpoint_dag", checkpoint=False)
        ).bind(False),
        workflow_id="checkpoint_skip",
    )
    assert np.isclose(outputs, 8388607.5)
    recovered = workflow.resume("checkpoint_skip")
    assert np.isclose(recovered, 8388607.5)

    @client_mode_wrap
    def check():
        wf_storage = workflow_storage.WorkflowStorage("checkpoint_skip")
        assert_task_checkpoints(wf_storage, "checkpoint_dag", mode="output_skipped")
        assert_task_checkpoints(wf_storage, "large_input", mode="all_skipped")
        assert_task_checkpoints(wf_storage, "identity", mode="all_skipped")
        assert_task_checkpoints(wf_storage, "average", mode="all_skipped")

    check()


def test_checkpoint_dag_skip_partial(workflow_start_regular_shared):
    outputs = workflow.run(
        checkpoint_dag.options(**workflow.options(task_id="checkpoint_dag")).bind(
            False
        ),
        workflow_id="checkpoint_partial",
    )
    assert np.isclose(outputs, 8388607.5)
    recovered = workflow.resume("checkpoint_partial")
    assert np.isclose(recovered, 8388607.5)

    @client_mode_wrap
    def check():
        wf_storage = workflow_storage.WorkflowStorage("checkpoint_partial")
        assert_task_checkpoints(wf_storage, "checkpoint_dag", mode="checkpointed")
        assert_task_checkpoints(wf_storage, "large_input", mode="output_skipped")
        assert_task_checkpoints(wf_storage, "identity", mode="output_skipped")
        assert_task_checkpoints(wf_storage, "average", mode="checkpointed")

    check()


def test_checkpoint_dag_full(workflow_start_regular_shared):
    outputs = workflow.run(
        checkpoint_dag.options(**workflow.options(task_id="checkpoint_dag")).bind(True),
        workflow_id="checkpoint_whole",
    )
    assert np.isclose(outputs, 8388607.5)
    recovered = workflow.resume("checkpoint_whole")
    assert np.isclose(recovered, 8388607.5)

    @client_mode_wrap
    def check():
        wf_storage = workflow_storage.WorkflowStorage("checkpoint_whole")
        assert_task_checkpoints(wf_storage, "checkpoint_dag", mode="checkpointed")
        assert_task_checkpoints(wf_storage, "large_input", mode="checkpointed")
        assert_task_checkpoints(wf_storage, "identity", mode="checkpointed")
        assert_task_checkpoints(wf_storage, "average", mode="checkpointed")

    check()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
