import ray
import pytest
from ray.tests.conftest import *  # noqa

import numpy as np
from ray import workflow
from ray.workflow import workflow_storage


@ray.remote
def checkpoint_dag(checkpoint):
    @ray.remote
    def large_input():
        return np.arange(2 ** 24)

    @ray.remote
    def identity(x):
        return x

    @ray.remote
    def average(x):
        return np.mean(x)

    x = large_input.options(
        **workflow.options(name="large_input", checkpoint=checkpoint)
    ).bind()
    y = identity.options(
        **workflow.options(name="identity", checkpoint=checkpoint)
    ).bind(x)
    return workflow.continuation(
        average.options(**workflow.options(name="average")).bind(y)
    )


def _assert_step_checkpoints(wf_storage, step_id, mode):
    result = wf_storage.inspect_step(step_id)
    if mode == "all_skipped":
        assert not result.output_object_valid
        assert result.output_step_id is None
        assert not result.args_valid
        assert not result.func_body_valid
        assert not result.step_options
    elif mode == "output_skipped":
        assert not result.output_object_valid
        assert result.output_step_id is None
        assert result.args_valid
        assert result.func_body_valid
        assert result.step_options is not None
    elif mode == "checkpointed":
        assert result.output_object_valid or result.output_step_id is not None
    else:
        raise ValueError("Unknown mode.")


def test_checkpoint_dag_skip_all(workflow_start_regular_shared):
    outputs = workflow.create(
        checkpoint_dag.options(
            **workflow.options(name="checkpoint_dag", checkpoint=False)
        ).bind(False)
    ).run(workflow_id="checkpoint_skip")
    assert np.isclose(outputs, 8388607.5)
    recovered = ray.get(workflow.resume("checkpoint_skip"))
    assert np.isclose(recovered, 8388607.5)

    wf_storage = workflow_storage.WorkflowStorage("checkpoint_skip")
    _assert_step_checkpoints(wf_storage, "checkpoint_dag", mode="output_skipped")
    _assert_step_checkpoints(wf_storage, "large_input", mode="all_skipped")
    _assert_step_checkpoints(wf_storage, "identity", mode="all_skipped")
    _assert_step_checkpoints(wf_storage, "average", mode="all_skipped")


def test_checkpoint_dag_skip_partial(workflow_start_regular_shared):
    outputs = workflow.create(
        checkpoint_dag.options(**workflow.options(name="checkpoint_dag")).bind(False)
    ).run(workflow_id="checkpoint_partial")
    assert np.isclose(outputs, 8388607.5)
    recovered = ray.get(workflow.resume("checkpoint_partial"))
    assert np.isclose(recovered, 8388607.5)

    wf_storage = workflow_storage.WorkflowStorage("checkpoint_partial")
    _assert_step_checkpoints(wf_storage, "checkpoint_dag", mode="checkpointed")
    _assert_step_checkpoints(wf_storage, "large_input", mode="output_skipped")
    _assert_step_checkpoints(wf_storage, "identity", mode="output_skipped")
    _assert_step_checkpoints(wf_storage, "average", mode="checkpointed")


def test_checkpoint_dag_full(workflow_start_regular_shared):
    outputs = workflow.create(
        checkpoint_dag.options(**workflow.options(name="checkpoint_dag")).bind(True)
    ).run(workflow_id="checkpoint_whole")
    assert np.isclose(outputs, 8388607.5)
    recovered = ray.get(workflow.resume("checkpoint_whole"))
    assert np.isclose(recovered, 8388607.5)

    wf_storage = workflow_storage.WorkflowStorage("checkpoint_whole")
    _assert_step_checkpoints(wf_storage, "checkpoint_dag", mode="checkpointed")
    _assert_step_checkpoints(wf_storage, "large_input", mode="checkpointed")
    _assert_step_checkpoints(wf_storage, "identity", mode="checkpointed")
    _assert_step_checkpoints(wf_storage, "average", mode="checkpointed")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
