import ray
import time
import pytest
from ray.tests.conftest import *  # noqa

import numpy as np
from ray import workflow
from ray.workflow.tests import utils
from ray.workflow import storage, workflow_storage
from ray.exceptions import RaySystemError


@workflow.step
def large_input():
    return np.arange(2 ** 24)


@workflow.step
def identity(x):
    return x


@workflow.step
def average(x):
    return np.mean(x)


@workflow.step
def checkpoint_dag(checkpoint):
    x = large_input.options(name="large_input", checkpoint=checkpoint).step()
    y = identity.options(name="identity", checkpoint=checkpoint).step(x)
    return average.options(name="average").step(y)


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


def test_checkpoint_dag_skip_all(workflow_start_regular):
    global_storage = storage.get_global_storage()

    outputs = (
        checkpoint_dag.options(name="checkpoint_dag", checkpoint=False)
        .step(False)
        .run(workflow_id="checkpoint_skip")
    )
    assert np.isclose(outputs, 8388607.5)
    recovered = ray.get(workflow.resume("checkpoint_skip"))
    assert np.isclose(recovered, 8388607.5)

    wf_storage = workflow_storage.WorkflowStorage("checkpoint_skip", global_storage)
    _assert_step_checkpoints(wf_storage, "checkpoint_dag", mode="output_skipped")
    _assert_step_checkpoints(wf_storage, "large_input", mode="all_skipped")
    _assert_step_checkpoints(wf_storage, "identity", mode="all_skipped")
    _assert_step_checkpoints(wf_storage, "average", mode="all_skipped")


def test_checkpoint_dag_skip_partial(workflow_start_regular):
    global_storage = storage.get_global_storage()

    outputs = (
        checkpoint_dag.options(name="checkpoint_dag")
        .step(False)
        .run(workflow_id="checkpoint_partial")
    )
    assert np.isclose(outputs, 8388607.5)
    recovered = ray.get(workflow.resume("checkpoint_partial"))
    assert np.isclose(recovered, 8388607.5)

    wf_storage = workflow_storage.WorkflowStorage("checkpoint_partial", global_storage)
    _assert_step_checkpoints(wf_storage, "checkpoint_dag", mode="checkpointed")
    _assert_step_checkpoints(wf_storage, "large_input", mode="output_skipped")
    _assert_step_checkpoints(wf_storage, "identity", mode="output_skipped")
    _assert_step_checkpoints(wf_storage, "average", mode="checkpointed")


def test_checkpoint_dag_full(workflow_start_regular):
    global_storage = storage.get_global_storage()

    outputs = (
        checkpoint_dag.options(name="checkpoint_dag")
        .step(True)
        .run(workflow_id="checkpoint_whole")
    )
    assert np.isclose(outputs, 8388607.5)
    recovered = ray.get(workflow.resume("checkpoint_whole"))
    assert np.isclose(recovered, 8388607.5)

    wf_storage = workflow_storage.WorkflowStorage("checkpoint_whole", global_storage)
    _assert_step_checkpoints(wf_storage, "checkpoint_dag", mode="checkpointed")
    _assert_step_checkpoints(wf_storage, "large_input", mode="checkpointed")
    _assert_step_checkpoints(wf_storage, "identity", mode="checkpointed")
    _assert_step_checkpoints(wf_storage, "average", mode="checkpointed")


@workflow.step
def identity2(x):
    if not utils.check_global_mark():
        import os

        os.kill(os.getpid(), 9)
    return x


@workflow.step
def checkpoint_dag2(checkpoint):
    x = large_input.options(checkpoint=checkpoint).step()
    y = identity2.options(checkpoint=checkpoint).step(x)
    return average.step(y)


def test_checkpoint_dag_recovery(workflow_start_regular):
    utils.set_global_mark()
    # warm up to ensure precise timing
    for _ in range(3):
        outputs = checkpoint_dag2.step(True).run()
        assert np.isclose(outputs, 8388607.5)

    utils.unset_global_mark()

    start = time.time()
    with pytest.raises(RaySystemError):
        checkpoint_dag2.options(checkpoint=False).step(False).run(
            workflow_id="checkpoint_skip2"
        )
    run_duration_skipped = time.time() - start

    utils.set_global_mark()

    start = time.time()
    recovered = ray.get(workflow.resume("checkpoint_skip2"))
    recover_duration_skipped = time.time() - start
    assert np.isclose(recovered, 8388607.5)

    utils.unset_global_mark()

    start = time.time()
    with pytest.raises(RaySystemError):
        checkpoint_dag2.step(False).run(workflow_id="checkpoint_partial2")
    run_duration_partial = time.time() - start

    utils.set_global_mark()

    start = time.time()
    recovered = ray.get(workflow.resume("checkpoint_partial2"))
    recover_duration_partial = time.time() - start
    assert np.isclose(recovered, 8388607.5)

    utils.unset_global_mark()

    start = time.time()
    with pytest.raises(RaySystemError):
        checkpoint_dag2.step(True).run(workflow_id="checkpoint_whole2")
    run_duration_whole = time.time() - start

    utils.set_global_mark()

    start = time.time()
    recovered = ray.get(workflow.resume("checkpoint_whole2"))
    recover_duration_whole = time.time() - start
    assert np.isclose(recovered, 8388607.5)

    print(
        f"[skipped] run_duration = {run_duration_skipped}, "
        f"recover_duration = {recover_duration_skipped}"
    )
    print(
        f"[partial] run_duration = {run_duration_partial}, "
        f"recover_duration = {recover_duration_partial}"
    )
    print(
        f"[whole] run_duration = {run_duration_whole}, "
        f"recover_duration = {recover_duration_whole}"
    )


@pytest.mark.skip(
    reason=(
        "Currently it is not clear how and if we need to check"
        "side effects of skipping checkpointing, e.g., the"
        "violation of exactly-once execution guarantee of workflow."
    )
)
def test_checkpoint_dag_validation(workflow_start_regular):
    @workflow.step
    def valid_checkpoint_dag_1():
        y = identity.options(checkpoint=False).step(42)
        return average.options(checkpoint=True).step(y)

    @workflow.step
    def invalid_checkpoint_dag_1():
        y = identity.options(checkpoint=True).step(42)
        return average.options(checkpoint=True).step(y)

    @workflow.step
    def invalid_checkpoint_dag_2():
        y = valid_checkpoint_dag_1.options(checkpoint=False).step()
        return average.options(checkpoint=True).step(y)

    valid_checkpoint_dag_1.options(checkpoint=False).step().run()
    # check invalid configuration
    with pytest.raises(RaySystemError):
        invalid_checkpoint_dag_1.options(checkpoint=False).step().run()
    # check invalid configuration
    with pytest.raises(RaySystemError):
        invalid_checkpoint_dag_2.options(checkpoint=False).step().run()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
