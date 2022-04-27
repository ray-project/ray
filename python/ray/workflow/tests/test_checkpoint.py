import ray
import time
import pytest
from ray.tests.conftest import *  # noqa

import numpy as np
from ray import workflow
from ray.workflow.tests import utils
from ray.workflow import storage, workflow_storage
from ray.exceptions import RaySystemError


@ray.remote
def large_input():
    return np.arange(2 ** 24)


@ray.remote
def identity(x):
    return x


@ray.remote
def average(x):
    return np.mean(x)


@ray.remote
def checkpoint_dag(checkpoint):
    x = utils.update_workflow_options(
        large_input, name="large_input", checkpoint=checkpoint
    ).bind()
    y = utils.update_workflow_options(
        identity, name="identity", checkpoint=checkpoint
    ).bind(x)
    return workflow.continuation(
        utils.update_workflow_options(average, name="average").bind(y)
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


def test_checkpoint_dag_skip_all(workflow_start_regular):
    global_storage = storage.get_global_storage()

    outputs = utils.run_workflow_dag_with_options(
        checkpoint_dag,
        (False,),
        workflow_id="checkpoint_skip",
        name="checkpoint_dag",
        checkpoint=False,
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

    outputs = utils.run_workflow_dag_with_options(
        checkpoint_dag,
        (False,),
        workflow_id="checkpoint_partial",
        name="checkpoint_dag",
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

    outputs = utils.run_workflow_dag_with_options(
        checkpoint_dag, (True,), workflow_id="checkpoint_whole", name="checkpoint_dag"
    )
    assert np.isclose(outputs, 8388607.5)
    recovered = ray.get(workflow.resume("checkpoint_whole"))
    assert np.isclose(recovered, 8388607.5)

    wf_storage = workflow_storage.WorkflowStorage("checkpoint_whole", global_storage)
    _assert_step_checkpoints(wf_storage, "checkpoint_dag", mode="checkpointed")
    _assert_step_checkpoints(wf_storage, "large_input", mode="checkpointed")
    _assert_step_checkpoints(wf_storage, "identity", mode="checkpointed")
    _assert_step_checkpoints(wf_storage, "average", mode="checkpointed")


@ray.remote
def identity2(x):
    if not utils.check_global_mark():
        import os

        os.kill(os.getpid(), 9)
    return x


@ray.remote
def checkpoint_dag2(checkpoint):
    x = utils.update_workflow_options(large_input, checkpoint=checkpoint).bind()
    y = utils.update_workflow_options(identity2, checkpoint=checkpoint).bind(x)
    return workflow.continuation(average.bind(y))


def test_checkpoint_dag_recovery(workflow_start_regular):
    utils.set_global_mark()
    # warm up to ensure precise timing
    for _ in range(3):
        outputs = workflow.create(checkpoint_dag2.bind(True)).run()
        assert np.isclose(outputs, 8388607.5)

    utils.unset_global_mark()

    start = time.time()
    with pytest.raises(RaySystemError):
        utils.run_workflow_dag_with_options(
            checkpoint_dag2, (False,), workflow_id="checkpoint_skip2", checkpoint=False
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
        workflow.create(checkpoint_dag2.bind(False)).run(
            workflow_id="checkpoint_partial2"
        )
    run_duration_partial = time.time() - start

    utils.set_global_mark()

    start = time.time()
    recovered = ray.get(workflow.resume("checkpoint_partial2"))
    recover_duration_partial = time.time() - start
    assert np.isclose(recovered, 8388607.5)

    utils.unset_global_mark()

    start = time.time()
    with pytest.raises(RaySystemError):
        workflow.create(checkpoint_dag2.bind(True)).run(workflow_id="checkpoint_whole2")
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
