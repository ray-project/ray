import asyncio
import os
from hashlib import sha1
import random

import pytest
import ray
from ray.experimental import workflow
from ray.experimental.workflow.storage import (get_global_storage,
                                               set_global_storage)
from ray.experimental.workflow.storage.debug import DebugStorage
from ray.experimental.workflow.workflow_storage import STEP_OUTPUTS_METADATA
from ray.experimental.workflow.workflow_storage import asyncio_run


@workflow.step
def pass_1(x: str, y: str):
    return sha1((x + y + "1").encode()).hexdigest()


@workflow.step
def pass_2(x: str, y: str):
    return sha1((x + y + "2").encode()).hexdigest()


@workflow.step
def pass_3(x: str, y: str):
    return sha1((x + y + "3").encode()).hexdigest()


def construct_linear_workflow(input: str, length: int):
    random.seed(41)
    x = pass_1.step(input, "0")
    for i in range(1, length):
        rnd = random.randint(1, 3)
        if rnd == 1:
            x = pass_1.step(x, str(i))
        elif rnd == 2:
            x = pass_2.step(x, str(i))
        else:
            x = pass_3.step(x, str(i))
    return x


def _alter_storage(new_storage):
    set_global_storage(new_storage)
    # alter the storage
    ray.shutdown()
    os.system("ray stop --force")
    workflow.init(new_storage)


def _locate_initial_commit(debug_store: DebugStorage) -> int:
    for i in range(len(debug_store)):
        log = debug_store.get_log(i)
        if log["key"].endswith(STEP_OUTPUTS_METADATA):
            return i
    return -1


@pytest.mark.parametrize(
    "workflow_start_regular",
    [{
        "num_cpus": 4,  # increase CPUs to add pressure
    }],
    indirect=True)
def test_linear_workflow_failure(workflow_start_regular):
    debug_store = DebugStorage(get_global_storage())
    _alter_storage(debug_store)

    wf = construct_linear_workflow("start", 20)
    result = wf.run(workflow_id="linear_workflow")

    index = _locate_initial_commit(debug_store)
    replays = [debug_store.replay(i) for i in range(index)]
    asyncio_run(asyncio.gather(*replays))
    resumed_result = ray.get(workflow.resume(workflow_id="linear_workflow"))
    assert resumed_result == result
