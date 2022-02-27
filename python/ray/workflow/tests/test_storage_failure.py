from hashlib import sha1
import tempfile

import pytest
import ray
from ray import workflow
from ray.workflow.storage import get_global_storage
from ray.workflow.storage.debug import DebugStorage
from ray.workflow.workflow_storage import STEP_OUTPUTS_METADATA
from ray.workflow.workflow_storage import asyncio_run
from ray.workflow.storage.filesystem import FilesystemStorageImpl
from ray.workflow.tests.utils import _alter_storage


@workflow.step
def pass_1(x: str, y: str):
    return sha1((x + y + "1").encode()).hexdigest()


@workflow.step
def pass_2(x: str, y: str):
    if sha1((x + y + "_2").encode()).hexdigest() > x:
        return sha1((x + y + "2").encode()).hexdigest()
    return pass_1.step(x, y)


@workflow.step
def pass_3(x: str, y: str):
    if sha1((x + y + "_3").encode()).hexdigest() > x:
        return sha1((x + y + "3").encode()).hexdigest()
    return pass_2.step(x, y)


@workflow.step
def merge(x0: str, x1: str, x2: str) -> str:
    return sha1((x0 + x1 + x2).encode()).hexdigest()


@workflow.step
def scan(x0: str, x1: str, x2: str):
    x0 = sha1((x0 + x2).encode()).hexdigest()
    x1 = sha1((x1 + x2).encode()).hexdigest()
    x2 = sha1((x0 + x1 + x2).encode()).hexdigest()
    y0, y1, y2 = pass_1.step(x0, x1), pass_2.step(x1, x2), pass_3.step(x2, x0)
    return merge.step(y0, y1, y2)


def construct_workflow(length: int):
    results = ["a", "b"]
    for i in range(length):
        x0, x1, x2 = results[-2], results[-1], str(i)
        results.append(scan.step(x0, x1, x2))
    return results[-1]


def _locate_initial_commit(debug_store: DebugStorage) -> int:
    for i in range(len(debug_store)):
        log = debug_store.get_log(i)
        if log["key"].endswith(STEP_OUTPUTS_METADATA):
            return i
    return -1


@pytest.mark.parametrize(
    "workflow_start_regular",
    [
        {
            "num_cpus": 4,  # increase CPUs to add pressure
        }
    ],
    indirect=True,
)
def test_failure_with_storage(workflow_start_regular):
    with tempfile.TemporaryDirectory() as temp_dir:
        debug_store = DebugStorage(get_global_storage(), temp_dir)
        _alter_storage(debug_store)

        wf = construct_workflow(length=3)
        result = wf.run(workflow_id="complex_workflow")
        index = _locate_initial_commit(debug_store) + 1
        debug_store.log_off()

        def resume(num_records_replayed):
            key = debug_store.wrapped_storage.make_key("complex_workflow")
            asyncio_run(debug_store.wrapped_storage.delete_prefix(key))

            async def replay():
                # We need to replay one by one to avoid conflict
                for i in range(num_records_replayed):
                    await debug_store.replay(i)

            asyncio_run(replay())
            return ray.get(workflow.resume(workflow_id="complex_workflow"))

        with pytest.raises(ValueError):
            # in cases, the replayed records are too few to resume the
            # workflow.
            resume(index - 1)

        if isinstance(debug_store.wrapped_storage, FilesystemStorageImpl):
            # filesystem is faster, so we can cover all cases
            step_len = 1
        else:
            step_len = max((len(debug_store) - index) // 5, 1)

        for j in range(index, len(debug_store), step_len):
            assert resume(j) == result


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
