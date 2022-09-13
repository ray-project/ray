from hashlib import sha1
import tempfile

import pytest
import ray
from ray import workflow
from ray.workflow.storage.debug import DebugStorage
from ray.workflow.workflow_storage import STEP_OUTPUTS_METADATA
from ray.workflow.common import asyncio_run
from ray.workflow.storage.filesystem import FilesystemStorageImpl
from ray.workflow.tests.utils import _alter_storage


@ray.remote
def pass_1(x: str, y: str):
    return sha1((x + y + "1").encode()).hexdigest()


@ray.remote
def pass_2(x: str, y: str):
    if sha1((x + y + "_2").encode()).hexdigest() > x:
        return sha1((x + y + "2").encode()).hexdigest()
    return workflow.continuation(pass_1.bind(x, y))


@ray.remote
def pass_3(x: str, y: str):
    if sha1((x + y + "_3").encode()).hexdigest() > x:
        return sha1((x + y + "3").encode()).hexdigest()
    return workflow.continuation(pass_2.bind(x, y))


@ray.remote
def merge(x0: str, x1: str, x2: str) -> str:
    return sha1((x0 + x1 + x2).encode()).hexdigest()


@ray.remote
def scan(x0: str, x1: str, x2: str):
    x0 = sha1((x0 + x2).encode()).hexdigest()
    x1 = sha1((x1 + x2).encode()).hexdigest()
    x2 = sha1((x0 + x1 + x2).encode()).hexdigest()
    y0, y1, y2 = pass_1.bind(x0, x1), pass_2.bind(x1, x2), pass_3.bind(x2, x0)
    return workflow.continuation(merge.bind(y0, y1, y2))


def construct_workflow(length: int):
    results = ["a", "b"]
    for i in range(length):
        x0, x1, x2 = results[-2], results[-1], str(i)
        results.append(scan.bind(x0, x1, x2))
    return results[-1]


def _locate_initial_commit(debug_store: DebugStorage) -> int:
    for i in range(len(debug_store)):
        log = debug_store.get_log(i)
        if log["key"].endswith(STEP_OUTPUTS_METADATA):
            return i
    return -1


@pytest.mark.skip(reason="TODO (suquark): Support debug storage.")
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
        debug_store = DebugStorage(temp_dir)
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
            return workflow.resume(workflow_id="complex_workflow")

        with pytest.raises(ValueError):
            # in cases, the replayed records are too few to resume the
            # workflow.
            resume(index - 1)

        if isinstance(debug_store.wrapped_storage, FilesystemStorageImpl):
            # filesystem is faster, so we can cover all cases
            task_len = 1
        else:
            task_len = max((len(debug_store) - index) // 5, 1)

        for j in range(index, len(debug_store), task_len):
            assert resume(j) == result


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
