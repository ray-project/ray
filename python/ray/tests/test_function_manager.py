import sys

import pytest

from ray._private.function_manager import FunctionActorManager, FunctionExecutionInfo
from ray._raylet import CppFunctionDescriptor


class _DummyActorID:
    def __init__(self, is_nil: bool):
        self._is_nil = is_nil

    def is_nil(self):
        return self._is_nil


class _DummyWorker:
    def __init__(self, is_actor_worker: bool):
        self.actor_id = _DummyActorID(is_nil=not is_actor_worker)
        self.actors = {self.actor_id: object()} if is_actor_worker else {}
        self.load_code_from_local = False
        self.node_ip_address = "127.0.0.1"
        self.worker_id = b"0" * 28


def test_get_execution_info_cpp_actor_descriptor_uses_cross_language_key():
    worker = _DummyWorker(is_actor_worker=True)
    manager = FunctionActorManager(worker)

    descriptor = CppFunctionDescriptor("foo", "PYTHON", "Bar")
    expected = FunctionExecutionInfo(
        function=lambda *_args, **_kwargs: None,
        function_name="foo",
        max_calls=0,
    )

    manager._function_execution_info[("Bar", "foo")] = expected

    info = manager.get_execution_info(job_id="job-id", function_descriptor=descriptor)

    assert info is expected


def test_get_execution_info_cpp_non_actor_descriptor_raises_runtime_error():
    worker = _DummyWorker(is_actor_worker=False)
    manager = FunctionActorManager(worker)

    descriptor = CppFunctionDescriptor("foo", "PYTHON", "Bar")

    with pytest.raises(
        RuntimeError, match="without function_id on a non-actor worker"
    ):
        manager.get_execution_info(job_id="job-id", function_descriptor=descriptor)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
