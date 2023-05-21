import pytest
import sys
import time

from unittest.mock import patch, Mock

import ray
from ray._raylet import StreamingObjectRefGenerator, ObjectRefStreamEoFError
from ray.cloudpickle import dumps
from ray.exceptions import WorkerCrashedError


class MockedWorker:
    def __init__(self, mocked_core_worker):
        self.core_worker = mocked_core_worker

    def reset_core_worker(self):
        """Emulate the case ray.shutdown is called
        and the core_worker instance is GC'ed.
        """
        self.core_worker = None


@pytest.fixture
def mocked_worker():
    mocked_core_worker = Mock()
    mocked_core_worker.try_read_next_object_ref_stream.return_value = None
    mocked_core_worker.delete_object_ref_stream.return_value = None
    mocked_core_worker.create_object_ref_stream.return_value = None
    worker = MockedWorker(mocked_core_worker)
    yield worker


def test_streaming_object_ref_generator_basic_unit(mocked_worker):
    """
    Verify the basic case:
    create a generator -> read values -> nothing more to read -> delete.
    """
    with patch("ray.wait") as mocked_ray_wait:
        c = mocked_worker.core_worker
        generator_ref = ray.ObjectRef.from_random()
        generator = StreamingObjectRefGenerator(generator_ref, mocked_worker)
        c.try_read_next_object_ref_stream.return_value = ray.ObjectRef.nil()
        c.create_object_ref_stream.assert_called()

        # Test when there's no new ref, it returns a nil.
        mocked_ray_wait.return_value = [], [generator_ref]
        ref = generator._next(timeout_s=0)
        assert ref.is_nil()

        # When the new ref is available, next should return it.
        for _ in range(3):
            new_ref = ray.ObjectRef.from_random()
            c.try_read_next_object_ref_stream.return_value = new_ref
            ref = generator._next(timeout_s=0)
            assert new_ref == ref

        # When try_read_next_object_ref_stream raises a
        # ObjectRefStreamEoFError, it should raise a stop iteration.
        c.try_read_next_object_ref_stream.side_effect = ObjectRefStreamEoFError(
            ""
        )  # noqa
        with pytest.raises(StopIteration):
            ref = generator._next(timeout_s=0)

        # Make sure we cannot serialize the generator.
        with pytest.raises(TypeError):
            dumps(generator)

        del generator
        c.delete_object_ref_stream.assert_called()


def test_streaming_object_ref_generator_task_failed_unit(mocked_worker):
    """
    Verify when a task is failed by a system error,
    the generator ref is returned.
    """
    with patch("ray.get") as mocked_ray_get:
        with patch("ray.wait") as mocked_ray_wait:
            c = mocked_worker.core_worker
            generator_ref = ray.ObjectRef.from_random()
            generator = StreamingObjectRefGenerator(generator_ref, mocked_worker)

            # Simulate the worker failure happens.
            mocked_ray_wait.return_value = [generator_ref], []
            mocked_ray_get.side_effect = WorkerCrashedError()

            c.try_read_next_object_ref_stream.return_value = ray.ObjectRef.nil()
            ref = generator._next(timeout_s=0)
            # If the generator task fails by a systsem error,
            # meaning the ref will raise an exception
            # it should be returned.
            print(ref)
            print(generator_ref)
            assert ref == generator_ref

            # Once exception is raised, it should always
            # raise stopIteration regardless of what
            # the ref contains now.
            with pytest.raises(StopIteration):
                ref = generator._next(timeout_s=0)


def test_streaming_object_ref_generator_network_failed_unit(mocked_worker):
    """
    Verify when a task is finished, but if the next ref is not available
    on time, it raises an assertion error.

    TODO(sang): Once we move the task subimssion path to use pubsub
    to guarantee the ordering, we don't need this test anymore.
    """
    with patch("ray.get") as mocked_ray_get:
        with patch("ray.wait") as mocked_ray_wait:
            c = mocked_worker.core_worker
            generator_ref = ray.ObjectRef.from_random()
            generator = StreamingObjectRefGenerator(generator_ref, mocked_worker)

            # Simulate the task has finished.
            mocked_ray_wait.return_value = [generator_ref], []
            mocked_ray_get.return_value = None

            # If StopIteration is not raised within
            # unexpected_network_failure_timeout_s second,
            # it should fail.
            c.try_read_next_object_ref_stream.return_value = ray.ObjectRef.nil()
            ref = generator._next(timeout_s=0, unexpected_network_failure_timeout_s=1)
            assert ref == ray.ObjectRef.nil()
            time.sleep(1)
            with pytest.raises(AssertionError):
                generator._next(timeout_s=0, unexpected_network_failure_timeout_s=1)
            # After that StopIteration should be raised.
            with pytest.raises(StopIteration):
                generator._next(timeout_s=0, unexpected_network_failure_timeout_s=1)


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
