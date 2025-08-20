import threading
import time

import pytest

from ray.train.v2._internal.exceptions import UserExceptionWithTraceback
from ray.train.v2._internal.execution.worker_group.thread_runner import ThreadRunner
from ray.train.v2._internal.util import construct_user_exception_with_traceback


@pytest.fixture()
def thread_runner():
    return ThreadRunner()


def test_successful_return(thread_runner):
    """Checks that a value can been successfully returned from the target function."""

    def target():
        return 42

    thread_runner.run(target)
    assert thread_runner.join() == 42

    assert thread_runner.get_return_value() == 42
    assert not thread_runner.is_running()
    assert thread_runner.get_error() is None


def test_error(thread_runner):
    """Checks that an exception can be captured from the target function."""

    def target():
        def nested():
            raise ValueError

        nested()

    thread_runner.run(target)
    assert not thread_runner.join()

    assert thread_runner.get_return_value() is None
    assert not thread_runner.is_running()

    error = thread_runner.get_error()

    assert isinstance(error, UserExceptionWithTraceback)
    assert isinstance(error._base_exc, ValueError)
    print(error._traceback_str)
    assert "_run_target" not in error._traceback_str


def test_nested_thread_error(thread_runner):
    """Checks that we capture exceptions from threads kicked off by target function."""

    def target():
        def nested():
            # Use dummy_condition to test the case where a nested thread raises
            # an exception but the target function is still running.
            dummy_condition = threading.Condition()

            # Use the same target() + nested() structure as test_error to simulate
            # `ThreadRunner._run_target` and `construct_train_func`
            def nested_nested():
                try:
                    raise ValueError
                except ValueError as e:
                    thread_runner.get_exception_queue().put(
                        construct_user_exception_with_traceback(e)
                    )
                    # If the user uses join(), it is their responsibility to ensure that
                    # the target function exits if a nested thread raises an exception.
                    with dummy_condition:
                        dummy_condition.notify_all()
                    raise e

            threading.Thread(target=nested_nested).start()

            while True:
                with dummy_condition:
                    # Do this to avoid race condition in which notify_all happens before wait.
                    if dummy_condition.wait(timeout=0.1):
                        break

        nested()

    thread_runner.run(target)
    # Pick arbitrary timeout to verify that we don't error
    assert not thread_runner.join(timeout=100)

    assert thread_runner.get_return_value() is None
    assert not thread_runner.is_running()

    error = thread_runner.get_error()

    assert isinstance(error, UserExceptionWithTraceback)
    assert isinstance(error._base_exc, ValueError)
    assert "_run_target" not in error._traceback_str


def test_running(thread_runner, tmp_path):
    """Checks that the running status can be queried."""

    running_marker = tmp_path.joinpath("running")
    running_marker.touch()

    def target():
        while running_marker.exists():
            time.sleep(0.01)

    thread_runner.run(target)
    assert thread_runner.is_running()

    # Let the training thread exit.
    running_marker.unlink()

    thread_runner.join()
    assert not thread_runner.is_running()


def test_join_before_run_exception(thread_runner):
    """Checks that an error is raised if `join` is called before `run`."""

    with pytest.raises(RuntimeError):
        thread_runner.join()


def test_run_twice_exception(thread_runner):
    """Checks that an error is raised if `run` is called twice."""
    thread_runner.run(lambda: None)

    with pytest.raises(RuntimeError):
        thread_runner.run(lambda: None)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
