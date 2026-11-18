import sys
import threading
import time

import pytest

from ray.train.v2._internal.exceptions import UserExceptionWithTraceback
from ray.train.v2._internal.execution.worker_group.thread_runner import ThreadRunner
from ray.train.v2._internal.util import construct_user_exception_with_traceback
from ray.util import rpdb


class ThreadRunnerWithJoin(ThreadRunner):
    def join(self):
        """Join both the target thread and the monitor thread.

        Do not include this with the main ThreadRunner class because:
        * It is tricky to avoid hangs when nested threads raise errors
        * We don't need to join in that case since the controller will see the
          error and shut down the worker
        """
        if self._monitor_thread is None or self._thread is None:
            raise RuntimeError("Must call `run` before trying to `join`.")
        self._monitor_thread.join()
        self._thread.join()
        return self.get_return_value()


@pytest.fixture()
def thread_runner():
    return ThreadRunnerWithJoin()


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
    """Checks that an exception can be captured from the target function.

    This test also checks that the traceback string only includes the frames
    from the user function (train_func) and not the wrapper frames.
    """

    original_monitor_target = thread_runner._monitor_target
    monitor_event = threading.Event()

    def monitor_target_patch():
        monitor_event.wait()
        original_monitor_target()

    thread_runner._monitor_target = monitor_target_patch

    def wrapped_train_func():
        def train_fn_with_final_checkpoint_flush():
            def train_func():
                raise ValueError

            train_func()

        train_fn_with_final_checkpoint_flush()

    thread_runner.run(wrapped_train_func)
    assert thread_runner.is_running() and thread_runner.get_error() is None
    monitor_event.set()

    assert not thread_runner.join()
    assert thread_runner.get_return_value() is None
    assert not thread_runner.is_running()

    error = thread_runner.get_error()

    assert isinstance(error, UserExceptionWithTraceback)
    assert isinstance(error._base_exc, ValueError)
    print(error._traceback_str)
    assert "_run_target" not in error._traceback_str
    assert "wrapped_train_func" not in error._traceback_str
    assert "train_fn_with_final_checkpoint_flush" not in error._traceback_str
    assert "train_func" in error._traceback_str


def test_nested_thread_error(thread_runner):
    """Checks that we capture exceptions from threads kicked off by target function."""

    original_monitor_target = thread_runner._monitor_target
    monitor_event = threading.Event()

    def monitor_target_patch():
        monitor_event.wait()
        original_monitor_target()

    thread_runner._monitor_target = monitor_target_patch

    target_event = threading.Event()

    def target():
        def nested():
            try:
                raise ValueError
            except ValueError as e:
                thread_runner.get_exception_queue().put(
                    construct_user_exception_with_traceback(e)
                )

        thread = threading.Thread(target=nested)
        thread.start()
        thread.join()
        target_event.set()

    thread_runner.run(target)
    target_event.wait()
    # While the monitor thread is processing the exception,
    # the thread runner is still considered running.
    assert thread_runner.is_running() and thread_runner.get_error() is None

    # Unblock the monitor thread.
    monitor_event.set()

    assert not thread_runner.join()
    assert thread_runner.get_return_value() is None
    assert not thread_runner.is_running()

    error = thread_runner.get_error()

    assert isinstance(error, UserExceptionWithTraceback)
    assert isinstance(error._base_exc, ValueError)


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


@pytest.mark.parametrize("post_mortem_enabled", [True, False])
def test_post_mortem_debugging(thread_runner, monkeypatch, post_mortem_enabled):
    """With RAY_DEBUG_POST_MORTEM=1, the runner enters the Ray debugger while the
    failing frames are alive, then still reports the exception."""
    if post_mortem_enabled:
        monkeypatch.setenv("RAY_DEBUG_POST_MORTEM", "1")
    else:
        monkeypatch.delenv("RAY_DEBUG_POST_MORTEM", raising=False)

    seen_exceptions = []
    monkeypatch.setattr(
        rpdb, "_post_mortem", lambda: seen_exceptions.append(sys.exc_info()[1])
    )

    def train_func():
        raise ValueError("boom")

    thread_runner.run(train_func)
    thread_runner.join()

    if post_mortem_enabled:
        assert len(seen_exceptions) == 1
        assert isinstance(seen_exceptions[0], ValueError)
    else:
        assert seen_exceptions == []
    error = thread_runner.get_error()
    assert isinstance(error, UserExceptionWithTraceback)
    assert "ValueError: boom" in str(error)


def test_post_mortem_skips_system_exit(thread_runner, monkeypatch):
    """Like Ray core, SystemExit from the training function doesn't enter the
    debugger, but is still reported."""
    monkeypatch.setenv("RAY_DEBUG_POST_MORTEM", "1")

    seen_exceptions = []
    monkeypatch.setattr(
        rpdb, "_post_mortem", lambda: seen_exceptions.append(sys.exc_info()[1])
    )

    def train_func():
        sys.exit(1)

    thread_runner.run(train_func)
    thread_runner.join()

    assert seen_exceptions == []
    assert isinstance(thread_runner.get_error(), UserExceptionWithTraceback)


@pytest.mark.parametrize("debugger_error", [RuntimeError, SystemExit])
def test_post_mortem_debugger_failure(thread_runner, monkeypatch, debugger_error):
    """A failure to start the debugger, including a BaseException, doesn't
    replace the user's exception."""
    monkeypatch.setenv("RAY_DEBUG_POST_MORTEM", "1")

    def failing_post_mortem():
        raise debugger_error("debugger failed")

    monkeypatch.setattr(rpdb, "_post_mortem", failing_post_mortem)

    def train_func():
        raise ValueError("boom")

    thread_runner.run(train_func)
    thread_runner.join()

    error = thread_runner.get_error()
    assert isinstance(error, UserExceptionWithTraceback)
    assert "ValueError: boom" in str(error)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
