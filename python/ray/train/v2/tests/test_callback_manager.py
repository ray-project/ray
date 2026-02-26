import sys
from unittest.mock import MagicMock

import pytest

from ray.train.v2._internal.execution.callback import ControllerCallback
from ray.train.v2._internal.execution.callback_manager import CallbackManager
from ray.train.v2.api.exceptions import ControllerError


def test_invoke_callback_without_hook():
    """Test that callbacks without the hook method raise an error."""

    class CallbackWithoutHook:
        pass

    manager = CallbackManager([CallbackWithoutHook()])
    with pytest.raises(ControllerError) as exc_info:
        manager.invoke("test_hook", "arg1")
    assert isinstance(exc_info.value.controller_failure, AttributeError)


def test_invoke_multiple_callbacks_all_succeed():
    """Test that multiple callbacks are invoked in sequence."""
    callback1 = MagicMock()
    callback1.test_hook = MagicMock()
    callback2 = MagicMock()
    callback2.test_hook = MagicMock()

    manager = CallbackManager([callback1, callback2])
    result = manager.invoke("test_hook", "arg")

    callback1.test_hook.assert_called_once_with("arg")
    callback2.test_hook.assert_called_once_with("arg")
    assert result is None


def test_invoke_with_real_controller_callback_error_returned():
    """Test with a real ControllerCallback implementation that raises an error."""

    class MockControllerCallback(ControllerCallback):
        def __init__(self):
            self.called = False

        def after_controller_start(self, train_run_context):
            self.called = True
            raise ValueError("Intentional error")

    callback = MockControllerCallback()

    manager = CallbackManager([callback])
    train_run_context = MagicMock()
    with pytest.raises(ControllerError) as exc_info:
        manager.invoke("after_controller_start", train_run_context)

    assert callback.called is True
    assert isinstance(exc_info.value.controller_failure, ValueError)


def test_invoke_callback_error_returns_controller_error():
    callback = MagicMock()
    callback.test_hook = MagicMock(side_effect=ValueError("Original hook error"))

    manager = CallbackManager([callback])
    with pytest.raises(ControllerError) as exc_info:
        manager.invoke("test_hook", "arg1", key1="value1")

    assert isinstance(exc_info.value.controller_failure, ValueError)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
