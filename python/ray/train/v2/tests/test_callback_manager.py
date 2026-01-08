import sys
from unittest.mock import ANY, MagicMock

import pytest

from ray.train.v2._internal.execution.callback import (
    CallbackErrorAction,
    ControllerCallback,
)
from ray.train.v2._internal.execution.callback_manager import CallbackManager
from ray.train.v2.api.exceptions import ControllerError


def test_invoke_callback_without_hook():
    """Test that callbacks without the hook method are skipped."""
    callback = MagicMock()

    manager = CallbackManager([callback])
    result = manager.invoke("test_hook", "arg1")

    assert result is None


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
    """Test with a real ControllerCallback implementation that returns a ControllerError."""

    class MockControllerCallback(ControllerCallback):
        def __init__(self):
            self.called = False
            self.error_called = False
            self.error_returned = None

        def after_controller_start(self, train_run_context):
            self.called = True
            raise ValueError("Intentional error")

        def on_callback_hook_exception(self, hook_name, error, **context):
            self.error_called = True
            return (CallbackErrorAction.RAISE, self.error_returned)

    callback = MockControllerCallback()
    callback.error_returned = ControllerError(ValueError("Mapped"))

    manager = CallbackManager([callback])
    train_run_context = MagicMock()
    result = manager.invoke("after_controller_start", train_run_context)

    assert callback.called is True
    assert callback.error_called is True
    assert isinstance(result, ControllerError)


def test_invoke_with_real_controller_callback_no_error_return():
    """Test with a real ControllerCallback that doesn't return error."""

    class MockControllerCallback(ControllerCallback):
        def __init__(self):
            self.called = False
            self.error_called = False

        def after_controller_start(self, train_run_context):
            self.called = True
            raise ValueError("Intentional error")

        def on_callback_hook_exception(self, hook_name, error, **context):
            self.error_called = True
            return (CallbackErrorAction.SUPPRESS, None)

    callback = MockControllerCallback()
    manager = CallbackManager([callback])
    train_run_context = MagicMock()
    result = manager.invoke("after_controller_start", train_run_context)

    assert callback.called is True
    assert callback.error_called is True
    assert result is None


def test_invoke_with_kwargs_passed_to_on_callback_hook_exception():
    """Test that kwargs are properly passed to on_callback_hook_exception."""
    callback = MagicMock()
    callback.test_hook = MagicMock(side_effect=ValueError("Error"))
    callback.on_callback_hook_exception = MagicMock(
        return_value=(CallbackErrorAction.SUPPRESS, None)
    )

    manager = CallbackManager([callback])
    manager.invoke("test_hook", "arg1", "arg2", key1="value1", key2="value2")

    # Verify on_callback_hook_exception was called with the same kwargs
    callback.on_callback_hook_exception.assert_called_once_with(
        "test_hook", ANY, key1="value1", key2="value2"
    )
    assert isinstance(callback.on_callback_hook_exception.call_args.args[1], ValueError)


def test_invoke_on_callback_hook_exception_can_return_none():
    callback = MagicMock()
    callback.test_hook = MagicMock(side_effect=ValueError("Error"))
    callback.on_callback_hook_exception = MagicMock(return_value=None)

    manager = CallbackManager([callback])
    result = manager.invoke("test_hook")

    # Returning None is an invalid return type; CallbackManager should surface
    # this as a controller-side failure rather than raising directly.
    assert isinstance(result, ControllerError)
    assert isinstance(result.controller_failure, TypeError)


def test_invoke_on_callback_hook_exception_can_return_training_failed_error():
    callback = MagicMock()
    callback.test_hook = MagicMock(side_effect=ValueError("Error"))
    callback.on_callback_hook_exception = MagicMock(
        return_value=(CallbackErrorAction.RAISE, ControllerError(ValueError("Mapped")))
    )

    manager = CallbackManager([callback])
    result = manager.invoke("test_hook")

    assert isinstance(result, ControllerError)


def test_invoke_on_callback_hook_exception_raises_is_propagated_as_controller_error():
    callback = MagicMock()
    callback.test_hook = MagicMock(side_effect=ValueError("Original hook error"))
    callback.on_callback_hook_exception = MagicMock(
        side_effect=RuntimeError("Handler error")
    )

    manager = CallbackManager([callback])
    result = manager.invoke("test_hook")

    assert isinstance(result, ControllerError)
    assert isinstance(result.controller_failure, RuntimeError)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
