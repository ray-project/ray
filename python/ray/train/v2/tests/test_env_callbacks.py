import os
from unittest.mock import MagicMock, patch

import pytest

from ray.train.v2._internal.callbacks.env_callback import _initialize_env_callbacks
from ray.train.v2._internal.constants import RAY_TRAIN_CALLBACKS_ENV_VAR
from ray.train.v2._internal.execution.callback import RayTrainCallback


class MockCallback(RayTrainCallback):
    pass


@pytest.mark.parametrize(
    "env_value,expected_callback_count",
    [
        ("my.module.Callback1", 1),
        ("module1.Callback1,module2.Callback2", 2),
        ("", 0),
        ("   ", 0),
        ("module.Callback1, ,module.Callback2", 2),
    ],
)
@patch("importlib.import_module")
def test_env_callbacks_loading(mock_import, env_value, expected_callback_count):
    """Test loading execution callbacks from environment variable with various inputs."""
    if env_value:
        with patch.dict(os.environ, {RAY_TRAIN_CALLBACKS_ENV_VAR: env_value}):

            mock_module = MagicMock()
            mock_module.Callback1 = MockCallback
            mock_module.Callback2 = MockCallback
            mock_import.return_value = mock_module

            callbacks = _initialize_env_callbacks()

            assert len(callbacks) == expected_callback_count
            for callback in callbacks:
                assert isinstance(callback, RayTrainCallback)

    else:
        with patch.dict(
            os.environ, {RAY_TRAIN_CALLBACKS_ENV_VAR: env_value}, clear=True
        ):
            callbacks = _initialize_env_callbacks()
            assert len(callbacks) == 0


@pytest.mark.parametrize(
    "env_value,original_error_type",
    [
        ("invalid_module", ValueError),
        ("module.Class", TypeError),
        ("module.NonExistentClass", AttributeError),
    ],
)
@patch("importlib.import_module")
def test_callback_loading_errors(mock_import, env_value, original_error_type):
    """Test handling of various error conditions when loading callbacks."""
    with patch.dict(os.environ, {RAY_TRAIN_CALLBACKS_ENV_VAR: env_value}):
        if "invalid_module" in env_value:
            pass
        elif "NonExistentClass" in env_value:
            mock_module = MagicMock()
            del mock_module.NonExistentClass
            mock_import.return_value = mock_module
        else:
            mock_module = MagicMock()

            class RegularClass:
                pass

            mock_module.Class = RegularClass
            mock_import.return_value = mock_module

        with pytest.raises(
            ValueError, match=f"Failed to import callback from '{env_value}'"
        ) as exc_info:
            _initialize_env_callbacks()

        assert isinstance(exc_info.value.__cause__, original_error_type)


def test_import_error_handling():
    """Test handling of import errors when loading callbacks."""
    with patch.dict(
        os.environ, {RAY_TRAIN_CALLBACKS_ENV_VAR: "nonexistent.module.TestCallback"}
    ):
        with pytest.raises(
            ValueError,
            match="Failed to import callback from 'nonexistent.module.TestCallback'",
        ):
            _initialize_env_callbacks()


def test_no_env_variable():
    """Test that no callbacks are loaded when environment variable is not set."""
    if RAY_TRAIN_CALLBACKS_ENV_VAR in os.environ:
        del os.environ[RAY_TRAIN_CALLBACKS_ENV_VAR]

    callbacks = _initialize_env_callbacks()
    assert len(callbacks) == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
