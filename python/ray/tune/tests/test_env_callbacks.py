import os
from unittest.mock import MagicMock, patch

import pytest

from ray.tune.utils.callback import _initialize_env_callbacks


def _create_mock_callback_setup():
    """Helper to create mock callback setup."""
    mock_callback = MagicMock()
    mock_module = MagicMock()
    mock_module.MockCallback = MagicMock(return_value=mock_callback)
    return mock_callback, mock_module


@patch("importlib.import_module")
@patch.dict(os.environ, {"RAY_TUNE_CALLBACKS": "my.module.TestCallback"})
def test_env_callbacks_loaded(mock_import):
    """Test loading execution callbacks from environment variable."""
    mock_callback, mock_module = _create_mock_callback_setup()
    mock_module.TestCallback = MagicMock(return_value=mock_callback)
    mock_import.return_value = mock_module

    callbacks = _initialize_env_callbacks()

    mock_import.assert_called_once_with("my.module")
    assert len([c for c in callbacks if c is mock_callback]) == 1


@patch("importlib.import_module")
@patch.dict(os.environ, {"RAY_TUNE_CALLBACKS": "module1.Callback1,module2.Callback2"})
def test_multiple_env_callbacks(mock_import):
    """Test loading multiple callbacks from environment variable."""
    mock_callback1, mock_module1 = _create_mock_callback_setup()
    mock_callback2, mock_module2 = _create_mock_callback_setup()
    mock_module1.Callback1 = MagicMock(return_value=mock_callback1)
    mock_module2.Callback2 = MagicMock(return_value=mock_callback2)

    def side_effect(name):
        return {"module1": mock_module1, "module2": mock_module2}[name]

    mock_import.side_effect = side_effect

    callbacks = _initialize_env_callbacks()

    assert len([c for c in callbacks if c is mock_callback1]) == 1
    assert len([c for c in callbacks if c is mock_callback2]) == 1


@patch.dict(os.environ, {"RAY_TUNE_CALLBACKS": "invalid_module"})
def test_invalid_callback_path():
    """Test handling of invalid callback paths in environment variable."""
    with pytest.raises(ValueError):
        _initialize_env_callbacks()


@patch("importlib.import_module")
@patch.dict(os.environ, {"RAY_TUNE_CALLBACKS": "nonexistent.module.TestCallback"})
def test_import_error_handling(mock_import):
    """Test handling of import errors when loading callbacks."""
    mock_import.side_effect = ImportError("No module named 'nonexistent'")
    with pytest.raises(ValueError):
        _initialize_env_callbacks()


def test_no_env_variable():
    """Test that no callbacks are loaded when environment variable is not set."""
    if "RAY_TUNE_CALLBACKS" in os.environ:
        del os.environ["RAY_TUNE_CALLBACKS"]

    callbacks = _initialize_env_callbacks()
    assert len(callbacks) == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
