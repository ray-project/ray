import os
from unittest.mock import patch

import pytest

from ray.data._internal.execution.execution_callback import (
    EXECUTION_CALLBACKS_ENV_VAR,
    ExecutionCallback,
    get_execution_callbacks,
)
from ray.data.context import DataContext


class MockExecutionCallback(ExecutionCallback):
    pass


@patch.dict(
    os.environ,
    {
        EXECUTION_CALLBACKS_ENV_VAR: "ray.data.tests.test_execution_callback.MockExecutionCallback"
    },
)
def test_default_callbacks_present_with_custom_env_var():
    """Test that default execution callbacks are present even when
    RAY_DATA_EXECUTION_CALLBACKS is set to a custom callback."""
    ctx = DataContext.get_current()
    callbacks = get_execution_callbacks(ctx)

    # Check that the default callbacks are present
    callback_type_names = [type(c).__name__ for c in callbacks]
    assert "IssueDetectionExecutionCallback" in callback_type_names
    assert "EpochIdxUpdateCallback" in callback_type_names
    # Check that the custom callback from env var is also present
    assert "MockExecutionCallback" in callback_type_names


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
