import pytest

import ray


def test_write_file_retry_on_errors_emits_deprecation_warning(caplog):
    ctx = ray.data.DataContext.get_current()
    with pytest.warns(DeprecationWarning):
        ctx.write_file_retry_on_errors = []


def test_data_context_current_context_manager():
    import copy

    from ray.data.context import DataContext

    original = DataContext.get_current()
    ctx1 = copy.deepcopy(original)
    ctx1.set_config("level", "1")

    ctx2 = copy.deepcopy(original)
    ctx2.set_config("level", "2")

    with pytest.raises(ValueError):
        with DataContext.current(ctx1):
            assert DataContext.get_current() is ctx1
            # Nested context manager
            with DataContext.current(ctx2):
                assert DataContext.get_current().get_config("level") == "2"

            assert DataContext.get_current().get_config("level") == "1"

            # Test that raising will reset context too
            raise ValueError("boom")

    assert DataContext.get_current() is original


def test_read_files_task_memory_eps_bytes_default():
    """``DataContext.read_files_task_memory_eps_bytes`` defaults to 64 MiB and is mutable."""
    from ray.data.context import (
        DEFAULT_READ_FILES_TASK_MEMORY_EPS_BYTES,
        DataContext,
    )

    ctx = DataContext.get_current()
    original = ctx.read_files_task_memory_eps_bytes
    try:
        # Default value is the documented 64 MiB (unless overridden by the
        # ``RAY_DATA_READ_FILES_TASK_MEMORY_EPS_BYTES`` env var).
        assert DEFAULT_READ_FILES_TASK_MEMORY_EPS_BYTES == 64 * 1024 * 1024
        # Live context exposes the field.
        assert (
            ctx.read_files_task_memory_eps_bytes
            == DEFAULT_READ_FILES_TASK_MEMORY_EPS_BYTES
        )

        # Field is mutable for per-job overrides.
        ctx.read_files_task_memory_eps_bytes = 7 * 1024 * 1024
        assert ctx.read_files_task_memory_eps_bytes == 7 * 1024 * 1024
    finally:
        ctx.read_files_task_memory_eps_bytes = original


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
