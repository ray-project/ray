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


def test_parquet_reader_target_batch_size_bytes_default_and_mutable():
    """``DataContext.parquet_reader_target_batch_size_bytes`` defaults to ``None``
    (meaning fall back to ``target_min_block_size``) and is mutable."""
    from ray.data.context import (
        DEFAULT_PARQUET_READER_TARGET_BATCH_SIZE_BYTES,
        DataContext,
    )

    assert DEFAULT_PARQUET_READER_TARGET_BATCH_SIZE_BYTES is None

    ctx = DataContext.get_current()
    original = ctx.parquet_reader_target_batch_size_bytes
    try:
        # Default matches the module-level default constant.
        assert (
            ctx.parquet_reader_target_batch_size_bytes
            == DEFAULT_PARQUET_READER_TARGET_BATCH_SIZE_BYTES
        )
        # Per-job override.
        ctx.parquet_reader_target_batch_size_bytes = 64 * 1024
        assert ctx.parquet_reader_target_batch_size_bytes == 64 * 1024
    finally:
        ctx.parquet_reader_target_batch_size_bytes = original


def test_parquet_reader_target_output_block_size_bytes_default_and_mutable():
    """``DataContext.parquet_reader_target_output_block_size_bytes`` defaults to
    ``None`` (meaning fall back to ``target_min_block_size``) and is mutable."""
    from ray.data.context import (
        DEFAULT_PARQUET_READER_TARGET_OUTPUT_BLOCK_SIZE_BYTES,
        DataContext,
    )

    assert DEFAULT_PARQUET_READER_TARGET_OUTPUT_BLOCK_SIZE_BYTES is None

    ctx = DataContext.get_current()
    original = ctx.parquet_reader_target_output_block_size_bytes
    try:
        assert (
            ctx.parquet_reader_target_output_block_size_bytes
            == DEFAULT_PARQUET_READER_TARGET_OUTPUT_BLOCK_SIZE_BYTES
        )
        # Per-job override.
        ctx.parquet_reader_target_output_block_size_bytes = 4 * 1024 * 1024
        assert ctx.parquet_reader_target_output_block_size_bytes == 4 * 1024 * 1024
    finally:
        ctx.parquet_reader_target_output_block_size_bytes = original


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
