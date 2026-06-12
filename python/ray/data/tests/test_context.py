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


def test_parquet_chunker_row_group_aware_default_and_mutable():
    from ray.data.context import DataContext

    ctx = DataContext.get_current()
    # Defaults on (the row-group-aware chunker); runtime-toggleable for A/B.
    assert ctx.parquet_chunker_row_group_aware is True
    original = ctx.parquet_chunker_row_group_aware
    try:
        ctx.parquet_chunker_row_group_aware = False
        assert DataContext.get_current().parquet_chunker_row_group_aware is False
    finally:
        ctx.parquet_chunker_row_group_aware = original


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
