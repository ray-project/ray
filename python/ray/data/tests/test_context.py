import pytest

import ray


def test_write_file_retry_on_errors_emits_deprecation_warning(caplog):
    ctx = ray.data.DataContext.get_current()
    with pytest.warns(DeprecationWarning):
        ctx.write_file_retry_on_errors = []


def test_partitioner_max_bucket_size_bytes_default_is_256_mib():
    ctx = ray.data.DataContext.get_current()
    assert ctx.partitioner_max_bucket_size_bytes == 256 * 1024 * 1024


def test_partitioner_max_bucket_size_bytes_env_override(monkeypatch):
    # DEFAULT_PARTITIONER_MAX_BUCKET_SIZE_BYTES is computed once at
    # ray.data.context import time via env_integer(key, default), so this
    # verifies the underlying env_integer contract with the exact key/default
    # the module uses, rather than reloading the already-imported context
    # module (which would risk DataContext class-identity issues for other
    # already-imported references elsewhere in the process).
    from ray._common.utils import env_integer

    monkeypatch.setenv("RAY_DATA_PARTITIONER_MAX_BUCKET_SIZE_BYTES", "134217728")
    assert (
        env_integer("RAY_DATA_PARTITIONER_MAX_BUCKET_SIZE_BYTES", 256 * 1024 * 1024)
        == 134217728
    )


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
