import pytest

import ray
from ray.data.context import ShuffleStrategy
from ray.util.annotations import RayDeprecationWarning


def test_write_file_retry_on_errors_emits_deprecation_warning(caplog):
    ctx = ray.data.DataContext.get_current()
    with pytest.warns(DeprecationWarning):
        ctx.write_file_retry_on_errors = []


@pytest.mark.parametrize(
    ("attr", "value"),
    [
        ("scheduling_strategy", "DEFAULT"),
        ("scheduling_strategy_large_args", "SPREAD"),
        ("large_args_threshold", 1),
    ],
)
def test_scheduling_config_emits_deprecation_warning(attr, value):
    ctx = ray.data.DataContext()
    with pytest.warns(RayDeprecationWarning, match=rf"DataContext\.{attr}"):
        setattr(ctx, attr, value)


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


def test_hash_shuffle_v2_strategy_alias():
    """`hash_shuffle_v2` remains a deprecated alias of `shuffle_v2`."""

    assert ShuffleStrategy.SHUFFLE_V2.value == "shuffle_v2"
    assert ShuffleStrategy.HASH_SHUFFLE_V2 is ShuffleStrategy.SHUFFLE_V2
    assert "hash_shuffle_v2" not in [s.value for s in ShuffleStrategy]

    # Deprecated value resolves to the current strategy
    with pytest.warns(DeprecationWarning, match="hash_shuffle_v2"):
        assert ShuffleStrategy("hash_shuffle_v2") is ShuffleStrategy.SHUFFLE_V2

    with pytest.raises(ValueError):
        ShuffleStrategy("not_a_shuffle_strategy")


def test_hash_shuffle_compression_alias():
    """`hash_shuffle_compression` remains a deprecated alias of
    `shuffle_compression`."""

    ctx = ray.data.DataContext()

    # Writes through the alias configure the current setting ...
    with pytest.warns(DeprecationWarning, match="hash_shuffle_compression"):
        ctx.hash_shuffle_compression = "lz4"
    assert ctx.shuffle_compression == "lz4"

    # ... and reads through it observe the current setting
    ctx.shuffle_compression = "zstd"
    with pytest.warns(DeprecationWarning, match="hash_shuffle_compression"):
        assert ctx.hash_shuffle_compression == "zstd"


def test_deprecated_shuffle_compression_env_var(monkeypatch):
    """`RAY_DATA_HASH_SHUFFLE_COMPRESSION` remains a deprecated alias of
    `RAY_DATA_SHUFFLE_COMPRESSION`."""

    from ray.data.context import _deduce_default_shuffle_compression

    monkeypatch.delenv("RAY_DATA_SHUFFLE_COMPRESSION", raising=False)
    monkeypatch.delenv("RAY_DATA_HASH_SHUFFLE_COMPRESSION", raising=False)
    assert _deduce_default_shuffle_compression() == "zstd"

    monkeypatch.setenv("RAY_DATA_HASH_SHUFFLE_COMPRESSION", "lz4")
    assert _deduce_default_shuffle_compression() == "lz4"

    # Current env var takes precedence over the deprecated one
    monkeypatch.setenv("RAY_DATA_SHUFFLE_COMPRESSION", "none")
    assert _deduce_default_shuffle_compression() == "none"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
