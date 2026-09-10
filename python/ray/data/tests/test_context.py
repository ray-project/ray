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


@pytest.mark.parametrize("job_setting", [False, True])
def test_enable_ray_data_reconstruction_resolved_from_core_worker(
    shutdown_only, job_setting
):
    """The constructor resolves the job-level ray data reconstruction
    setting off the core worker.
    """
    from ray.data.context import DataContext

    original = DataContext.get_current()
    try:
        ray.init(
            job_config=ray.job_config.JobConfig(
                _enable_ray_data_reconstruction=job_setting
            )
        )
        # `original` was resolved before the driver connected, and
        # `get_current()` caches it process-wide. Drop that copy so the setting
        # is re-resolved against this job's core worker.
        DataContext._set_current(DataContext())

        assert DataContext.get_current().enable_ray_data_reconstruction is job_setting

        # The sealed per-Dataset copy carries it too.
        ds = ray.data.range(1)
        assert ds.context.enable_ray_data_reconstruction is job_setting
    finally:
        DataContext._set_current(original)


def test_enable_ray_data_reconstruction_defaults_false(shutdown_only):
    """Resolution falls back to `False` when the process isn't connected."""
    from ray.data.context import DataContext

    ray.shutdown()
    assert DataContext().enable_ray_data_reconstruction is False

    ray.init()
    assert DataContext().enable_ray_data_reconstruction is False


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
