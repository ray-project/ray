import pytest

import ray
from ray.data.context import ShuffleStrategy, _deduce_default_shuffle_compression
from ray.job_config import JobConfig
from ray.util.annotations import RayDeprecationWarning

_RECONSTRUCTION_OVERRIDE_ERROR = (
    "only set the enable_ray_data_reconstruction value using the job config"
)


def test_write_file_retry_on_errors_emits_deprecation_warning(caplog):
    ctx = ray.data.DataContext.get_current()
    with pytest.warns(DeprecationWarning):
        ctx.write_file_retry_on_errors = []


def _init_job_with_reconstruction(enabled: bool) -> None:
    """Start a job whose `_enable_ray_data_reconstruction` is `enabled`."""
    ray.init(job_config=JobConfig(_enable_ray_data_reconstruction=enabled))


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


def test_hash_shuffle_compression_alias(monkeypatch):
    """`hash_shuffle_compression` remains a deprecated alias of
    `shuffle_compression`."""

    ctx = ray.data.DataContext()

    with pytest.warns(DeprecationWarning, match="hash_shuffle_compression") as record:
        ctx.hash_shuffle_compression = "lz4"
    assert ctx.shuffle_compression == "lz4"
    # Warning has to be blamed on the caller, otherwise Python's default
    # filters drop it (`pytest.warns` alone passes at any `stacklevel`)
    assert record[0].filename == __file__

    ctx.shuffle_compression = "zstd"
    with pytest.warns(DeprecationWarning, match="hash_shuffle_compression") as record:
        assert ctx.hash_shuffle_compression == "zstd"
    assert record[0].filename == __file__

    # Deprecated env var is still honored, but the current one wins
    monkeypatch.setenv("RAY_DATA_HASH_SHUFFLE_COMPRESSION", "lz4")
    assert _deduce_default_shuffle_compression() == "lz4"
    monkeypatch.setenv("RAY_DATA_SHUFFLE_COMPRESSION", "none")
    assert _deduce_default_shuffle_compression() == "none"


@pytest.mark.parametrize("job_setting", [False, True])
def test_enable_ray_data_reconstruction_resolved_from_core_worker(
    shutdown_only, job_setting: bool
):
    """The job-level ray data reconstruction setting is read off the core worker."""
    from ray.data.context import DataContext

    original = DataContext.get_current()
    try:
        _init_job_with_reconstruction(job_setting)

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


def test_enable_ray_data_reconstruction_is_read_only(shutdown_only):
    """The job config is the only supported way to set this.

    Assigning on the `DataContext` doesn't reach the cluster -- Ray Core reads
    `_enable_ray_data_reconstruction` off the job config to decide whether to
    pin object lineage. We should not allow users to set this value on the context.
    """
    from ray.data.context import DataContext

    _init_job_with_reconstruction(True)
    context = DataContext()

    with pytest.raises(AttributeError):
        context.enable_ray_data_reconstruction = False

    assert context.enable_ray_data_reconstruction is True


@pytest.mark.parametrize("job_setting", [False, True])
def test_enable_ray_data_reconstruction_rejects_conflicting_override(
    shutdown_only, job_setting: bool
):
    """An override that disagrees with the job config is rejected on read.

    Reading is the last point at which the two can be compared, and letting a
    stale override through would silently diverge from Ray Core's lineage
    pinning decision for this job.
    """
    from ray.data.context import DataContext

    original = DataContext.get_current()
    try:
        _init_job_with_reconstruction(job_setting)

        context = DataContext(_enable_ray_data_reconstruction=not job_setting)

        with pytest.raises(ValueError, match=_RECONSTRUCTION_OVERRIDE_ERROR):
            _ = context.enable_ray_data_reconstruction

        # The override travels with the sealed per-Dataset copy, so a Dataset
        # can't pick it up unnoticed either.
        DataContext._set_current(context)
        with pytest.raises(ValueError, match=_RECONSTRUCTION_OVERRIDE_ERROR):
            _ = ray.data.range(1).context.enable_ray_data_reconstruction
    finally:
        DataContext._set_current(original)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
