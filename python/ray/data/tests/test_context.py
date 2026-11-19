import pytest

import ray
from ray.data.context import (
    ShuffleStrategy,
    _deduce_default_enable_disk_shuffle,
    _deduce_default_shuffle_compression,
)
from ray.job_config import JobConfig
from ray.util.annotations import RayDeprecationWarning

_RECONSTRUCTION_OVERRIDE_ERROR = (
    "enable_ray_data_reconstruction value does not match the "
    "disable_job_level_lineage_reconstruction value in the cluster"
)


def test_write_file_retry_on_errors_emits_deprecation_warning(caplog):
    ctx = ray.data.DataContext.get_current()
    with pytest.warns(DeprecationWarning):
        ctx.write_file_retry_on_errors = []


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, ("default",)),
        ('["timeout", "HTTP (429|503), retry"]', ("timeout", "HTTP (429|503), retry")),
        ("[]", ()),
    ],
)
def test_retry_errors_env(monkeypatch, value, expected):
    from ray.data.context import _env_retry_errors

    name = "RAY_DATA_RETRIED_IO_ERRORS"
    monkeypatch.delenv(name, raising=False)
    if value is not None:
        monkeypatch.setenv(name, value)
    assert _env_retry_errors(name, ("default",)) == expected


@pytest.mark.parametrize(
    "value", ["", "not-json", "{}", '"timeout"', "null", '["timeout", 1]']
)
def test_retry_errors_env_invalid(monkeypatch, value):
    from ray.data.context import _env_retry_errors

    name = "RAY_DATA_RETRIED_IO_ERRORS"
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match=name):
        _env_retry_errors(name, ())


def test_retry_errors_env_defaults_in_subprocess():
    import os
    import subprocess
    import sys

    env = os.environ.copy()
    env["RAY_DATA_RETRIED_IO_ERRORS"] = '["read-error"]'
    subprocess.check_call(
        [
            sys.executable,
            "-c",
            "from ray.data.context import DataContext, DEFAULT_RETRIED_IO_ERRORS; "
            "assert DEFAULT_RETRIED_IO_ERRORS == ('read-error',); "
            "a, b = DataContext(), DataContext(); "
            "assert a.retried_io_errors == ['read-error']; "
            "a.retried_io_errors.append('extra'); "
            "assert b.retried_io_errors == ['read-error']",
        ],
        env=env,
    )


def test_io_retry_max_attempts_default_and_validation():
    from ray.data.context import DEFAULT_IO_RETRY_MAX_ATTEMPTS

    ctx = ray.data.DataContext()
    assert ctx.io_retry_max_attempts == DEFAULT_IO_RETRY_MAX_ATTEMPTS
    assert ctx.io_retry_max_attempts >= 1

    with pytest.raises(ValueError, match="io_retry_max_attempts"):
        ray.data.DataContext(io_retry_max_attempts=0)


def test_io_retry_max_attempts_env_override():
    import os
    import subprocess
    import sys

    env = os.environ.copy()
    env["RAY_DATA_IO_RETRY_MAX_ATTEMPTS"] = "7"
    output = subprocess.check_output(
        [
            sys.executable,
            "-c",
            "from ray.data import context; "
            "print(context.DEFAULT_IO_RETRY_MAX_ATTEMPTS)",
        ],
        env=env,
        text=True,
    )
    assert output.strip() == "7"


def test_parquet_v2_io_retry_uses_context(monkeypatch):
    from ray._common import retry as retry_module
    from ray.data._internal.datasource_v2.formats.parquet.parquet_file_chunking_utils import (
        _with_io_retry,
    )

    monkeypatch.setattr(retry_module.time, "sleep", lambda _: None)

    ctx = ray.data.DataContext.get_current()
    original = ctx.io_retry_max_attempts
    original_errors = ctx.retried_io_errors
    ctx.io_retry_max_attempts = 3
    ctx.retried_io_errors = ["transient"]
    calls = []

    def _fail():
        calls.append(1)
        raise ValueError("transient")

    try:
        with pytest.raises(ValueError, match="transient"):
            _with_io_retry(_fail, "test op")
    finally:
        ctx.io_retry_max_attempts = original
        ctx.retried_io_errors = original_errors

    assert len(calls) == 3


@pytest.mark.parametrize("explicit_attempts, expected_attempts", [(None, 3), (2, 2)])
def test_retrying_filesystem_captures_context_attempts(
    monkeypatch, explicit_attempts, expected_attempts
):
    from pyarrow.fs import LocalFileSystem

    from ray._common import retry as retry_module
    from ray.data._internal.util import RetryingPyFileSystem

    monkeypatch.setattr(retry_module.time, "sleep", lambda _: None)
    ctx = ray.data.DataContext(io_retry_max_attempts=3)
    calls = []

    def fail():
        calls.append(1)
        raise ValueError("transient")

    with ray.data.DataContext.current(ctx):
        fs = RetryingPyFileSystem.wrap(
            LocalFileSystem(), ["transient"], max_attempts=explicit_attempts
        )
        ctx.io_retry_max_attempts = 5
        with pytest.raises(ValueError, match="transient"):
            fs.handler._retry_operation(fail, "test op")

    assert len(calls) == expected_attempts


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


def test_use_external_hash_shuffle_alias(monkeypatch):
    """`use_external_hash_shuffle` remains a deprecated alias of
    `use_disk_based_hash_shuffle`."""

    ctx = ray.data.DataContext()

    with pytest.warns(DeprecationWarning, match="use_external_hash_shuffle") as record:
        ctx.use_external_hash_shuffle = True
    assert ctx.use_disk_based_hash_shuffle is True
    # Warning has to be blamed on the caller, otherwise Python's default
    # filters drop it (`pytest.warns` alone passes at any `stacklevel`)
    assert record[0].filename == __file__

    ctx.use_disk_based_hash_shuffle = False
    with pytest.warns(DeprecationWarning, match="use_external_hash_shuffle") as record:
        assert ctx.use_external_hash_shuffle is False
    assert record[0].filename == __file__

    # Deprecated env var is still honored, but the current one wins
    monkeypatch.setenv("RAY_DATA_ENABLE_EXTERNAL_SHUFFLE", "1")
    assert _deduce_default_enable_disk_shuffle() is True
    monkeypatch.setenv("RAY_DATA_ENABLE_DISK_SHUFFLE", "0")
    assert _deduce_default_enable_disk_shuffle() is False


def test_enable_ray_data_reconstruction_defaults_false():
    """Resolution falls back to `False` without a connection and by default."""
    from ray.data.context import DataContext

    ray.shutdown()
    assert DataContext().enable_ray_data_reconstruction is False
    # A bare `ray.init()` starts a job with this default.
    assert JobConfig()._disable_job_level_lineage_reconstruction is False


@pytest.mark.parametrize("job_setting", [False, True])
def test_enable_ray_data_reconstruction_follows_job_config(
    shutdown_only, job_setting: bool
):
    """The setting is read from the core worker's job config, and an override
    that disagrees with it is rejected on read, also on a Dataset's sealed copy.
    """
    from ray.data.context import DataContext

    ray.init(
        num_cpus=1,
        job_config=JobConfig(_disable_job_level_lineage_reconstruction=job_setting),
    )
    original = DataContext.get_current()
    try:
        assert ray.data.range(1).context.enable_ray_data_reconstruction is job_setting

        conflicting = DataContext(_enable_ray_data_reconstruction=not job_setting)
        with pytest.raises(ValueError, match=_RECONSTRUCTION_OVERRIDE_ERROR):
            _ = conflicting.enable_ray_data_reconstruction

        DataContext._set_current(conflicting)
        with pytest.raises(ValueError, match=_RECONSTRUCTION_OVERRIDE_ERROR):
            _ = ray.data.range(1).context.enable_ray_data_reconstruction
    finally:
        DataContext._set_current(original)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
