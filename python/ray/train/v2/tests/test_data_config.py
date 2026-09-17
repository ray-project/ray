from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
)
from ray.train._internal.data_config import DataConfig


def test_per_dataset_execution_options_single(ray_start_4_cpus):
    """A single ExecutionOptions object applies to all datasets."""
    execution_options = ExecutionOptions()
    execution_options.preserve_order = True
    execution_options.verbose_progress = True

    data_config = DataConfig(execution_options=execution_options)

    # All datasets should get the same options.
    for dataset_name in ("train", "test", "val"):
        assert data_config._resolve_execution_options(dataset_name) == execution_options


def test_per_dataset_execution_options_dict(ray_start_4_cpus):
    """Per-dataset ExecutionOptions only apply to configured datasets."""
    train_options = ExecutionOptions()
    train_options.preserve_order = True
    train_options.verbose_progress = True
    train_options.resource_limits = train_options.resource_limits.copy(cpu=4, gpu=2)

    test_options = ExecutionOptions()
    test_options.preserve_order = False
    test_options.verbose_progress = False
    test_options.resource_limits = test_options.resource_limits.copy(cpu=2, gpu=1)

    execution_options_dict = {
        "train": train_options,
        "test": test_options,
    }
    data_config = DataConfig(execution_options=execution_options_dict)

    assert data_config._resolve_execution_options("train") == train_options
    assert data_config._resolve_execution_options("test") == test_options

    # Datasets not in the dict fall back to default ingest options.
    assert data_config._resolve_execution_options("val") == (
        DataConfig.default_ingest_options()
    )


def test_per_dataset_execution_options_default(ray_start_4_cpus):
    """When execution_options is None, datasets use default ingest options."""
    default_options = DataConfig.default_ingest_options()

    data_config_none = DataConfig(execution_options=None)
    assert data_config_none._get_user_execution_options("train") is None
    assert data_config_none._resolve_execution_options("train") == default_options

    # empty dict is user-specified but contains no per-dataset overrides.
    data_config_empty = DataConfig(execution_options={})
    assert data_config_empty._get_user_execution_options("train") is None
    assert data_config_empty._resolve_execution_options("train") == default_options


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
