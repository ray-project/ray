from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
)
from ray.train import DataConfig


def test_per_dataset_execution_options_single(ray_start_4_cpus):
    """Test that a single ExecutionOptions object applies to all datasets."""
    # Create execution options with specific settings
    execution_options = ExecutionOptions()
    execution_options.preserve_order = True
    execution_options.verbose_progress = True

    data_config = DataConfig(execution_options=execution_options)

    # Verify that all datasets get the same execution options
    train_options = data_config._get_execution_options("train")
    test_options = data_config._get_execution_options("test")
    val_options = data_config._get_execution_options("val")

    assert train_options.preserve_order is True
    assert train_options.verbose_progress is True
    assert test_options.preserve_order is True
    assert test_options.verbose_progress is True
    assert val_options.preserve_order is True
    assert val_options.verbose_progress is True


def test_per_dataset_execution_options_dict(ray_start_4_cpus):
    """Test that a dict of ExecutionOptions maps to specific datasets, and datasets
    not in the dict get default ingest options. Also tests resource limits."""
    # Create different execution options for different datasets
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

    # Verify that each dataset in the dict gets its specific options
    retrieved_train_options = data_config._get_execution_options("train")
    retrieved_test_options = data_config._get_execution_options("test")

    assert retrieved_train_options.preserve_order is True
    assert retrieved_train_options.verbose_progress is True
    assert retrieved_test_options.preserve_order is False
    assert retrieved_test_options.verbose_progress is False

    # Verify resource limits
    assert retrieved_train_options.resource_limits.cpu == 4
    assert retrieved_train_options.resource_limits.gpu == 2
    assert retrieved_test_options.resource_limits.cpu == 2
    assert retrieved_test_options.resource_limits.gpu == 1

    # Verify that a dataset not in the dict gets default options
    default_options = DataConfig.default_ingest_options()
    retrieved_val_options = data_config._get_execution_options("val")
    assert retrieved_val_options.preserve_order == default_options.preserve_order
    assert retrieved_val_options.verbose_progress == default_options.verbose_progress
    assert (
        retrieved_val_options.resource_limits.cpu == default_options.resource_limits.cpu
    )
    assert (
        retrieved_val_options.resource_limits.gpu == default_options.resource_limits.gpu
    )


def test_per_dataset_execution_options_default(ray_start_4_cpus):
    """Test that None or empty dict execution_options results in all datasets
    using default options."""
    # Test with None
    data_config_none = DataConfig(execution_options=None)
    default_options = DataConfig.default_ingest_options()
    retrieved_train_options = data_config_none._get_execution_options("train")
    retrieved_test_options = data_config_none._get_execution_options("test")

    assert retrieved_train_options.preserve_order == default_options.preserve_order
    assert retrieved_test_options.preserve_order == default_options.preserve_order

    # Test with empty dict
    data_config_empty = DataConfig(execution_options={})
    retrieved_train_options = data_config_empty._get_execution_options("train")
    retrieved_test_options = data_config_empty._get_execution_options("test")

    assert retrieved_train_options.preserve_order == default_options.preserve_order
    assert retrieved_test_options.preserve_order == default_options.preserve_order


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
