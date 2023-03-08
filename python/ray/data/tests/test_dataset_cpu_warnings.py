import pytest
from unittest.mock import patch

import ray
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


class LoggerWarningCalled(Exception):
    """Custom exception used in test_warning_execute_with_no_cpu() and
    test_nowarning_execute_with_cpu(). Raised when the `logger.warning` method
    is called, so that we can kick out of `plan.execute()` by catching this Exception
    and check logging was done properly."""

    pass


def test_warning_execute_with_no_cpu(ray_start_cluster):
    """Tests ExecutionPlan.execute() to ensure a warning is logged
    when no CPU resources are available."""
    # Create one node with no CPUs to trigger the Dataset warning
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)

    logger = DatasetLogger("ray.data._internal.plan").get_logger()
    with patch.object(
        logger,
        "warning",
        side_effect=LoggerWarningCalled,
    ) as mock_logger:
        try:
            ds = ray.data.range(10)
            ds = ds.map_batches(lambda x: x)
            ds.take()
        except Exception as e:
            if ray.data.context.DatasetContext.get_current().use_streaming_executor:
                assert isinstance(e, ValueError)
                assert "exceeds the execution limits ExecutionResources(cpu=0.0" in str(
                    e
                )
            else:
                assert isinstance(e, LoggerWarningCalled)
                logger_args, logger_kwargs = mock_logger.call_args
                assert (
                    "Warning: The Ray cluster currently does not have "
                    in logger_args[0]
                )


def test_nowarning_execute_with_cpu(ray_start_cluster_init):
    """Tests ExecutionPlan.execute() to ensure no warning is logged
    when there are available CPU resources."""
    # Create one node with CPUs to avoid triggering the Dataset warning
    ray.init(ray_start_cluster_init.address)

    logger = DatasetLogger("ray.data._internal.plan").get_logger()
    with patch.object(
        logger,
        "warning",
        side_effect=LoggerWarningCalled,
    ) as mock_logger:
        ds = ray.data.range(10)
        ds = ds.map_batches(lambda x: x)
        ds.take()
        mock_logger.assert_not_called()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
