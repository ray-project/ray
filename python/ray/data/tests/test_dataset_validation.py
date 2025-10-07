import logging
import sys
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data import Schema
from ray.data._internal.util import _check_pyarrow_version
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_column_name_type_check(ray_start_regular_shared):
    df = pd.DataFrame({"1": np.random.rand(10), "a": np.random.rand(10)})
    ds = ray.data.from_pandas(df)
    assert ds.schema() == Schema(pa.schema([("1", pa.float64()), ("a", pa.float64())]))
    assert ds.count() == 10

    df = pd.DataFrame({1: np.random.rand(10), "a": np.random.rand(10)})
    with pytest.raises(ValueError):
        ray.data.from_pandas(df)


@pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="TODO(scottjlee): Not working yet for py312"
)
def test_unsupported_pyarrow_versions_check(shutdown_only):
    ray.shutdown()

    # Test that unsupported pyarrow versions cause an error to be raised upon the
    # initial pyarrow use.
    ray.init(runtime_env={"pip": ["pyarrow==8.0.0"]})

    @ray.remote
    def should_error():
        _check_pyarrow_version()

    with pytest.raises(
        Exception,
        match=r".*Dataset requires pyarrow >= 9.0.0, but 8.0.0 is installed.*",
    ):
        ray.get(should_error.remote())


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
    ray.shutdown()
    ray.init(ray_start_cluster.address)
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)

    try:
        ds = ray.data.range(10)
        ds = ds.map_batches(lambda x: x)
        ds.take()
    except Exception as e:
        assert isinstance(e, ValueError)
        assert "exceeds the execution limits ExecutionResources(cpu=0.0" in str(e)


def test_nowarning_execute_with_cpu(ray_start_cluster):
    """Tests ExecutionPlan.execute() to ensure no warning is logged
    when there are available CPU resources."""
    # Create one node with CPUs to avoid triggering the Dataset warning
    ray.shutdown()
    ray.init(ray_start_cluster.address)

    logger = logging.getLogger("ray.data._internal.plan")
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
    sys.exit(pytest.main(["-v", __file__]))
