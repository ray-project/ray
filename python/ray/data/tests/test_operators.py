import pytest

import ray
from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.execution.operators.all_to_all_operator import AllToAllOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer


def test_input_data_buffer():
    # Create with bundles.
    # Check we return all bundles in order.
    assert False


def test_map_operator():
    # Create with inputs.
    # Check we return transformed bundles in order.
    # Check dataset stats.
    # Check memory stats and ownership flag.
    assert False


def test_map_operator_ray_args():
    # Check it uses ray remote args.
    assert False


def test_map_operator_shutdown():
    # Check shutdown cancels tasks.
    assert False


def test_map_operator_min_rows_per_batch():
    # Test batching behavior.
    assert False


def test_all_to_all_operator():
    # Create with bundles.
    # Check we return transformed bundles.
    # Check dataset stats.
    assert False


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
