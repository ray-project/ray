from ray.tests.conftest import *  # noqa

import pytest

import ray
from ray import workflow


@workflow.step
def gen_dataset():
    # TODO(ekl) seems checkpointing hangs with nested refs of
    # LazyBlockList.
    return ray.data.range(1000).map(lambda x: x)


@workflow.step
def transform_dataset(in_data):
    return in_data.map(lambda x: x * 2)


@workflow.step
def sum_dataset(ds):
    return ds.sum()


def test_dataset(workflow_start_regular):
    ds_ref = gen_dataset.step()
    transformed_ref = transform_dataset.step(ds_ref)
    output_ref = sum_dataset.step(transformed_ref)

    result = output_ref.run()
    assert result == 2 * sum(range(1000))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
