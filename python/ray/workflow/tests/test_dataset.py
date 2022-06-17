from ray.tests.conftest import *  # noqa

import pytest

import ray
from ray import workflow


@ray.remote
def gen_dataset():
    return ray.data.range(1000).map(lambda x: x)


@ray.remote
def gen_dataset_1():
    return ray.data.range(1000)


@ray.remote
def gen_dataset_2():
    return ray.data.range_table(1000)


@ray.remote
def transform_dataset(in_data):
    return in_data.map(lambda x: x * 2)


@ray.remote
def transform_dataset_1(in_data):
    return in_data.map(lambda r: {"v2": r["value"] * 2})


@ray.remote
def sum_dataset(ds):
    return ds.sum()


def test_dataset(workflow_start_regular_shared):
    ds_ref = gen_dataset.bind()
    transformed_ref = transform_dataset.bind(ds_ref)
    output_ref = sum_dataset.bind(transformed_ref)

    result = workflow.create(output_ref).run()
    assert result == 2 * sum(range(1000))


def test_dataset_1(workflow_start_regular_shared):
    ds_ref = gen_dataset_1.bind()
    transformed_ref = transform_dataset.bind(ds_ref)
    output_ref = sum_dataset.bind(transformed_ref)

    result = workflow.create(output_ref).run()
    assert result == 2 * sum(range(1000))


def test_dataset_2(workflow_start_regular_shared):
    ds_ref = gen_dataset_2.bind()
    transformed_ref = transform_dataset_1.bind(ds_ref)
    output_ref = sum_dataset.bind(transformed_ref)

    result = workflow.create(output_ref).run()
    assert result == 2 * sum(range(1000))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
