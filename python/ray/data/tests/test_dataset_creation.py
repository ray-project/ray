import sys

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces.ref_bundle import (
    _ref_bundles_iterator_to_block_refs_list,
)
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.util import extract_values
from ray.tests.conftest import *  # noqa


@pytest.mark.parametrize(
    "input_blocks",
    [
        [pd.DataFrame({"column": ["spam"]}), pd.DataFrame({"column": ["ham", "eggs"]})],
        [
            pa.Table.from_pydict({"column": ["spam"]}),
            pa.Table.from_pydict({"column": ["ham", "eggs"]}),
        ],
    ],
)
def test_from_blocks(input_blocks, ray_start_regular_shared):
    ds = ray.data.from_blocks(input_blocks)

    bundles = ds.iter_internal_ref_bundles()
    output_blocks = ray.get(_ref_bundles_iterator_to_block_refs_list(bundles))
    assert len(input_blocks) == len(output_blocks)
    assert all(
        input_block.equals(output_block)
        for input_block, output_block in zip(input_blocks, output_blocks)
    )


def test_from_items(ray_start_regular_shared):
    ds = ray.data.from_items(["hello", "world"])
    assert extract_values("item", ds.take()) == ["hello", "world"]
    assert isinstance(next(iter(ds.iter_batches(batch_format=None))), pa.Table)


@pytest.mark.parametrize("parallelism", list(range(1, 21)))
def test_from_items_parallelism(ray_start_regular_shared, parallelism):
    # Test that specifying parallelism yields the expected number of blocks.
    n = 20
    records = [{"a": i} for i in range(n)]
    ds = ray.data.from_items(records, override_num_blocks=parallelism)
    out = ds.take_all()
    assert out == records
    assert ds._plan.initial_num_blocks() == parallelism


def test_from_items_parallelism_truncated(ray_start_regular_shared):
    # Test that specifying parallelism greater than the number of items is truncated to
    # the number of items.
    n = 10
    parallelism = 20
    records = [{"a": i} for i in range(n)]
    ds = ray.data.from_items(records, override_num_blocks=parallelism)
    out = ds.take_all()
    assert out == records
    assert ds._plan.initial_num_blocks() == n


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
