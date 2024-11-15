import pickle
import random
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
import ray.data
from ray.data._internal.pandas_block import PandasBlockAccessor
from ray.data.extensions.object_extension import object_extension_type_allowed


def test_append_column(ray_start_regular_shared):
    animals = ["Flamingo", "Centipede"]
    num_legs = [2, 100]
    block = pd.DataFrame({"animals": animals})

    block_accessor = PandasBlockAccessor.for_block(block)
    actual_block = block_accessor.append_column("num_legs", num_legs)

    expected_block = pd.DataFrame({"animals": animals, "num_legs": num_legs})
    assert actual_block.equals(expected_block)


@pytest.mark.skipif(
    object_extension_type_allowed(), reason="Objects can be put into Arrow"
)
def test_dict_fallback_to_pandas_block(ray_start_regular_shared):
    # If the UDF returns a column with dict, this throws
    # an error during block construction because we cannot cast dicts
    # to a supported arrow type. This test checks that the block
    # construction falls back to pandas and still succeeds.
    def fn(batch):
        batch["data_dict"] = [{"data": 0} for _ in range(len(batch["id"]))]
        return batch

    ds = ray.data.range(10).map_batches(fn)
    ds = ds.materialize()
    block = ray.get(ds.get_internal_block_refs()[0])
    # TODO: Once we support converting dict to a supported arrow type,
    # the block type should be Arrow.
    assert isinstance(block, pd.DataFrame)

    def fn2(batch):
        batch["data_none"] = [None for _ in range(len(batch["id"]))]
        return batch

    ds2 = ray.data.range(10).map_batches(fn2)
    ds2 = ds2.materialize()
    block = ray.get(ds2.get_internal_block_refs()[0])
    assert isinstance(block, pd.DataFrame)


class TestSizeBytes:
    def test_size_bytes_small(ray_start_regular_shared):
        animals = ["Flamingo", "Centipede"]
        block = pd.DataFrame({"animals": animals})
        block["animals"] = block["animals"].astype("string")

        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        # generally strings are hard, so let's use what Pandas gives us.
        # get memory usage from pandas
        memory_usage = block.memory_usage(index=True, deep=True).sum()
        # check that memory usage is within 10% of the size_bytes
        assert memory_usage * 0.9 <= bytes_size <= memory_usage * 1.1, (
            bytes_size,
            memory_usage,
        )

    def test_size_bytes_large_str(ray_start_regular_shared):
        animals = [
            random.choice(["alligator", "crocodile", "centipede", "flamingo"])
            for i in range(100_000)
        ]
        block = pd.DataFrame({"animals": animals})
        block["animals"] = block["animals"].astype("string")

        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        # String disk usage is wildly different from in-process memory usage
        memory_usage = block.memory_usage(index=True, deep=True).sum()
        # check that memory usage is within 10% of the size_bytes
        assert memory_usage * 0.9 <= bytes_size <= memory_usage * 1.1, (
            bytes_size,
            memory_usage,
        )

    def test_size_bytes_large_floats(ray_start_regular_shared):
        animals = [random.random() for i in range(100_000)]
        block = pd.DataFrame({"animals": animals})

        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        memory_usage = pickle.dumps(block).__sizeof__()
        # check that memory usage is within 10% of the size_bytes
        assert memory_usage * 0.9 <= bytes_size <= memory_usage * 1.1, (
            bytes_size,
            memory_usage,
        )

    def test_size_bytes_bytes_object(ray_start_regular_shared):
        def generate_data(batch):
            for _ in range(8):
                yield {"data": [[b"\x00" * 128 * 1024 * 128]]}

        ds = (
            ray.data.range(1, override_num_blocks=1)
            .map_batches(generate_data, batch_size=1)
            .map_batches(lambda batch: batch, batch_format="pandas")
        )

        true_value = 128 * 1024 * 128 * 8
        for bundle in ds.iter_internal_ref_bundles():
            size = bundle.size_bytes()
            # assert that true_value is within 10% of bundle.size_bytes()
            assert true_value * 0.9 <= size <= true_value * 1.1, (true_value, size)

    def test_size_bytes_unowned_numpy(ray_start_regular_shared):
        df = pd.DataFrame(
            {
                "data": [
                    np.random.randint(size=1024, low=0, high=100, dtype=np.int8)
                    for _ in range(1_000)
                ],
            }
        )

        block_accessor = PandasBlockAccessor.for_block(df)
        block_size = block_accessor.size_bytes()
        true_value = 1024 * 1000
        assert true_value * 0.9 <= block_size <= true_value * 1.1

    def test_size_bytes_nested_objects(ray_start_regular_shared):
        data = {
            "lists": [
                [random.randint(0, 100) for _ in range(10)] for _ in range(10_000)
            ],
        }
        block = pd.DataFrame(data)

        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        true_size = 10_000 * (
            sys.getsizeof([random.randint(0, 100) for _ in range(10)]) + 10 * 28
        )
        # List overhead + 10 integers per list

        assert true_size * 0.9 <= bytes_size <= true_size * 1.1, (
            bytes_size,
            true_size,
        )

    def test_size_bytes_mixed_types(ray_start_regular_shared):
        data = {
            "integers": [random.randint(0, 100) for _ in range(10_000)],
            "floats": [random.random() for _ in range(10_000)],
            "strings": [
                random.choice(["apple", "banana", "cherry"]) for _ in range(10_000)
            ],
        }
        block = pd.DataFrame(data)
        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        # Manually calculate the size
        int_size = 10_000 * 8
        float_size = 10_000 * 8
        str_size = (
            10_000 * sum([sys.getsizeof(s) for s in ["apple", "banana", "cherry"]]) // 3
        )

        true_size = int_size + float_size + str_size
        assert true_size * 0.9 <= bytes_size <= true_size * 1.1, (bytes_size, true_size)

    def test_size_bytes_nested_lists_strings(ray_start_regular_shared):
        data = {
            "nested_lists": [
                [random.choice(["a", "bb", "ccc"]) for _ in range(10)]
                for _ in range(5_000)
            ],
        }
        block = pd.DataFrame(data)
        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        # Manually calculate the size
        list_overhead = sys.getsizeof(
            block["nested_lists"].iloc[0]
        ) + 10 * sys.getsizeof("bb")
        true_size = 5_000 * list_overhead
        assert true_size * 0.9 <= bytes_size <= true_size * 1.1, (bytes_size, true_size)

    def test_size_bytes_multi_level_nesting(ray_start_regular_shared):
        data = {
            "complex": [
                {"list": [np.random.rand(10)], "value": {"key": "val"}}
                for _ in range(1_000)
            ],
        }
        block = pd.DataFrame(data)
        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        # Manually calculate the size
        list_overhead = sys.getsizeof([0] * 10) + 10 * 28
        dict_size = (
            sys.getsizeof({"key": "val"}) + sys.getsizeof("key") + sys.getsizeof("val")
        )
        true_size = 1_000 * (list_overhead + dict_size)
        assert true_size * 0.85 <= bytes_size <= true_size * 1.15, (
            bytes_size,
            true_size,
        )

    def test_size_bytes_boolean(ray_start_regular_shared):
        data = [random.choice([True, False, None]) for _ in range(100_000)]
        block = pd.DataFrame({"flags": pd.Series(data, dtype="boolean")})
        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        # No object case
        true_size = block.memory_usage(index=True, deep=True).sum()
        assert true_size * 0.9 <= bytes_size <= true_size * 1.1, (bytes_size, true_size)

    def test_size_bytes_arrow(ray_start_regular_shared):
        data = [
            random.choice(["alligator", "crocodile", "flamingo"]) for _ in range(50_000)
        ]
        arrow_dtype = pd.ArrowDtype(pa.string())
        block = pd.DataFrame({"animals": pd.Series(data, dtype=arrow_dtype)})
        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        true_size = block.memory_usage(index=True, deep=True).sum()
        assert true_size * 0.9 <= bytes_size <= true_size * 1.1, (bytes_size, true_size)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
