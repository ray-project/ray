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
from ray.data.extensions.object_extension import _object_extension_type_allowed

# Set seed for the test for size as it related to sampling
np.random.seed(42)


def test_append_column(ray_start_regular_shared):
    animals = ["Flamingo", "Centipede"]
    num_legs = [2, 100]
    block = pd.DataFrame({"animals": animals})

    block_accessor = PandasBlockAccessor.for_block(block)
    actual_block = block_accessor.append_column("num_legs", num_legs)

    expected_block = pd.DataFrame({"animals": animals, "num_legs": num_legs})
    assert actual_block.equals(expected_block)


@pytest.mark.skipif(
    _object_extension_type_allowed(), reason="Objects can be put into Arrow"
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
    def test_small(ray_start_regular_shared):
        animals = ["Flamingo", "Centipede"]
        block = pd.DataFrame({"animals": animals})

        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        # check that memory usage is within 10% of the size_bytes
        # For strings, Pandas seems to be fairly accurate, so let's use that.
        memory_usage = block.memory_usage(index=True, deep=True).sum()
        assert bytes_size == pytest.approx(memory_usage, rel=0.1), (
            bytes_size,
            memory_usage,
        )

    def test_large_str(ray_start_regular_shared):
        animals = [
            random.choice(["alligator", "crocodile", "centipede", "flamingo"])
            for i in range(100_000)
        ]
        block = pd.DataFrame({"animals": animals})
        block["animals"] = block["animals"].astype("string")

        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        memory_usage = block.memory_usage(index=True, deep=True).sum()
        assert bytes_size == pytest.approx(memory_usage, rel=0.1), (
            bytes_size,
            memory_usage,
        )

    def test_large_str_object(ray_start_regular_shared):
        """Note - this test breaks if you refactor/move the list of animals."""
        num = 100_000
        animals = [
            random.choice(["alligator", "crocodile", "centipede", "flamingo"])
            for i in range(num)
        ]
        block = pd.DataFrame({"animals": animals})

        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        memory_usage = sum([sys.getsizeof(animal) for animal in animals])

        assert bytes_size == pytest.approx(memory_usage, rel=0.1), (
            bytes_size,
            memory_usage,
        )

    def test_large_floats(ray_start_regular_shared):
        animals = [random.random() for i in range(100_000)]
        block = pd.DataFrame({"animals": animals})

        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        memory_usage = pickle.dumps(block).__sizeof__()
        # check that memory usage is within 10% of the size_bytes
        assert bytes_size == pytest.approx(memory_usage, rel=0.1), (
            bytes_size,
            memory_usage,
        )

    def test_bytes_object(ray_start_regular_shared):
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
            assert size == pytest.approx(true_value, rel=0.1), (
                size,
                true_value,
            )

    def test_nested_numpy(ray_start_regular_shared):
        size = 1024
        rows = 1_000
        data = [
            np.random.randint(size=size, low=0, high=100, dtype=np.int8)
            for _ in range(rows)
        ]
        df = pd.DataFrame({"data": data})

        block_accessor = PandasBlockAccessor.for_block(df)
        block_size = block_accessor.size_bytes()
        true_value = rows * size
        assert block_size == pytest.approx(true_value, rel=0.1), (
            block_size,
            true_value,
        )

    def test_nested_objects(ray_start_regular_shared):
        size = 10
        rows = 10_000
        lists = [[random.randint(0, 100) for _ in range(size)] for _ in range(rows)]
        data = {"lists": lists}
        block = pd.DataFrame(data)

        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        # List overhead + 10 integers per list
        true_size = rows * (
            sys.getsizeof([random.randint(0, 100) for _ in range(size)]) + size * 28
        )

        assert bytes_size == pytest.approx(true_size, rel=0.1), (
            bytes_size,
            true_size,
        )

    def test_mixed_types(ray_start_regular_shared):
        rows = 10_000

        data = {
            "integers": [random.randint(0, 100) for _ in range(rows)],
            "floats": [random.random() for _ in range(rows)],
            "strings": [
                random.choice(["apple", "banana", "cherry"]) for _ in range(rows)
            ],
            "object": [b"\x00" * 128 for _ in range(rows)],
        }
        block = pd.DataFrame(data)
        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        # Manually calculate the size
        int_size = rows * 8
        float_size = rows * 8
        str_size = sum(sys.getsizeof(string) for string in data["strings"])
        object_size = rows * sys.getsizeof(b"\x00" * 128)

        true_size = int_size + float_size + str_size + object_size
        assert bytes_size == pytest.approx(true_size, rel=0.1), (bytes_size, true_size)

    def test_nested_lists_strings(ray_start_regular_shared):
        rows = 5_000
        nested_lists = ["a"] * 3 + ["bb"] * 4 + ["ccc"] * 3
        data = {
            "nested_lists": [nested_lists for _ in range(rows)],
        }
        block = pd.DataFrame(data)
        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        # Manually calculate the size
        list_overhead = sys.getsizeof(block["nested_lists"].iloc[0]) + sum(
            [sys.getsizeof(x) for x in nested_lists]
        )
        true_size = rows * list_overhead
        assert bytes_size == pytest.approx(true_size, rel=0.1), (bytes_size, true_size)

    @pytest.mark.parametrize("size", [10, 1024])
    def test_multi_level_nesting(ray_start_regular_shared, size):
        rows = 1_000
        data = {
            "complex": [
                {"list": [np.random.rand(size)], "value": {"key": "val"}}
                for _ in range(rows)
            ],
        }
        block = pd.DataFrame(data)
        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        numpy_size = np.random.rand(size).nbytes

        values = ["list", "value", "key", "val"]
        str_size = sum([sys.getsizeof(v) for v in values])

        list_ref_overhead = sys.getsizeof([np.random.rand(size)])

        dict_overhead1 = sys.getsizeof({"key": "val"})

        dict_overhead3 = sys.getsizeof(
            {"list": [np.random.rand(size)], "value": {"key": "val"}}
        )

        true_size = (
            numpy_size + str_size + list_ref_overhead + dict_overhead1 + dict_overhead3
        ) * rows
        assert bytes_size == pytest.approx(true_size, rel=0.15), (
            bytes_size,
            true_size,
        )

    def test_boolean(ray_start_regular_shared):
        data = [random.choice([True, False, None]) for _ in range(100_000)]
        block = pd.DataFrame({"flags": pd.Series(data, dtype="boolean")})
        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        # No object case
        true_size = block.memory_usage(index=True, deep=True).sum()
        assert bytes_size == pytest.approx(true_size, rel=0.1), (bytes_size, true_size)

    def test_arrow(ray_start_regular_shared):
        data = [
            random.choice(["alligator", "crocodile", "flamingo"]) for _ in range(50_000)
        ]
        arrow_dtype = pd.ArrowDtype(pa.string())
        block = pd.DataFrame({"animals": pd.Series(data, dtype=arrow_dtype)})
        block_accessor = PandasBlockAccessor.for_block(block)
        bytes_size = block_accessor.size_bytes()

        true_size = block.memory_usage(index=True, deep=True).sum()
        assert bytes_size == pytest.approx(true_size, rel=0.1), (bytes_size, true_size)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
