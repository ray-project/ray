import numpy as np
import pyarrow as pa
import pytest

import ray
from ray._private.test_utils import run_string_as_driver
from ray.data._internal.arrow_block import ArrowBlockAccessor


def test_append_column(ray_start_regular_shared):
    animals = ["Flamingo", "Centipede"]
    num_legs = [2, 100]
    block = pa.Table.from_pydict({"animals": animals})

    block_accessor = ArrowBlockAccessor.for_block(block)
    actual_block = block_accessor.append_column("num_legs", num_legs)

    expected_block = pa.Table.from_pydict({"animals": animals, "num_legs": num_legs})
    assert actual_block.equals(expected_block)


def test_register_arrow_types(tmp_path):
    # Test that our custom arrow extension types are registered on initialization.
    ds = ray.data.from_items(np.zeros((8, 8, 8), dtype=np.int64))
    tmp_file = f"{tmp_path}/test.parquet"
    ds.write_parquet(tmp_file)

    ds = ray.data.read_parquet(tmp_file)
    schema = (
        "Column  Type\n------  ----\nitem    numpy.ndarray(shape=(8, 8), dtype=int64)"
    )
    assert str(ds.schema()) == schema

    # Also run in driver script to eliminate existing imports.
    driver_script = """import ray
ds = ray.data.read_parquet("{0}")
schema = ds.schema()
assert str(schema) == \"\"\"{1}\"\"\"
""".format(
        tmp_file, schema
    )
    run_string_as_driver(driver_script)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
