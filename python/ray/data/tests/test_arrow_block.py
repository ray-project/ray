import pyarrow as pa
import pytest

from ray.data._internal.arrow_block import ArrowBlockAccessor


def test_append_column(ray_start_regular_shared):
    animals = ["Flamingo", "Centipede"]
    num_legs = [2, 100]
    block = pa.Table.from_pydict({"animals": animals})

    block_accessor = ArrowBlockAccessor.for_block(block)
    actual_block = block_accessor.append_column("num_legs", num_legs)

    expected_block = pa.Table.from_pydict({"animals": animals, "num_legs": num_legs})
    assert actual_block.equals(expected_block)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
