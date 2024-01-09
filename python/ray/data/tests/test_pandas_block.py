import pandas as pd
import pytest

from ray.data._internal.pandas_block import PandasBlockAccessor


def test_append_column(ray_start_regular_shared):
    animals = ["Flamingo", "Centipede"]
    num_legs = [2, 100]
    block = pd.DataFrame({"animals": animals})

    block_accessor = PandasBlockAccessor.for_block(block)
    actual_block = block_accessor.append_column("num_legs", num_legs)

    expected_block = pd.DataFrame({"animals": animals, "num_legs": num_legs})
    assert actual_block.equals(expected_block)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
