import sys

import daft
import numpy as np
import pandas as pd
import pytest

import ray


@pytest.fixture(scope="module")
def ray_start(request):
    try:
        yield ray.init(num_cpus=16)
    finally:
        ray.shutdown()


def test_daft_round_trip(ray_start):
    print("test_daft_round_trip")
    data = {
        "int_col": list(range(128)),
        "str_col": [str(i) for i in range(128)],
        "nested_list_col": [[i] * 3 for i in range(128)],
        "tensor_col": [np.array([[i] * 3] * 3) for i in range(128)],
    }
    print("data", data)
    df = daft.from_pydict(data)
    print("df", df)
    ds = ray.data.from_daft(df)
    print("ds", ds)
    pd.testing.assert_frame_equal(ds.to_pandas(), df.to_pandas())

    print("ds.to_daft()")
    df2 = ds.to_daft()
    print("df2", df2)
    df_pandas = df.to_pandas()
    print("df_pandas", df_pandas)
    df2_pandas = df2.to_pandas()
    print("df2_pandas", df2_pandas)

    for c in data.keys():
        # NOTE: tensor behavior on round-trip is different because Ray Data provides
        # Daft with more information about a column being a fixed-shape-tensor.
        #
        # Hence the Pandas representation of `df1` is "just" an object column, but
        # `df2` knows that this is actually a numpy fixed shaped tensor column
        if c == "tensor_col":
            np.testing.assert_equal(
                np.array(list(df_pandas[c])), df2_pandas[c].to_numpy()
            )
        else:
            pd.testing.assert_series_equal(df_pandas[c], df2_pandas[c])
    print("test_daft_round_trip passed")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
