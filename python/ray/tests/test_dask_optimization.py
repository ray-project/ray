import dask
import dask.dataframe as dd
from dask.dataframe.shuffle import SimpleShuffleLayer
import mock
import numpy as np
import pandas as pd
import pytest

from ray.util.dask import dataframe_optimize
from ray.util.dask.optimizations import (rewrite_simple_shuffle_layer,
                                         MultipleReturnSimpleShuffleLayer)


def test_rewrite_simple_shuffle_layer():
    npartitions = 100
    df = dd.from_pandas(
        pd.DataFrame(
            np.random.randint(0, 100, size=(10000, 2)),
            columns=["age", "grade"]),
        npartitions=npartitions)
    # We set max_branch=npartitions in order to ensure that the task-based
    # shuffle happens in a single stage, which is required in order for our
    # optimization to work.
    a = df.set_index(
        ["age"], shuffle="tasks", max_branch=npartitions).head(
            10, npartitions=-1, compute=False)

    dsk = a.__dask_graph__()
    keys = a.__dask_keys__()
    assert any(type(v) is SimpleShuffleLayer for k, v in dsk.layers.items())
    dsk = rewrite_simple_shuffle_layer(dsk, keys)
    assert all(
        type(v) is not SimpleShuffleLayer for k, v in dsk.layers.items())
    assert any(
        type(v) is MultipleReturnSimpleShuffleLayer
        for k, v in dsk.layers.items())


@mock.patch("ray.util.dask.optimizations.rewrite_simple_shuffle_layer")
def test_dataframe_optimize(mock_rewrite):
    mock_rewrite.side_effect = lambda dsk, keys: dsk
    with dask.config.set(dataframe_optimize=dataframe_optimize):
        npartitions = 100
        df = dd.from_pandas(
            pd.DataFrame(
                np.random.randint(0, 100, size=(10000, 2)),
                columns=["age", "grade"]),
            npartitions=npartitions)
        # We set max_branch=npartitions in order to ensure that the task-based
        # shuffle happens in a single stage, which is required in order for our
        # optimization to work.
        a = df.set_index(
            ["age"], shuffle="tasks", max_branch=npartitions).head(
                10, npartitions=-1, compute=False)
        a.compute()

    assert mock_rewrite.call_count == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
