import sys
from unittest import mock

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest
from packaging.version import Version

from ray.tests.conftest import *  # noqa
from ray.util.dask import dataframe_optimize

try:
    import dask_expr  # noqa: F401

    DASK_EXPR_INSTALLED = True
except ImportError:
    DASK_EXPR_INSTALLED = False
    pass

if Version(dask.__version__) < Version("2025.1") and not DASK_EXPR_INSTALLED:
    from dask.dataframe.shuffle import SimpleShuffleLayer

    from ray.util.dask.optimizations import (
        MultipleReturnSimpleShuffleLayer,
        rewrite_simple_shuffle_layer,
    )

pytestmark = pytest.mark.skipif(
    Version(dask.__version__) >= Version("2025.1") or DASK_EXPR_INSTALLED,
    reason="Skip dask tests for Dask 2025.1+",
)


def test_rewrite_simple_shuffle_layer(ray_start_regular_shared):
    npartitions = 10
    df = dd.from_pandas(
        pd.DataFrame(
            np.random.randint(0, 100, size=(100, 2)), columns=["age", "grade"]
        ),
        npartitions=npartitions,
    )
    # We set max_branch=npartitions in order to ensure that the task-based
    # shuffle happens in a single stage, which is required in order for our
    # optimization to work.
    a = df.set_index(["age"], shuffle="tasks", max_branch=npartitions)

    dsk = a.__dask_graph__()
    keys = a.__dask_keys__()
    assert any(type(v) is SimpleShuffleLayer for k, v in dsk.layers.items())
    dsk = rewrite_simple_shuffle_layer(dsk, keys)
    assert all(type(v) is not SimpleShuffleLayer for k, v in dsk.layers.items())
    assert any(
        type(v) is MultipleReturnSimpleShuffleLayer for k, v in dsk.layers.items()
    )


@mock.patch("ray.util.dask.optimizations.rewrite_simple_shuffle_layer")
def test_dataframe_optimize(mock_rewrite, ray_start_regular_shared):
    def side_effect(dsk, keys):
        return rewrite_simple_shuffle_layer(dsk, keys)

    mock_rewrite.side_effect = side_effect
    with dask.config.set(dataframe_optimize=dataframe_optimize):
        npartitions = 10
        df = dd.from_pandas(
            pd.DataFrame(
                np.random.randint(0, 100, size=(100, 2)), columns=["age", "grade"]
            ),
            npartitions=npartitions,
        )
        # We set max_branch=npartitions in order to ensure that the task-based
        # shuffle happens in a single stage, which is required in order for our
        # optimization to work.
        a = df.set_index(["age"], shuffle="tasks", max_branch=npartitions).compute()

    assert mock_rewrite.call_count == 2
    assert a.index.is_monotonic_increasing


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
