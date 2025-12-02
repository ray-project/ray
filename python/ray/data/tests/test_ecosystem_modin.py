import sys

import pandas as pd
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_from_modin(ray_start_regular_shared):
    import modin.pandas as mopd

    df = pd.DataFrame(
        {"one": list(range(100)), "two": list(range(100))},
    )
    modf = mopd.DataFrame(df)
    ds = ray.data.from_modin(modf)
    dfds = ds.to_pandas()
    assert df.equals(dfds)


def test_to_modin(ray_start_regular_shared):
    # create two modin dataframes
    # one directly from a pandas dataframe, and
    # another from ray.dataset created from the original pandas dataframe
    #
    import modin.pandas as mopd

    df = pd.DataFrame(
        {"one": list(range(100)), "two": list(range(100))},
    )
    modf1 = mopd.DataFrame(df)
    ds = ray.data.from_pandas([df])
    modf2 = ds.to_modin()
    assert modf1.equals(modf2)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
