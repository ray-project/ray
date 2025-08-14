import os
from unittest.mock import patch

import pyarrow as pa
import pytest
from packaging.version import parse as parse_version


@pytest.fixture(scope="module")
def ray_start(request):
    """Initialize Ray with proper serialization format."""
    # TODO: Remove this once Daft issue is fixed to default to Cloudpickle
    # serialization format.
    # Force the serialization format to JSON for this test.
    # Refer Daft issue https://github.com/Eventual-Inc/Daft/issues/4828
    # and Ray issue https://github.com/ray-project/ray/issues/54837
    # for more details.

    # Set environment variable before importing ray
    os.environ["RAY_DATA_ARROW_EXTENSION_SERIALIZATION_LEGACY_JSON_FORMAT"] = "1"

    import ray
    import ray.air.util.tensor_extensions.arrow as arrow_module
    from ray.air.util.tensor_extensions.arrow import _SerializationFormat

    # Force the serialization format to JSON after import
    arrow_module.ARROW_EXTENSION_SERIALIZATION_FORMAT = _SerializationFormat.JSON

    try:
        # Set environment variable for Ray workers
        yield ray.init(
            num_cpus=16,
        )
    finally:
        ray.shutdown()


def test_from_daft_raises_error_on_pyarrow_14(ray_start):
    # This test assumes that `from_daft` calls `get_pyarrow_version` to get the
    # PyArrow version. We can't mock `__version__` on the module directly because
    # `get_pyarrow_version` caches the version.
    with patch(
        "ray.data.read_api.get_pyarrow_version", return_value=parse_version("14.0.0")
    ):
        import daft

        import ray

        with pytest.raises(RuntimeError):
            ray.data.from_daft(daft.from_pydict({"col": [0]}))


@pytest.mark.skipif(
    parse_version(pa.__version__) >= parse_version("14.0.0"),
    reason="https://github.com/ray-project/ray/issues/53278",
)
def test_daft_round_trip(ray_start):
    import daft
    import numpy as np
    import pandas as pd

    import ray

    data = {
        "int_col": list(range(128)),
        "str_col": [str(i) for i in range(128)],
        "nested_list_col": [[i] * 3 for i in range(128)],
        "tensor_col": [np.array([[i] * 3] * 3) for i in range(128)],
    }
    df = daft.from_pydict(data)
    ds = ray.data.from_daft(df)
    pd.testing.assert_frame_equal(ds.to_pandas(), df.to_pandas())

    df2 = ds.to_daft()
    df_pandas = df.to_pandas()
    df2_pandas = df2.to_pandas()

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
