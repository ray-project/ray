import pytest

import pandas as pd
from ray.ml.utils.torch_utils import convert_pandas_to_torch_tensor

def test_convert_pandas_to_torch_tensor():
    df = pd.DataFrame([])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))