import re

import pytest

from ray.air.constants import MAX_REPR_LENGTH
from ray.train.huggingface import HuggingFaceCheckpoint
from ray.train.lightgbm import LightGBMCheckpoint
from ray.train.rl import RLCheckpoint
from ray.train.sklearn import SklearnCheckpoint
from ray.train.tensorflow import TensorflowCheckpoint
from ray.train.xgboost import XGBoostCheckpoint
from ray.train.torch import TorchCheckpoint


@pytest.mark.parametrize(
    "checkpoint",
    [
        HuggingFaceCheckpoint(data_dict={"foo": "bar"}),
        LightGBMCheckpoint(data_dict={"foo": "bar"}),
        RLCheckpoint(data_dict={"foo": "bar"}),
        SklearnCheckpoint(data_dict={"foo": "bar"}),
        TensorflowCheckpoint(data_dict={"foo": "bar"}),
        XGBoostCheckpoint(data_dict={"foo": "bar"}),
        TorchCheckpoint(data_dict={"foo": "bar"}),
    ],
)
def test_repr(checkpoint):
    representation = repr(checkpoint)

    assert len(representation) < MAX_REPR_LENGTH
    pattern = re.compile(f"^{checkpoint.__class__.__name__}\\((.*)\\)$")
    assert pattern.match(representation)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
