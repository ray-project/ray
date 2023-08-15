import re

import pytest

from ray.air.constants import MAX_REPR_LENGTH
from ray.train.huggingface import LegacyTransformersCheckpoint
from ray.train.lightgbm import LegacyLightGBMCheckpoint
from ray.train.sklearn import LegacySklearnCheckpoint
from ray.train.tensorflow import TensorflowCheckpoint
from ray.train.xgboost import LegacyXGBoostCheckpoint
from ray.train.torch import LegacyTorchCheckpoint


@pytest.mark.parametrize(
    "checkpoint",
    [
        LegacyTransformersCheckpoint(data_dict={"foo": "bar"}),
        LegacyLightGBMCheckpoint(data_dict={"foo": "bar"}),
        LegacySklearnCheckpoint(data_dict={"foo": "bar"}),
        TensorflowCheckpoint(data_dict={"foo": "bar"}),
        LegacyXGBoostCheckpoint(data_dict={"foo": "bar"}),
        LegacyTorchCheckpoint(data_dict={"foo": "bar"}),
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
