from unittest import mock

import pandas as pd
import pytest

import ray
from ray.air.checkpoint import Checkpoint
from ray.data import Preprocessor
from ray.train.predictor import Predictor, PredictorNotSerializableException


class DummyPreprocessor(Preprocessor):
    def transform_batch(self, df):
        return df * 2


class DummyPredictor(Predictor):
    def __init__(self, factor: float = 1.0):
        self.factor = factor
        self.preprocessor = DummyPreprocessor()

    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint, **kwargs) -> "DummyPredictor":
        checkpoint_data = checkpoint.to_dict()
        return cls(**checkpoint_data)

    def _predict_pandas(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return data * self.factor


def test_serialization():
    """Tests that Predictor instances are not serializable."""

    # Class is serializable.
    ray.put(DummyPredictor)

    # Instance is not serializable.
    predictor = DummyPredictor()
    with pytest.raises(PredictorNotSerializableException):
        ray.put(predictor)


def test_from_checkpoint():
    checkpoint = Checkpoint.from_dict({"factor": 2.0})
    assert DummyPredictor.from_checkpoint(checkpoint).factor == 2.0


@mock.patch(
    "ray.train.predictor.convert_batch_type_to_pandas",
    return_value=mock.DEFAULT,
)
@mock.patch(
    "ray.train.predictor.convert_pandas_to_batch_type",
    return_value=mock.DEFAULT,
)
def test_predict(convert_from_pandas_mock, convert_to_pandas_mock):
    checkpoint = Checkpoint.from_dict({"factor": 2.0})
    predictor = DummyPredictor.from_checkpoint(checkpoint)

    input = pd.DataFrame({"x": [1, 2, 3]})
    expected_output = input * 4.0
    actual_output = predictor.predict(input)
    assert actual_output.equals(expected_output)

    # Ensure the proper conversion functions are called.
    convert_to_pandas_mock.assert_called_once()
    convert_from_pandas_mock.assert_called_once()


def test_from_udf():
    def check_truth(df, all_true=False):
        if all_true:
            return pd.DataFrame({"bool": [True] * len(df)})
        return pd.DataFrame({"bool": df["a"] == df["b"]})

    predictor = Predictor.from_pandas_udf(check_truth)

    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 5, 6]})

    output = predictor.predict(df)
    output = output["bool"].tolist()
    assert output == [True, False, False]

    output = predictor.predict(df, all_true=True)
    output = output["bool"].tolist()
    assert output == [True, True, True]


@mock.patch.object(DummyPredictor, "_predict_pandas", return_value=mock.DEFAULT)
def test_kwargs(predict_pandas_mock):
    checkpoint = Checkpoint.from_dict({"factor": 2.0})
    predictor = DummyPredictor.from_checkpoint(checkpoint)

    input = pd.DataFrame({"x": [1, 2, 3]})
    predictor.predict(input, extra_arg=1)

    # Second element in call_args is the kwargs.
    assert "extra_arg" in predict_pandas_mock.call_args[1]
    assert predict_pandas_mock.call_args[1]["extra_arg"] == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
