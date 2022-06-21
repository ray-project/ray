import pandas as pd
import pytest
from unittest import mock

import ray
from ray.air.checkpoint import Checkpoint
from ray.train.predictor import Predictor, PredictorNotSerializableException
from ray.data import Preprocessor
from ray.train.batch_predictor import BatchPredictor
from ray.train.predictor import Predictor


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
        return DummyPredictor(**checkpoint_data)

    def _predict_pandas(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return data * self.factor


<<<<<<< HEAD:python/ray/air/tests/test_predictor.py
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
    "ray.air.predictor.convert_batch_type_to_pandas",
    return_value=mock.DEFAULT,
)
@mock.patch(
    "ray.air.predictor.convert_pandas_to_batch_type",
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


@mock.patch.object(DummyPredictor, "_predict_pandas", return_value=mock.DEFAULT)
def test_kwargs(predict_pandas_mock):
    checkpoint = Checkpoint.from_dict({"factor": 2.0})
    predictor = DummyPredictor.from_checkpoint(checkpoint)

    input = pd.DataFrame({"x": [1, 2, 3]})
    predictor.predict(input, extra_arg=1)

    # Second element in call_args is the kwargs.
    assert "extra_arg" in predict_pandas_mock.call_args[1]
    assert predict_pandas_mock.call_args[1]["extra_arg"] == 1
=======
class DummyPredictorFS(DummyPredictor):
    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint, **kwargs) -> "DummyPredictor":
        with checkpoint.as_directory():
            # simulate reading
            time.sleep(1)
        checkpoint_data = checkpoint.to_dict()
        return DummyPredictor(**checkpoint_data)


def test_batch_prediction():
    batch_predictor = BatchPredictor.from_checkpoint(
        Checkpoint.from_dict({"factor": 2.0}), DummyPredictor
    )

    test_dataset = ray.data.range(4)
    ds = batch_predictor.predict(test_dataset)
    # Check fusion occurred.
    assert "read->map_batches" in ds.stats(), ds.stats()
    assert ds.to_pandas().to_numpy().squeeze().tolist() == [
        0.0,
        4.0,
        8.0,
        12.0,
    ]

    test_dataset = ray.data.from_items([1.0, 2.0, 3.0, 4.0])
    assert next(
        batch_predictor.predict_pipelined(
            test_dataset, blocks_per_window=2
        ).iter_datasets()
    ).to_pandas().to_numpy().squeeze().tolist() == [
        4.0,
        8.0,
    ]


def test_batch_prediction_fs():
    batch_predictor = BatchPredictor.from_checkpoint(
        Checkpoint.from_dict({"factor": 2.0}), DummyPredictorFS
    )

    test_dataset = ray.data.from_items([1.0, 2.0, 3.0, 4.0] * 32).repartition(8)
    assert (
        batch_predictor.predict(test_dataset, min_scoring_workers=4)
        .to_pandas()
        .to_numpy()
        .squeeze()
        .tolist()
        == [
            4.0,
            8.0,
            12.0,
            16.0,
        ]
        * 32
    )
>>>>>>> 5c58d43df27f171418a69612430ad0c7b120c345:python/ray/train/tests/test_batch_predictor.py


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
