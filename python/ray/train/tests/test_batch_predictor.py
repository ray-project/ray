import time

import pandas as pd
import pytest

import ray
from ray.air.checkpoint import Checkpoint
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
