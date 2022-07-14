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
    def __init__(self, factor: float = 1.0, use_gpu: bool = False):
        self.factor = factor
        self.preprocessor = DummyPreprocessor()
        self.use_gpu = use_gpu

    @classmethod
    def from_checkpoint(
        cls, checkpoint: Checkpoint, use_gpu: bool = False, **kwargs
    ) -> "DummyPredictor":
        checkpoint_data = checkpoint.to_dict()
        return cls(**checkpoint_data, use_gpu=use_gpu)

    def _predict_pandas(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        # Need to throw exception here instead of constructor to surface the
        # exception to pytest rather than ray worker.
        if self.use_gpu:
            raise ValueError("DummyPredictor does not support GPU prediction.")
        else:
            return data * self.factor


class DummyPredictorFS(DummyPredictor):
    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint, **kwargs) -> "DummyPredictor":
        with checkpoint.as_directory():
            # simulate reading
            time.sleep(1)
        checkpoint_data = checkpoint.to_dict()
        return cls(**checkpoint_data)


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


def test_batch_prediction_feature_cols():
    batch_predictor = BatchPredictor.from_checkpoint(
        Checkpoint.from_dict({"factor": 2.0}), DummyPredictor
    )

    test_dataset = ray.data.from_pandas(pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}))

    assert batch_predictor.predict(
        test_dataset, feature_columns=["a"]
    ).to_pandas().to_numpy().squeeze().tolist() == [4.0, 8.0, 12.0]


def test_batch_prediction_keep_cols():
    batch_predictor = BatchPredictor.from_checkpoint(
        Checkpoint.from_dict({"factor": 2.0}), DummyPredictor
    )

    test_dataset = ray.data.from_pandas(
        pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
    )

    output_df = batch_predictor.predict(
        test_dataset, feature_columns=["a"], keep_columns=["b"]
    ).to_pandas()

    assert set(output_df.columns) == {"a", "b"}

    assert output_df["a"].tolist() == [4.0, 8.0, 12.0]
    assert output_df["b"].tolist() == [4, 5, 6]


def test_automatic_enable_gpu_from_num_gpus_per_worker():
    """
    Test we automatically set underlying Predictor creation use_gpu to True if
    we found num_gpus_per_worker > 0 in BatchPredictor's predict() call.
    """

    batch_predictor = BatchPredictor.from_checkpoint(
        Checkpoint.from_dict({"factor": 2.0}), DummyPredictor
    )
    test_dataset = ray.data.range(4)

    with pytest.raises(
        ValueError, match="DummyPredictor does not support GPU prediction"
    ):
        _ = batch_predictor.predict(test_dataset, num_gpus_per_worker=1)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
