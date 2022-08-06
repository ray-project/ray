import re
import time
from typing import Optional

import pandas as pd
import pytest
from ray.air.constants import MAX_REPR_LENGTH, PREPROCESSOR_KEY

import ray
from ray.air.checkpoint import Checkpoint
from ray.data import Preprocessor
from ray.tests.conftest import *  # noqa
from ray.train.batch_predictor import BatchPredictor
from ray.train.predictor import Predictor


class DummyPreprocessor(Preprocessor):
    _is_fittable = False

    def __init__(self, multiplier=2):
        self.multiplier = multiplier

    def _transform_pandas(self, df):
        return df * self.multiplier


def test_repr(shutdown_only):
    predictor = BatchPredictor.from_checkpoint(
        Checkpoint.from_dict({"factor": 2.0}),
        DummyPredictorFS,
    )

    representation = repr(predictor)

    assert len(representation) < MAX_REPR_LENGTH
    pattern = re.compile("^BatchPredictor\\((.*)\\)$")
    assert pattern.match(representation)


class DummyPredictor(Predictor):
    def __init__(
        self,
        factor: float = 1.0,
        preprocessor: Optional[Preprocessor] = None,
        use_gpu: bool = False,
    ):
        self.factor = factor
        self.use_gpu = use_gpu
        super().__init__(preprocessor)

    @classmethod
    def from_checkpoint(
        cls, checkpoint: Checkpoint, use_gpu: bool = False, **kwargs
    ) -> "DummyPredictor":
        checkpoint_data = checkpoint.to_dict()
        preprocessor = checkpoint.get_preprocessor()
        return cls(
            checkpoint_data["factor"], preprocessor=preprocessor, use_gpu=use_gpu
        )

    def _predict_pandas(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        # Need to throw exception here instead of constructor to surface the
        # exception to pytest rather than ray worker.
        if self.use_gpu and "allow_gpu" not in kwargs:
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
        preprocessor = checkpoint.get_preprocessor()
        return cls(checkpoint_data["factor"], preprocessor=preprocessor)


def test_separate_gpu_stage(shutdown_only):
    ray.init(num_gpus=1)
    batch_predictor = BatchPredictor.from_checkpoint(
        Checkpoint.from_dict({"factor": 2.0, PREPROCESSOR_KEY: DummyPreprocessor()}),
        DummyPredictor,
    )
    ds = batch_predictor.predict(
        ray.data.range_table(10),
        num_gpus_per_worker=1,
        separate_gpu_stage=True,
        allow_gpu=True,
    )
    stats = ds.stats()
    assert "Stage 1 read->map_batches:" in stats, stats
    assert "Stage 2 map_batches:" in stats, stats
    assert ds.max("value") == 36.0, ds

    ds = batch_predictor.predict(
        ray.data.range_table(10),
        num_gpus_per_worker=1,
        separate_gpu_stage=False,
        allow_gpu=True,
    )
    stats = ds.stats()
    assert "Stage 1 read:" in stats, stats
    assert "Stage 2 map_batches:" in stats, stats
    assert ds.max("value") == 36.0, ds


def test_automatic_enable_gpu_from_num_gpus_per_worker(shutdown_only):
    """
    Test we automatically set underlying Predictor creation use_gpu to True if
    we found num_gpus_per_worker > 0 in BatchPredictor's predict() call.
    """
    ray.init(num_gpus=1)

    batch_predictor = BatchPredictor.from_checkpoint(
        Checkpoint.from_dict({"factor": 2.0, PREPROCESSOR_KEY: DummyPreprocessor()}),
        DummyPredictor,
    )
    test_dataset = ray.data.range_table(4)

    with pytest.raises(
        ValueError, match="DummyPredictor does not support GPU prediction"
    ):
        _ = batch_predictor.predict(test_dataset, num_gpus_per_worker=1)


def test_batch_prediction():
    batch_predictor = BatchPredictor.from_checkpoint(
        Checkpoint.from_dict({"factor": 2.0, PREPROCESSOR_KEY: DummyPreprocessor()}),
        DummyPredictor,
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
        Checkpoint.from_dict({"factor": 2.0, PREPROCESSOR_KEY: DummyPreprocessor()}),
        DummyPredictorFS,
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
        Checkpoint.from_dict({"factor": 2.0, PREPROCESSOR_KEY: DummyPreprocessor()}),
        DummyPredictor,
    )

    test_dataset = ray.data.from_pandas(pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}))

    assert batch_predictor.predict(
        test_dataset, feature_columns=["a"]
    ).to_pandas().to_numpy().squeeze().tolist() == [4.0, 8.0, 12.0]


def test_batch_prediction_keep_cols():
    batch_predictor = BatchPredictor.from_checkpoint(
        Checkpoint.from_dict({"factor": 2.0, PREPROCESSOR_KEY: DummyPreprocessor()}),
        DummyPredictor,
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


def test_batch_prediction_from_pandas_udf():
    def check_truth(df, all_true=False):
        if all_true:
            return pd.DataFrame({"bool": [True] * len(df)})
        return pd.DataFrame({"bool": df["a"] == df["b"]})

    batch_predictor = BatchPredictor.from_pandas_udf(check_truth)

    test_dataset = ray.data.from_pandas(pd.DataFrame({"a": [1, 2, 3], "b": [1, 5, 6]}))

    output_ds = batch_predictor.predict(test_dataset)
    output = [row["bool"] for row in output_ds.take()]
    assert output == [True, False, False]

    output_ds = batch_predictor.predict(test_dataset, all_true=True)
    output = [row["bool"] for row in output_ds.take()]
    assert output == [True, True, True]


def test_get_and_set_preprocessor():
    """Test preprocessor can be set and get."""

    preprocessor = DummyPreprocessor(1)
    batch_predictor = BatchPredictor.from_checkpoint(
        Checkpoint.from_dict({"factor": 2.0, PREPROCESSOR_KEY: preprocessor}),
        DummyPredictor,
    )
    assert batch_predictor.get_preprocessor() == preprocessor

    test_dataset = ray.data.range(4)
    output_ds = batch_predictor.predict(test_dataset)
    assert output_ds.to_pandas().to_numpy().squeeze().tolist() == [
        0.0,
        2.0,
        4.0,
        6.0,
    ]

    preprocessor2 = DummyPreprocessor(2)
    batch_predictor.set_preprocessor(preprocessor2)
    assert batch_predictor.get_preprocessor() == preprocessor2

    output_ds = batch_predictor.predict(test_dataset)
    assert output_ds.to_pandas().to_numpy().squeeze().tolist() == [
        0.0,
        4.0,
        8.0,
        12.0,
    ]


def test_separate_gpu_stage_pipelined(shutdown_only):
    if ray.is_initialized():
        ray.shutdown()
    ray.init(num_gpus=1)
    batch_predictor = BatchPredictor.from_checkpoint(
        Checkpoint.from_dict({"factor": 2.0, PREPROCESSOR_KEY: DummyPreprocessor()}),
        DummyPredictor,
    )
    ds = batch_predictor.predict_pipelined(
        ray.data.range_table(5),
        blocks_per_window=1,
        num_gpus_per_worker=1,
        separate_gpu_stage=True,
        allow_gpu=True,
    )
    out = [x["value"] for x in ds.iter_rows()]
    stats = ds.stats()
    assert "Stage 1 read->map_batches:" in stats, stats
    assert "Stage 2 map_batches:" in stats, stats
    assert max(out) == 16.0, out

    ds = batch_predictor.predict_pipelined(
        ray.data.range_table(5),
        blocks_per_window=1,
        num_gpus_per_worker=1,
        separate_gpu_stage=False,
        allow_gpu=True,
    )
    out = [x["value"] for x in ds.iter_rows()]
    stats = ds.stats()
    assert "Stage 1 read:" in stats, stats
    assert "Stage 2 map_batches:" in stats, stats
    assert max(out) == 16.0, out


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
