import re
import pytest
import tempfile
import torch

import numpy as np
import pytorch_lightning as pl
from torch.utils.data import DataLoader

import ray
from ray.air.constants import MAX_REPR_LENGTH, MODEL_KEY
from ray.air.util.data_batch_conversion import convert_batch_type_to_pandas
from ray.train.batch_predictor import BatchPredictor
from ray.train.lightning import LightningCheckpoint, LightningPredictor
from ray.train.tests.dummy_preprocessor import DummyPreprocessor
from ray.train.tests.lightning_test_utils import LightningMNISTClassifier


def test_repr():
    model = pl.LightningModule()
    predictor = LightningPredictor(model)

    representation = repr(predictor)

    assert len(representation) < MAX_REPR_LENGTH
    pattern = re.compile("^LightningPredictor\\((.*)\\)$")
    assert pattern.match(representation)


def save_checkpoint(model: pl.LightningModule, ckpt_path: str):
    trainer = pl.Trainer(max_epochs=0)
    trainer.fit(model, train_dataloaders=DataLoader(torch.randn(1)))
    trainer.save_checkpoint(ckpt_path)


@pytest.mark.parametrize("fs", ["s3", "local"])
@pytest.mark.parametrize("use_gpu", [True, False])
@pytest.mark.parametrize("use_preprocessor", [True, False])
@pytest.mark.parametrize("batch_format", ["numpy", "pandas"])
def test_predictor(
    mock_s3_bucket_uri,
    fs: str,
    use_preprocessor: bool,
    use_gpu: bool,
    batch_format: str,
):
    model_config = {
        "layer_1": 32,
        "layer_2": 64,
        "lr": 1e-4,
    }
    model = LightningMNISTClassifier(**model_config)

    with tempfile.TemporaryDirectory() as tmpdir:
        ckpt_path = f"{tmpdir}/{MODEL_KEY}"
        save_checkpoint(model, ckpt_path)

        # Test load checkpoint from local dir or remote path
        checkpoint = LightningCheckpoint.from_path(ckpt_path)
        if fs == "s3":
            checkpoint.to_uri(mock_s3_bucket_uri)
            checkpoint = LightningCheckpoint.from_uri(mock_s3_bucket_uri)

        preprocessor = DummyPreprocessor() if use_preprocessor else None

        # Instantiate a predictor from checkpoint
        predictor = LightningPredictor.from_checkpoint(
            checkpoint=checkpoint,
            model=LightningMNISTClassifier,
            use_gpu=use_gpu,
            preprocessor=preprocessor,
            **model_config,
        )

        # Create synthetic input data
        batch_size = 10
        batch = np.random.rand(batch_size, 1, 28, 28).astype(np.float32)
        if batch_format == "pandas":
            batch = convert_batch_type_to_pandas(batch)

        output = predictor.predict(batch)

        assert len(output["predictions"]) == batch_size
        if preprocessor:
            assert predictor.get_preprocessor().has_preprocessed


@pytest.mark.parametrize("use_gpu", [True, False])
def test_batch_predictor(use_gpu: bool):
    with tempfile.TemporaryDirectory() as tmpdir:
        batch_size = 32
        synthetic_data = convert_batch_type_to_pandas(
            {
                "image": np.random.rand(batch_size, 1, 28, 28).astype(np.float32),
                "label": np.random.randint(0, 10, (batch_size,)),
            }
        )
        ds = ray.data.from_pandas(synthetic_data)

        # Create a PTL native checkpoint
        ckpt_path = f"{tmpdir}/{MODEL_KEY}"
        model_config = {
            "layer_1": 32,
            "layer_2": 64,
            "lr": 1e-4,
        }
        model = LightningMNISTClassifier(**model_config)
        save_checkpoint(model, ckpt_path)

        # Create a LightningCheckpoint from the native checkpoint
        checkpoint = LightningCheckpoint.from_path(ckpt_path)

        batch_predictor = BatchPredictor(
            checkpoint=checkpoint,
            predictor_cls=LightningPredictor,
            use_gpu=use_gpu,
            model=LightningMNISTClassifier,
            **model_config,
        )

        predictions = batch_predictor.predict(
            ds,
            feature_columns=["image"],
            keep_columns=["label"],
            batch_size=8,
            min_scoring_workers=2,
            max_scoring_workers=2,
            num_gpus_per_worker=1 if use_gpu else 0,
        )

        assert predictions.count() == batch_size


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
