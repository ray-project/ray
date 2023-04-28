import re
import pytest
import torch

import numpy as np
import pytorch_lightning as pl
from torch.utils.data import DataLoader

import ray
from ray.air.constants import MAX_REPR_LENGTH, MODEL_KEY
from ray.air.util.data_batch_conversion import _convert_batch_type_to_pandas
from ray.train.tests.conftest import *  # noqa
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
    trainer = pl.Trainer(max_epochs=0, accelerator="cpu")
    trainer.fit(model, train_dataloaders=DataLoader(torch.randn(1)))
    trainer.save_checkpoint(ckpt_path)


@pytest.mark.parametrize(
    "checkpoint_source", ["from_path", "from_uri", "from_directory"]
)
@pytest.mark.parametrize("use_gpu", [True, False])
@pytest.mark.parametrize("use_preprocessor", [True, False])
def test_predictor(
    mock_s3_bucket_uri,
    tmpdir,
    checkpoint_source: str,
    use_preprocessor: bool,
    use_gpu: bool,
):
    """Test LightningPredictor instantiation and prediction step."""
    model_config = {
        "layer_1": 32,
        "layer_2": 64,
        "lr": 1e-4,
    }
    model = LightningMNISTClassifier(**model_config)

    ckpt_path = str(tmpdir / MODEL_KEY)
    save_checkpoint(model, ckpt_path)

    # Test load checkpoint from local dir or remote path
    checkpoint = LightningCheckpoint.from_path(ckpt_path)
    if checkpoint_source == "from_uri":
        checkpoint.to_uri(mock_s3_bucket_uri)
        checkpoint = LightningCheckpoint.from_uri(mock_s3_bucket_uri)
    if checkpoint_source == "from_directory":
        checkpoint = LightningCheckpoint.from_directory(tmpdir)

    preprocessor = DummyPreprocessor() if use_preprocessor else None

    # Instantiate a predictor from checkpoint
    predictor = LightningPredictor.from_checkpoint(
        checkpoint=checkpoint,
        model_class=LightningMNISTClassifier,
        use_gpu=use_gpu,
        preprocessor=preprocessor,
        **model_config,
    )

    # Create synthetic input data
    batch_size = 10
    batch = np.random.rand(batch_size, 1, 28, 28).astype(np.float32)

    output = predictor.predict(batch)

    assert len(output["predictions"]) == batch_size
    if preprocessor:
        assert predictor.get_preprocessor().has_preprocessed


@pytest.mark.parametrize("use_gpu", [True, False])
def test_batch_predictor(tmpdir, use_gpu: bool):
    """Test batch prediction with a LightningPredictor."""
    batch_size = 32
    synthetic_data = _convert_batch_type_to_pandas(
        {
            "image": np.random.rand(batch_size, 1, 28, 28).astype(np.float32),
            "label": np.random.randint(0, 10, (batch_size,)),
        }
    )
    ds = ray.data.from_pandas(synthetic_data)

    # Create a PTL native checkpoint
    ckpt_path = str(tmpdir / MODEL_KEY)
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
        model_class=LightningMNISTClassifier,
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
