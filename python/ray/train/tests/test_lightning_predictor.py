import re
import pytest
import torch

import numpy as np
import pytorch_lightning as pl
from torch.utils.data import DataLoader

from ray.air.constants import MAX_REPR_LENGTH, MODEL_KEY
from ray.train.tests.conftest import *  # noqa
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


@pytest.mark.parametrize("use_gpu", [True, False])
@pytest.mark.parametrize("use_preprocessor", [True, False])
def test_predictor(
    tmpdir,
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


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
