import re
import numpy as np
import pytest
import pytorch_lightning as pl
import ray
from ray.train.lightning import LightningPredictor, LightningCheckpoint
from ray.train.tests.lightning_test_utils import LightningMNISTClassifier, MNISTDataModule, LightningMNISTModelConfig
from ray.train.batch_predictor import BatchPredictor
from ray.air.constants import MAX_REPR_LENGTH
from ray.train.tests.dummy_preprocessor import DummyPreprocessor
from ray.air.util.data_batch_conversion import convert_batch_type_to_pandas
import tempfile
from torch.utils.data import DataLoader

def test_repr():
    model = pl.LightningModule()
    predictor = LightningPredictor(model)

    representation = repr(predictor)

    assert len(representation) < MAX_REPR_LENGTH
    pattern = re.compile("^LightningPredictor\\((.*)\\)$")
    assert pattern.match(representation)


def get_model():
    return LightningMNISTClassifier(**LightningMNISTModelConfig)

def create_checkpoint(model: pl.LightningModule, dataloader: DataLoader, ckpt_dir: str):
    ckpt_path = f"{ckpt_dir}/checkpoint.ckpt"
    trainer = pl.Trainer(max_epochs=0)
    trainer.fit(model, train_dataloaders=dataloader)
    trainer.save_checkpoint(ckpt_path)
    

@pytest.mark.parametrize("fs", ["s3", "local"])
@pytest.mark.parametrize("use_gpu", [True, False])
@pytest.mark.parametrize("use_preprocessor", [True, False])
@pytest.mark.parametrize("batch_format", ["numpy", "pandas"])
def test_predictor(fs: str, use_preprocessor: bool, use_gpu: bool, batch_format: str):
    batch_size = 10
    datamodule = MNISTDataModule(batch_size=batch_size)
    datamodule.setup()
    dataloader = datamodule.val_dataloader()
    model = LightningMNISTClassifier(**LightningMNISTModelConfig)

    with tempfile.TemporaryDirectory() as tmpdir:
        if fs == "local":
            create_checkpoint(model, dataloader, tmpdir)
            checkpoint = LightningCheckpoint.from_directory(tmpdir)
        else:
            checkpoint_uri = "s3://anyscale-yunxuanx-demo/checkpoint_000008/"
            checkpoint = LightningCheckpoint.from_uri(checkpoint_uri)

        preprocessor = DummyPreprocessor() if use_preprocessor else None
        predictor = LightningPredictor.from_checkpoint(
            checkpoint=checkpoint, 
            model=LightningMNISTClassifier, 
            model_init_config=LightningMNISTModelConfig, 
            use_gpu=use_gpu, 
            preprocessor=preprocessor
        )

        batch = next(iter(dataloader))
        batch = batch[0].numpy()
        if batch_format == "pandas":
            batch = convert_batch_type_to_pandas(batch)

        output = predictor.predict(batch)
        assert len(output["predictions"]) == batch_size
        if preprocessor:
            assert predictor.get_preprocessor().has_preprocessed


@pytest.mark.parametrize("use_gpu", [True, False])
def test_batch_predictor(use_gpu: bool):
    with tempfile.TemporaryDirectory() as tmpdir:
        synthetic_data = convert_batch_type_to_pandas({
            "image": np.random.rand(32, 1, 28, 28).astype(np.float32),
            "label": np.random.randint(0, 10, (32,))
        })
        ds = ray.data.from_pandas(synthetic_data)

        # Create a model checkpoint    
        dataloader = DataLoader(synthetic_data)
        model = LightningMNISTClassifier(**LightningMNISTModelConfig)
        create_checkpoint(model, dataloader, tmpdir)
        checkpoint = LightningCheckpoint.from_directory(tmpdir)

        batch_predictor = BatchPredictor(
            checkpoint=checkpoint, 
            predictor_cls=LightningPredictor, 
            use_gpu=use_gpu, 
            model=LightningMNISTClassifier, 
            model_init_config=LightningMNISTModelConfig
        )

        batch_predictor.predict(
            ds,
            feature_columns=["image"],
            keep_columns=["label"],
            batch_size=8,
            min_scoring_workers=2,
            max_scoring_workers=2,
            num_gpus_per_worker=1 if use_gpu else 0,
        )



if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
    