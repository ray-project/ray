import os

import torch
import pytest
import ray
from pl_bolts.datamodules.mnist_datamodule import MNISTDataModule
from ray.util.sgd.tests.test_ptl import PTL_Module
from ray.tune.examples.mnist_ptl_mini import LightningMNISTClassifier
from ray.util.lightning_accelerators import HorovodRayAccelerator
import pytorch_lightning as pl

try:
    import horovod  # noqa: F401
    from horovod.common.util import nccl_built
except ImportError:
    HOROVOD_AVAILABLE = False
else:
    HOROVOD_AVAILABLE = True


def _nccl_available():
    if not HOROVOD_AVAILABLE:
        return False
    try:
        return nccl_built()
    except AttributeError:
        return False


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    ray.shutdown()


@pytest.fixture
def ray_start_2_gpus():
    address_info = ray.init(num_cpus=2, num_gpus=2)
    yield address_info
    ray.shutdown()
    # This env var is set by Pytorch Lightning.
    # Make sure to reset it after each test.
    # TODO: Upstream to PTL to not set this env var if using Ray.
    del os.environ["CUDA_VISIBLE_DEVICES"]


@pytest.fixture
def seed():
    pl.seed_everything(0)


def get_model(lr=1e-2, hidden_size=1, data_size=10, val_size=10, batch_size=2):
    config = {
        "lr": lr,
        "hidden_size": hidden_size,
        "data_size": data_size,
        "val_size": val_size,
        "batch_size": batch_size
    }
    return PTL_Module(config)


def get_trainer(dir,
                num_slots=2,
                use_gpu=False,
                max_epochs=1,
                limit_train_batches=10,
                limit_val_batches=10,
                progress_bar_refresh_rate=0):
    accelerator = HorovodRayAccelerator(num_slots=num_slots, use_gpu=use_gpu)
    trainer = pl.Trainer(
        default_root_dir=dir,
        gpus=1 if use_gpu else 0,
        max_epochs=max_epochs,
        limit_train_batches=limit_train_batches,
        limit_val_batches=limit_val_batches,
        progress_bar_refresh_rate=progress_bar_refresh_rate,
        checkpoint_callback=True,
        accelerator=accelerator)
    return trainer


def train_test(trainer, model):
    initial_values = torch.tensor(
        [torch.sum(torch.abs(x)) for x in model.parameters()])
    result = trainer.fit(model)
    post_train_values = torch.tensor(
        [torch.sum(torch.abs(x)) for x in model.parameters()])
    assert result == 1, "trainer failed"
    # Check that the model is actually changed post-training.
    assert torch.norm(initial_values - post_train_values) > 0.1


@pytest.mark.parametrize("num_slots", [1, 2])
def test_train(tmpdir, ray_start_2_cpus, seed, num_slots):
    model = get_model()

    trainer = get_trainer(tmpdir, num_slots=num_slots)
    train_test(trainer, model)


def load_test(trainer, model):
    trainer.fit(model)
    trained_model = PTL_Module.load_from_checkpoint(
        trainer.checkpoint_callback.best_model_path, config=model.config)
    assert trained_model is not None, "loading model failed"


@pytest.mark.parametrize("num_slots", [1, 2])
def test_load(tmpdir, ray_start_2_cpus, seed, num_slots):
    model = get_model()
    trainer = get_trainer(tmpdir, num_slots=num_slots)
    load_test(trainer, model)


def predict_test(trainer, model, dm):
    trainer.fit(model, dm)
    test_loader = dm.test_dataloader()
    acc = pl.metrics.Accuracy()
    for batch in test_loader:
        x, y = batch
        with torch.no_grad():
            y_hat = model(x)
        y_hat = y_hat.cpu()
        acc.update(y_hat, y)
    average_acc = acc.compute()
    assert average_acc >= 0.5, f"This model is expected to get > {0.5} in " \
                               f"test set (it got {average_acc})"


@pytest.mark.parametrize("num_slots", [1, 2])
def test_predict(tmpdir, ray_start_2_cpus, seed, num_slots):
    config = {
        "layer_1": 32,
        "layer_2": 32,
        "lr": 1e-2,
        "batch_size": 32,
    }
    model = LightningMNISTClassifier(config, tmpdir)
    dm = MNISTDataModule(
        data_dir=tmpdir, num_workers=1, batch_size=config["batch_size"])
    trainer = get_trainer(
        tmpdir, limit_train_batches=10, max_epochs=1, num_slots=num_slots)
    predict_test(trainer, model, dm)


@pytest.mark.skipif(
    not _nccl_available(), reason="test requires Horovod with NCCL support")
@pytest.mark.skipif(
    torch.cuda.device_count() < 2, reason="test requires multi-GPU machine")
@pytest.mark.parametrize("num_slots", [1, 2])
def test_train_gpu(tmpdir, ray_start_2_gpus, seed, num_slots):
    model = get_model()
    trainer = get_trainer(tmpdir, num_slots=num_slots, use_gpu=True)
    train_test(trainer, model)


@pytest.mark.skipif(
    not _nccl_available(), reason="test requires Horovod with NCCL support")
@pytest.mark.skipif(
    torch.cuda.device_count() < 2, reason="test requires multi-GPU machine")
@pytest.mark.parametrize("num_slots", [1, 2])
def test_load_gpu(tmpdir, ray_start_2_gpus, seed, num_slots):
    model = get_model()
    trainer = get_trainer(tmpdir, num_slots=num_slots, use_gpu=True)
    load_test(trainer, model)


@pytest.mark.skipif(
    not _nccl_available(), reason="test requires Horovod with NCCL support")
@pytest.mark.skipif(
    torch.cuda.device_count() < 2, reason="test requires multi-GPU machine")
@pytest.mark.parametrize("num_slots", [1, 2])
def test_predict_gpu(tmpdir, ray_start_2_gpus, seed, num_slots):
    config = {
        "layer_1": 32,
        "layer_2": 32,
        "lr": 1e-2,
        "batch_size": 32,
    }
    model = LightningMNISTClassifier(config, tmpdir)
    dm = MNISTDataModule(
        data_dir=tmpdir, num_workers=1, batch_size=config["batch_size"])
    trainer = get_trainer(
        tmpdir,
        limit_train_batches=10,
        max_epochs=1,
        num_slots=num_slots,
        use_gpu=True)
    predict_test(trainer, model, dm)
