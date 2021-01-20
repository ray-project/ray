import os
import numpy as np
import torch
import pytest
import ray
from pl_bolts.datamodules import MNISTDataModule
from ray.tune.examples.mnist_ptl_mini import LightningMNISTClassifier
from ray.util.lightning_accelerators import HorovodRayAccelerator
import pytorch_lightning as pl

path_here = os.path.abspath(os.path.dirname(__file__))
path_root = os.path.abspath(os.path.join(path_here, '..', '..'))

try:
    import horovod
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
    try:
        yield address_info
    finally:
        ray.shutdown()

@pytest.fixture
def seed():
    pl.seed_everything(0)

class LinearDataset(torch.utils.data.Dataset):
    """y = a * x + b"""

    def __init__(self, a, b, x=None, size=1000):
        if x is None:
            x = np.arange(0, 10, 10 / size, dtype=np.float32)
        self.x = torch.from_numpy(x)
        self.y = torch.from_numpy(a * x + b)

    def __getitem__(self, index):
        return self.x[index, None], self.y[index, None]

    def __len__(self):
        return len(self.x)

class PTL_Module(pl.LightningModule):
    def __init__(self, lr=1e-2, hidden_size=1, data_size=10, val_size=10,
                 batch_size=2):
        super().__init__()
        self.save_hyperparameters()
        self.lr = lr
        self.data_size=data_size
        self.val_size=val_size
        self.hidden_size=hidden_size
        self.batch_size=batch_size
        self.layer = torch.nn.Linear(1, hidden_size)

    def forward(self, x):
        return self.layer.forward(x)

    def configure_optimizers(self):
        optimizer = torch.optim.SGD(self.parameters(), lr=self.lr)
        return [optimizer]

    def training_step(self, batch, batch_idx):
        x, y = batch
        #print(x, y)
        output = self(x)
        #print(output)
        loss = self.loss(output, y)
        return loss

    def validation_step(self, batch, batch_idx):
        x, y = batch
        output = self(x)
        loss = self.loss(output, y)
        _, predicted = torch.max(output.data, 1)
        num_correct = (predicted == y).sum().item()
        num_samples = y.size(0)
        return {"val_loss": loss.item(), "val_acc": num_correct / num_samples}

    def setup(self, stage):
        train_dataset = LinearDataset(2, 5, size=self.data_size)
        val_dataset = LinearDataset(2, 5, size=self.val_size)
        self.train_loader = torch.utils.data.DataLoader(
            train_dataset,
            batch_size=self.batch_size,
        )
        self.val_loader = torch.utils.data.DataLoader(
            val_dataset,
            batch_size=self.batch_size)
        self.loss = torch.nn.MSELoss()

    def train_dataloader(self):
        return self.train_loader

    def val_dataloader(self):
        return self.val_loader

    def test_dataloader(self):
        #inputs = np.random.randint(0, 10, 10).astype(np.float32)
        inputs = np.arange(0, 10, dtype=np.float32)
        test_dataset = LinearDataset(2, 5, x=inputs)
        return torch.utils.data.DataLoader(test_dataset, batch_size=2)


def get_trainer(dir, num_processes=2, gpus=0, max_epochs=1,
                limit_train_batches=10,
                limit_val_batches=10, progress_bar_refresh_rate=0):
    accelerator = HorovodRayAccelerator()
    trainer = pl.Trainer(
        default_root_dir=dir,
        num_processes=num_processes,
        gpus=gpus,
        max_epochs=max_epochs,
        limit_train_batches=limit_train_batches,
        limit_val_batches=limit_val_batches,
        progress_bar_refresh_rate=progress_bar_refresh_rate,
        checkpoint_callback=True,
        accelerator=accelerator
    )
    assert False, trainer.distributed_backend
    return trainer

def train_test(trainer, model):
    initial_values = initial_values = torch.tensor(
        [torch.sum(torch.abs(x)) for x in model.parameters()])
    result = trainer.fit(model)
    post_train_values = torch.tensor(
        [torch.sum(torch.abs(x)) for x in model.parameters()])
    assert result == 1, 'trainer failed'
    # Check that the model is actually changed post-training.
    assert torch.norm(initial_values - post_train_values) > 0.1

@pytest.mark.parametrize("num_processes", [1, 2])
def test_train(tmpdir, ray_start_2_cpus, seed, num_processes):
    model = PTL_Module()

    trainer = get_trainer(tmpdir, num_processes=num_processes)
    train_test(trainer, model)

@pytest.mark.parametrize("num_processes", [1, 2])
def test_load(tmpdir, ray_start_2_cpus, seed, num_processes):
    model = PTL_Module()
    trainer = get_trainer(tmpdir, num_processes=num_processes)
    trainer.fit(model)
    trained_model = PTL_Module.load_from_checkpoint(
        trainer.checkpoint_callback.best_model_path)
    assert trained_model is not None, 'loading model failed'

@pytest.mark.parametrize("num_processes", [1, 2])
def test_predict(tmpdir, ray_start_2_cpus, seed, num_processes):
    config = {
        "layer_1": 32,
        "layer_2": 32,
        "lr": 1e-2,
        "batch_size": 32,
    }
    model = LightningMNISTClassifier(config, tmpdir)
    dm = MNISTDataModule(
        data_dir=tmpdir, num_workers=1, batch_size=config["batch_size"])
        #seed=0)
    trainer = get_trainer(tmpdir, limit_train_batches=10, max_epochs=1,
                          progress_bar_refresh_rate=1, num_processes=num_processes)
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


# @mock.patch('pytorch_lightning.accelerators.horovod_ray_accelerator.get_executable_cls')
# @pytest.mark.skipif(platform.system() == "Windows", reason="Horovod is not supported on Windows")
# @pytest.mark.skipif(not _nccl_available(), reason="test requires Horovod with NCCL support")
# @pytest.mark.skipif(torch.cuda.device_count() < 2, reason="test requires multi-GPU machine")
# def test_horovod_multi_gpu(mock_executable_cls, tmpdir, ray_start_2_gpus):
#     """Test Horovod with multi-GPU support."""
#     mock_executable_cls.return_value = create_mock_executable()
#     trainer_options = dict(
#         default_root_dir=tmpdir,
#         max_epochs=1,
#         limit_train_batches=10,
#         limit_val_batches=10,
#         gpus=2,
#         distributed_backend='horovod_ray',
#         progress_bar_refresh_rate=0
#     )
#
#     model = EvalModelTemplate()
#     tpipes.run_model_test(trainer_options, model)
