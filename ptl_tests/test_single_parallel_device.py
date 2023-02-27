from pytorch_lightning import callbacks
from ray.air import session
from ray import train
from ray.tune.syncer import SyncConfig
from ray.air.config import ScalingConfig, RunConfig, CheckpointConfig
from ray.train.torch import TorchTrainer, TorchCheckpoint
import ray
import socket
import pytorch_lightning as pl
from torch.utils.data import DataLoader
from torchvision.datasets import MNIST
from torchvision import transforms
import torch.nn.functional as F
from torch import nn
import torch
import os

from pytorch_lightning.callbacks import ModelCheckpoint
from pytorch_lightning import strategies
os.environ['RAY_ML_DEV'] = "1"

from lightning_fabric.plugins.environments.lightning import LightningEnvironment
from pytorch_lightning import Trainer
from pytorch_lightning.utilities import rank_zero_info, rank_zero_only


class RayModelCheckpoint(ModelCheckpoint):
    def on_train_batch_end(self, trainer: "pl.Trainer", pl_module: "pl.LightningModule", outputs, batch, batch_idx: int) -> None:
        monitor_candidates = self._monitor_candidates(trainer)
        print("on_train_epoch_end[monitor_candidates]: ", monitor_candidates)
        return super().on_train_batch_end(trainer, pl_module, outputs, batch, batch_idx)

class Encoder(nn.Module):
    def __init__(self):
        super().__init__()
        self.l1 = nn.Sequential(nn.Linear(28 * 28, 64), nn.ReLU(), nn.Linear(64, 3))

    def forward(self, x):
        return self.l1(x)


class Decoder(nn.Module):
    def __init__(self):
        super().__init__()
        self.l1 = nn.Sequential(nn.Linear(3, 64), nn.ReLU(), nn.Linear(64, 28 * 28))

    def forward(self, x):
        return self.l1(x)


class LitAutoEncoder(pl.LightningModule):
    def __init__(self, encoder, decoder):
        super().__init__()
        self.encoder = encoder
        self.decoder = decoder
        self.global_steps = 0

    def training_step(self, batch, batch_idx):
        # training_step defines the train loop.
        x, y = batch
        x = x.view(x.size(0), -1)
        z = self.encoder(x)
        x_hat = self.decoder(z)
        loss = F.mse_loss(x_hat, x)
        loss_value = loss.item()

        self.global_steps += 1
        if self.global_steps % 5 == 0:
            self.log_dict({"loss_5": loss_value, "steps": self.global_steps})
        
        if self.global_step % 10 == 0:
            self.log_dict({"loss_10": loss_value, "steps": self.global_steps})
        return loss

    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.parameters(), lr=1e-3)
        return optimizer

    def training_epoch_end(self, outputs) -> None:
        loss = sum(output["loss"] for output in outputs) / len(outputs)
        print("Epoch Loss = ", loss)
        print("weight = ", self.encoder.l1[0].weight)

class RayEnvironment(LightningEnvironment):
    def world_size(self) -> int:
        return session.get_world_size()

    def global_rank(self) -> int:
        return session.get_world_rank()

    def local_rank(self) -> int:
        return session.get_local_rank()

    def node_rank(self) -> int:
        return session.get_node_rank()

    def set_world_size(self, size: int) -> None:
        self._world_size = session.get_world_size()

    def set_global_rank(self, rank: int) -> None:
        self._global_rank = session.get_world_rank()
        rank_zero_only.rank = rank

    def teardown(self):
        pass

def train_loop_per_worker():
    dataset = MNIST(os.getcwd(), download=True, transform=transforms.ToTensor())
    train_loader = DataLoader(dataset, batch_size=100)
    train_loader = train.torch.prepare_data_loader(train_loader)

    # model
    encoder = Encoder()
    decoder = Decoder()
    autoencoder = LitAutoEncoder(encoder, decoder)

    # train model
    device = train.torch.get_device()
    os.environ["LOCAL_RANK"] = "0"
    # visible_device = os.getenv("CUDA_VISIBLE_DEVICES").split(",")
    # visible_device = [int(dev) for dev in visible_device]
    # checkpoint_callback = RayModelCheckpoint(monitor="loss", mode="min", dirpath="/mnt/cluster_storage/ptl_result")
    # trainer = pl.Trainer(max_epochs=5, accelerator="gpu", strategy="ddp", devices=[device.index], callbacks=[checkpoint_callback])
    trainer = pl.Trainer(max_epochs=100, accelerator="gpu", strategy="ddp", devices=[device.index])


    print("Ray Local Rank: ", session.get_local_rank())
    print("Ray World Rank: ", session.get_world_rank())
    print("Ray Local Size: ", session.get_local_world_size())
    print("Ray World Size: ", session.get_world_size())
    print("Ray Node Rank: ", session.get_node_rank())
    print("Ray Train Get device", device.index)
    print("CUDA VISIBLE DEVICES", os.getenv("CUDA_VISIBLE_DEVICES"))
    message = ""
    message += f"\ntorch.cuda.current_device        : {torch.cuda.current_device()}"
    message += f"\ntorch.distributed.get_rank       : {torch.distributed.get_rank()}"
    message += f"\ntorch.cuda.device_count          : {torch.cuda.device_count()}"
    message += f"\ntorch.distributed.get_world_size : {torch.distributed.get_world_size()}"
    message += f"\nsocket.gethostname               : {socket.gethostname()}"
    print(message)

    trainer.fit(model=autoencoder, train_dataloaders=train_loader)


# Scale out model training across 4 GPUs.
# scaling_config = ScalingConfig(num_workers=4, use_gpu=True, resources_per_worker={"CPU": 1, "GPU": 2})
scaling_config = ScalingConfig(num_workers=8, use_gpu=True, resources_per_worker={"CPU": 1, "GPU": 1})


# Save the latest checkpoint
checkpoint_config = CheckpointConfig(num_to_keep=1)


# Set experiment name and checkpoint configs
run_config = RunConfig(
    name="finetune-resnet",
    local_dir="/mnt/cluster_storage/ray_lightning_results",
    sync_config=SyncConfig(),
    checkpoint_config=checkpoint_config
)


trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    scaling_config=scaling_config,
    run_config=run_config,
)


result = trainer.fit()
print(result)
