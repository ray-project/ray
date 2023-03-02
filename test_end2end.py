import os
os.environ['RAY_ML_DEV'] = "1"

import pytorch_lightning as pl
from ray.train.lightning import LightningTrainer
from torchvision.datasets import MNIST
from torchvision import transforms
from torch.utils.data import DataLoader, random_split
import torch
from torch import nn
import torch.nn.functional as F

from ray.tune.syncer import SyncConfig
from ray.air.config import CheckpointConfig, ScalingConfig, RunConfig
import ray.train as train
from ptl_tests.utils import LitAutoEncoder, LightningMNISTClassifier, MNISTDataModule
from torchmetrics import Accuracy

LightningMNISTModelConfig = {
    "config": {
        "layer_1": 32,
        "layer_2": 64,
        "lr": 1e-4,
    }
}

lightning_trainer_config = {
    "max_epochs": 10, 
    "accelerator": "gpu",
    "strategy": "ddp"
}

model_checkpoint_config = {
    "monitor": "ptl/val_accuracy",
    "save_top_k": 3,
    "mode": "max"
}

scaling_config = ScalingConfig(num_workers=8, use_gpu=True, resources_per_worker={"CPU": 1, "GPU": 1})

air_checkpoint_config = CheckpointConfig(num_to_keep=3, checkpoint_score_attribute="ptl/val_accuracy", checkpoint_score_order="max")

run_config = RunConfig(
    name="ptl-e2e-classifier",
    local_dir="/mnt/cluster_storage/ray_lightning_results",
    sync_config=SyncConfig(syncer=None),
    checkpoint_config=air_checkpoint_config
)

trainer = LightningTrainer(
    lightning_module=LightningMNISTClassifier,
    lightning_module_config=LightningMNISTModelConfig,
    lightning_trainer_config=lightning_trainer_config,
    ddp_strategy_config={},
    model_checkpoint_config=model_checkpoint_config,
    scaling_config=scaling_config,
    run_config=run_config,
    datamodule=MNISTDataModule()
)

trainer.fit()