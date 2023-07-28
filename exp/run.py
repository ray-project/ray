import torch
import torch.nn as nn

import subprocess

import ray
from ray import train
from ray.air import session, Checkpoint
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig
from ray.air.config import RunConfig
from ray.air.config import CheckpointConfig

# If using GPUs, set this to True.
use_gpu = False

# Define NN layers archicture, epochs, and number of workers
num_workers = 2

# Define your train worker loop
def train_loop_per_worker():
    subprocess.run(["python", "train.py"], check=True, capture_output=True)
    # subprocess.run(["python", "-c", "'print(1)'"], check=True, capture_output=True)
    session.report({"msg:": "Finished training!"})

# Define scaling and run configs
scaling_config = ScalingConfig(num_workers=num_workers, use_gpu=use_gpu)
run_config = RunConfig(checkpoint_config=CheckpointConfig(num_to_keep=1))

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    scaling_config=scaling_config
)

result = trainer.fit()

print(result.metrics)