# flake8: noqa

MOCK = True

# __ft_initial_run_start__
import os
import tempfile
from typing import Dict, Optional

import torch

import ray
from ray import train
from ray.train import Checkpoint
from ray.train.torch import TorchTrainer


def get_datasets() -> Dict[str, ray.data.Dataset]:
    return {"train": ray.data.from_items([{"x": i, "y": 2 * i} for i in range(10)])}


def train_loop_per_worker(config: dict):
    from torchvision.models import resnet18

    model = resnet18()

    # Checkpoint loading
    checkpoint: Optional[Checkpoint] = train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            model_state_dict = torch.load(os.path.join(checkpoint_dir, "model.pt"))
            model.load_state_dict(model_state_dict)

    model = train.torch.prepare_model(model)

    train_ds = train.get_dataset_shard("train")

    for epoch in range(5):
        # Do some training...

        # Checkpoint saving
        with tempfile.TemporaryDirectory() as tmpdir:
            torch.save(model.module.state_dict(), os.path.join(tmpdir, "model.pt"))
            train.report({"epoch": epoch}, checkpoint=Checkpoint.from_directory(tmpdir))


trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    datasets=get_datasets(),
    scaling_config=train.ScalingConfig(num_workers=2),
    run_config=train.RunConfig(
        name="dl_trainer_restore", storage_path=os.path.expanduser("~/ray_results")
    ),
)
result = trainer.fit()
# __ft_initial_run_end__

# __ft_restored_run_start__
from ray.train.torch import TorchTrainer

restored_trainer = TorchTrainer.restore(
    path=os.path.expanduser("~/ray_results/dl_trainer_restore"),
    datasets=get_datasets(),
)
# __ft_restored_run_end__


if not MOCK:
    # __ft_restore_from_cloud_initial_start__
    original_trainer = TorchTrainer(
        # ...
        run_config=train.RunConfig(
            # Configure cloud storage
            storage_path="s3://results-bucket",
            name="dl_trainer_restore",
        ),
    )
    result = trainer.fit()
    # __ft_restore_from_cloud_initial_end__

    # __ft_restore_from_cloud_restored_start__
    restored_trainer = TorchTrainer.restore(
        "s3://results-bucket/dl_trainer_restore",
        datasets=get_datasets(),
    )
    # __ft_restore_from_cloud_restored_end__


# __ft_autoresume_start__
experiment_path = os.path.expanduser("~/ray_results/dl_restore_autoresume")
if TorchTrainer.can_restore(experiment_path):
    trainer = TorchTrainer.restore(experiment_path, datasets=get_datasets())
    result = trainer.fit()
else:
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        datasets=get_datasets(),
        scaling_config=train.ScalingConfig(num_workers=2),
        run_config=train.RunConfig(
            storage_path=os.path.expanduser("~/ray_results"),
            name="dl_restore_autoresume",
        ),
    )
result = trainer.fit()
# __ft_autoresume_end__
