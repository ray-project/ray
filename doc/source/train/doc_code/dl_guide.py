# flake8: noqa

MOCK = True

# __ft_initial_run_start__
from typing import Dict, Optional

import ray
from ray import train
from ray.train.torch import LegacyTorchCheckpoint, TorchTrainer


def get_datasets() -> Dict[str, ray.data.Dataset]:
    return {"train": ray.data.from_items([{"x": i, "y": 2 * i} for i in range(10)])}


def train_loop_per_worker(config: dict):
    from torchvision.models import resnet18

    # Checkpoint loading
    checkpoint: Optional[LegacyTorchCheckpoint] = train.get_checkpoint()
    model = checkpoint.get_model() if checkpoint else resnet18()
    ray.train.torch.prepare_model(model)

    train_ds = train.get_dataset_shard("train")

    for epoch in range(5):
        # Do some training...

        # Checkpoint saving
        train.report(
            {"epoch": epoch},
            checkpoint=LegacyTorchCheckpoint.from_model(model),
        )


trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    datasets=get_datasets(),
    scaling_config=train.ScalingConfig(num_workers=2),
    run_config=train.RunConfig(
        storage_path="~/ray_results",
        name="dl_trainer_restore",
    ),
)
result = trainer.fit()
# __ft_initial_run_end__

# __ft_restored_run_start__
from ray.train.torch import TorchTrainer

restored_trainer = TorchTrainer.restore(
    path="~/ray_results/dl_trainer_restore",
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
if TorchTrainer.can_restore("~/ray_results/dl_restore_autoresume"):
    trainer = TorchTrainer.restore(
        "~/ray_results/dl_restore_autoresume",
        datasets=get_datasets(),
    )
    result = trainer.fit()
else:
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        datasets=get_datasets(),
        scaling_config=train.ScalingConfig(num_workers=2),
        run_config=train.RunConfig(
            storage_path="~/ray_results", name="dl_restore_autoresume"
        ),
    )
result = trainer.fit()
# __ft_autoresume_end__
