# fmt: off
"""
This file holds code for a Distributed Pytorch + Tune page in the docs.

FIXME: We switched our code formatter from YAPF to Black. Check if we can enable code
formatting on this module and update the paragraph below. See issue #21318.

It ignores yapf because yapf doesn't allow comments right after code blocks,
but we put comments right after code blocks to prevent large white spaces
in the documentation.
"""

import torch
import torch.nn as nn
from ray.tune.utils import merge_dicts
from torch.optim.lr_scheduler import ReduceLROnPlateau
from torch.utils.data import DataLoader

import ray
from ray import tune
from ray.util.sgd.torch import TorchTrainer, TrainingOperator
from ray.util.sgd.utils import BATCH_SIZE
from ray.util.sgd.torch.examples.train_example import LinearDataset


def model_creator(config):
    return nn.Linear(1, 1)


def optimizer_creator(model, config):
    """Returns optimizer."""
    return torch.optim.SGD(model.parameters(), lr=config.get("lr", 1e-4))


def data_creator(config):
    """Returns training dataloader, validation dataloader."""
    train_dataset = LinearDataset(2, 5)
    val_dataset = LinearDataset(2, 5, size=400)
    train_loader = DataLoader(train_dataset, batch_size=config[BATCH_SIZE])
    validation_loader = DataLoader(val_dataset, batch_size=config[BATCH_SIZE])
    return train_loader, validation_loader


def scheduler_creator(optimizer, config):
    """Returns scheduler. We are using a ReduceLROnPleateau scheduler."""
    scheduler = ReduceLROnPlateau(optimizer, mode="min")
    return scheduler


# __torch_tune_example__
def tune_example(operator_cls, num_workers=1, use_gpu=False):
    TorchTrainable = TorchTrainer.as_trainable(
        training_operator_cls=operator_cls,
        num_workers=num_workers,
        use_gpu=use_gpu,
        config={BATCH_SIZE: 128}
    )

    analysis = tune.run(
        TorchTrainable,
        num_samples=3,
        config={"lr": tune.grid_search([1e-4, 1e-3])},
        stop={"training_iteration": 2},
        verbose=1)

    return analysis.get_best_config(metric="val_loss", mode="min")
# __end_torch_tune_example__


# __torch_tune_manual_lr_example__
def tune_example_manual(operator_cls, num_workers=1, use_gpu=False):
    def step(trainer, info: dict):
        """Define a custom training loop for tune.
         This is needed because we want to manually update our scheduler.
         """
        train_stats = trainer.train(profile=True)
        validation_stats = trainer.validate(profile=True)
        # Manually update our scheduler with the given metric.
        trainer.update_scheduler(metric=validation_stats["val_loss"])
        all_stats = merge_dicts(train_stats, validation_stats)
        return all_stats

    TorchTrainable = TorchTrainer.as_trainable(
        override_tune_step=step,
        training_operator_cls=operator_cls,
        num_workers=num_workers,
        use_gpu=use_gpu,
        scheduler_step_freq="manual",
        config={BATCH_SIZE: 128}
    )

    analysis = tune.run(
        TorchTrainable,
        num_samples=3,
        config={"lr": tune.grid_search([1e-4, 1e-3])},
        stop={"training_iteration": 2},
        verbose=1)

    return analysis.get_best_config(metric="val_loss", mode="min")
# __end_torch_tune_manual_lr_example__


def get_custom_training_operator(lr_reduce_on_plateau=False):
    return TrainingOperator.from_creators(
        model_creator=model_creator, optimizer_creator=optimizer_creator,
        data_creator=data_creator, loss_creator=nn.MSELoss,
        scheduler_creator=scheduler_creator if lr_reduce_on_plateau
        else None)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    parser.add_argument(
        "--address",
        type=str,
        help="the address to use for Ray")
    parser.add_argument(
        "--server-address",
        type=str,
        default=None,
        required=False,
        help="The address of server to connect to if using "
             "Ray Client.")
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=1,
        help="Sets number of workers for training.")
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enables GPU training")
    parser.add_argument(
        "--lr-reduce-on-plateau",
        action="store_true",
        default=False,
        help="If enabled, use a ReduceLROnPlateau scheduler. If not set, "
             "no scheduler is used."
    )

    args, _ = parser.parse_known_args()

    if args.smoke_test:
        ray.init(num_cpus=3)
    elif args.server_address:
        ray.init(f"ray://{args.server_address}")
    else:
        ray.init(address=args.address)

    CustomTrainingOperator = get_custom_training_operator(
        args.lr_reduce_on_plateau)
    if not args.lr_reduce_on_plateau:
        tune_example(CustomTrainingOperator, num_workers=args.num_workers,
                     use_gpu=args.use_gpu)
    else:
        tune_example_manual(CustomTrainingOperator,
                            num_workers=args.num_workers, use_gpu=args.use_gpu)
