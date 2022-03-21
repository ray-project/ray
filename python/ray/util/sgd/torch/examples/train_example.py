"""Example code for RaySGD Torch in the documentation.

FIXME: We switched our code formatter from YAPF to Black. Check if we can enable code
formatting on this module and update the paragraph below. See issue #21318.

It ignores yapf because yapf doesn't allow comments right after code blocks,
but we put comments right after code blocks to prevent large white spaces
in the documentation.
"""

# fmt: off
# __torch_train_example__
import argparse
import numpy as np
import torch
import torch.nn as nn

from ray.util.sgd import TorchTrainer
from ray.util.sgd.torch import TrainingOperator


class LinearDataset(torch.utils.data.Dataset):
    """y = a * x + b"""

    def __init__(self, a, b, size=1000):
        x = np.arange(0, 10, 10 / size, dtype=np.float32)
        self.x = torch.from_numpy(x)
        self.y = torch.from_numpy(a * x + b)

    def __getitem__(self, index):
        return self.x[index, None], self.y[index, None]

    def __len__(self):
        return len(self.x)


def model_creator(config):
    """Returns a torch.nn.Module object."""
    return nn.Linear(1, config.get("hidden_size", 1))


def optimizer_creator(model, config):
    """Returns optimizer defined upon the model parameters."""
    return torch.optim.SGD(model.parameters(), lr=config.get("lr", 1e-2))


def scheduler_creator(optimizer, config):
    """Returns a learning rate scheduler wrapping the optimizer.

    You will need to set ``TorchTrainer(scheduler_step_freq="epoch")``
    for the scheduler to be incremented correctly.

    If using a scheduler for validation loss, be sure to call
    ``trainer.update_scheduler(validation_loss)``.
    """
    return torch.optim.lr_scheduler.StepLR(optimizer, step_size=5, gamma=0.9)


def data_creator(config):
    """Returns training dataloader, validation dataloader."""
    train_dataset = LinearDataset(2, 5, size=config.get("data_size", 1000))
    val_dataset = LinearDataset(2, 5, size=config.get("val_size", 400))
    train_loader = torch.utils.data.DataLoader(
        train_dataset,
        batch_size=config.get("batch_size", 32),
    )
    validation_loader = torch.utils.data.DataLoader(
        val_dataset,
        batch_size=config.get("batch_size", 32))
    return train_loader, validation_loader


def train_example(num_workers=1, use_gpu=False):
    CustomTrainingOperator = TrainingOperator.from_creators(
        model_creator=model_creator, optimizer_creator=optimizer_creator,
        data_creator=data_creator, scheduler_creator=scheduler_creator,
        loss_creator=nn.MSELoss)
    trainer1 = TorchTrainer(
        training_operator_cls=CustomTrainingOperator,
        num_workers=num_workers,
        use_gpu=use_gpu,
        config={
            "lr": 1e-2,  # used in optimizer_creator
            "hidden_size": 1,  # used in model_creator
            "batch_size": 4,  # used in data_creator
        },
        backend="gloo",
        scheduler_step_freq="epoch")
    for i in range(5):
        stats = trainer1.train()
        print(stats)

    print(trainer1.validate())

    # If using Ray Client, make sure to force model onto CPU.
    import ray
    m = trainer1.get_model(to_cpu=ray.util.client.ray.is_connected())
    print("trained weight: % .2f, bias: % .2f" % (
        m.weight.item(), m.bias.item()))
    trainer1.shutdown()
    print("success!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address",
        required=False,
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
        "--tune", action="store_true", default=False, help="Tune training")
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.")

    args, _ = parser.parse_known_args()

    import ray
    if args.smoke_test:
        ray.init(num_cpus=2)
    elif args.server_address:
        ray.init(f"ray://{args.server_address}")
    else:
        ray.init(address=args.address)
    train_example(num_workers=args.num_workers, use_gpu=args.use_gpu)
