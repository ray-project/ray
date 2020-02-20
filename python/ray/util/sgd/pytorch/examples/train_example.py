"""
This file holds code for a Training guide for PytorchSGD in the documentation.

It ignores yapf because yapf doesn't allow comments right after code blocks,
but we put comments right after code blocks to prevent large white spaces
in the documentation.
"""

# yapf: disable
# __torch_train_example__
import argparse
import numpy as np
import torch
import torch.nn as nn

from ray.util.sgd import PyTorchTrainer


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

    You will need to set ``PyTorchTrainer(scheduler_step_freq="epoch")``
    for the scheduler to be incremented correctly.

    If using a scheduler for validation loss, be sure to call
    ``trainer.update_scheduler(validation_loss)``.
    """
    return torch.optim.lr_scheduler.StepLR(optimizer, step_size=5, gamma=0.9)


def data_creator(config):
    """Returns training dataloader, validation dataloader."""
    return LinearDataset(2, 5),  LinearDataset(2, 5, size=400)


def train_example(num_replicas=1, use_gpu=False):
    trainer1 = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.MSELoss,
        scheduler_creator=scheduler_creator,
        num_replicas=num_replicas,
        use_gpu=use_gpu,
        batch_size=num_replicas * 4,
        config={"lr": 1e-2, "hidden_size": 1},
        backend="gloo",
        scheduler_step_freq="epoch")
    for i in range(5):
        stats = trainer1.train()
        print(stats)

    print(trainer1.validate())
    m = trainer1.get_model()
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
        "--num-replicas",
        "-n",
        type=int,
        default=1,
        help="Sets number of replicas for training.")
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enables GPU training")
    parser.add_argument(
        "--tune", action="store_true", default=False, help="Tune training")

    args, _ = parser.parse_known_args()

    import ray

    ray.init(address=args.address)
    train_example(num_replicas=args.num_replicas, use_gpu=args.use_gpu)
