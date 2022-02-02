import os
import torch
import torch.nn as nn
import argparse

from filelock import FileLock
from torch.utils.data import DataLoader, Subset
from torchvision.datasets import CIFAR10
import torchvision.transforms as transforms

from tqdm import trange

import ray
from ray.util.sgd.torch import TorchTrainer, TrainingOperator
from ray.util.sgd.torch.resnet import ResNet18
from ray.util.sgd.utils import BATCH_SIZE, override


def initialization_hook():
    # Need this for avoiding a connection restart issue on AWS.
    os.environ["NCCL_SOCKET_IFNAME"] = "^docker0,lo"
    os.environ["NCCL_LL_THRESHOLD"] = "0"

    # set the below if needed
    # print("NCCL DEBUG SET")
    # os.environ["NCCL_DEBUG"] = "INFO"


class CifarTrainingOperator(TrainingOperator):
    @override(TrainingOperator)
    def setup(self, config):
        # Create model.
        model = ResNet18(config)

        # Create optimizer.
        optimizer = torch.optim.SGD(
            model.parameters(),
            lr=config.get("lr", 0.1),
            momentum=config.get("momentum", 0.9),
        )

        # Load in training and validation data.
        transform_train = transforms.Compose(
            [
                transforms.RandomCrop(32, padding=4),
                transforms.RandomHorizontalFlip(),
                transforms.ToTensor(),
                transforms.Normalize(
                    (0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)
                ),
            ]
        )  # meanstd transformation

        transform_test = transforms.Compose(
            [
                transforms.ToTensor(),
                transforms.Normalize(
                    (0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)
                ),
            ]
        )
        with FileLock(".ray.lock"):
            train_dataset = CIFAR10(
                root="~/data", train=True, download=True, transform=transform_train
            )
            validation_dataset = CIFAR10(
                root="~/data", train=False, download=False, transform=transform_test
            )

        if config["test_mode"]:
            train_dataset = Subset(train_dataset, list(range(64)))
            validation_dataset = Subset(validation_dataset, list(range(64)))

        train_loader = DataLoader(
            train_dataset, batch_size=config[BATCH_SIZE], num_workers=2
        )
        validation_loader = DataLoader(
            validation_dataset, batch_size=config[BATCH_SIZE], num_workers=2
        )

        # Create scheduler.
        scheduler = torch.optim.lr_scheduler.MultiStepLR(
            optimizer, milestones=[150, 250, 350], gamma=0.1
        )

        # Create loss.
        criterion = nn.CrossEntropyLoss()

        # Register all components.
        self.model, self.optimizer, self.criterion, self.scheduler = self.register(
            models=model,
            optimizers=optimizer,
            criterion=criterion,
            schedulers=scheduler,
        )
        self.register_data(
            train_loader=train_loader, validation_loader=validation_loader
        )


def train_cifar(
    test_mode=False, num_workers=1, use_gpu=False, num_epochs=5, fp16=False
):
    trainer1 = TorchTrainer(
        training_operator_cls=CifarTrainingOperator,
        initialization_hook=initialization_hook,
        num_workers=num_workers,
        config={
            "lr": 0.1,
            "test_mode": test_mode,  # subset the data
            # this will be split across workers.
            BATCH_SIZE: 128 * num_workers,
        },
        use_gpu=use_gpu,
        scheduler_step_freq="epoch",
        use_fp16=fp16,
        use_tqdm=False,
    )
    pbar = trange(num_epochs, unit="epoch")
    for i in pbar:
        info = {"num_steps": 1} if test_mode else {}
        info["epoch_idx"] = i
        info["num_epochs"] = num_epochs
        # Increase `max_retries` to turn on fault tolerance.
        trainer1.train(max_retries=1, info=info)
        val_stats = trainer1.validate()
        pbar.set_postfix(dict(acc=val_stats["val_accuracy"]))

    print(trainer1.validate())
    trainer1.shutdown()
    print("success!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address",
        required=False,
        type=str,
        help="the address to use for connecting to the Ray cluster",
    )
    parser.add_argument(
        "--server-address",
        type=str,
        default=None,
        required=False,
        help="The address of server to connect to if using " "Ray Client.",
    )
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=1,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--num-epochs", type=int, default=5, help="Number of epochs to train."
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="Enables GPU training"
    )
    parser.add_argument(
        "--fp16",
        action="store_true",
        default=False,
        help="Enables FP16 training. Requires `use-gpu`.",
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.",
    )
    parser.add_argument(
        "--tune", action="store_true", default=False, help="Tune training"
    )

    args, _ = parser.parse_known_args()

    if args.server_address:
        ray.init(f"ray://{args.server_address}")
    else:
        num_cpus = 4 if args.smoke_test else None
        ray.init(address=args.address, num_cpus=num_cpus, log_to_driver=True)

    train_cifar(
        test_mode=args.smoke_test,
        num_workers=args.num_workers,
        use_gpu=args.use_gpu,
        num_epochs=args.num_epochs,
        fp16=args.fp16,
    )
