import argparse
import torch
import torch.utils.data

import torchvision
from torchvision import transforms, datasets

from torchmetrics.classification.accuracy import Accuracy


import ray
from ray.air.config import ScalingConfig
import ray.train as train
from ray.air import session


def trainer_init_per_worker(config):
    from composer.core.evaluator import Evaluator
    from composer.models.tasks import ComposerClassifier
    import composer.optim

    BATCH_SIZE = 64
    # prepare the model for distributed training and wrap with ComposerClassifier for
    # Composer Trainer compatibility
    model = ComposerClassifier(torchvision.models.resnet18(num_classes=10))

    # prepare train/test dataset
    mean = (0.507, 0.487, 0.441)
    std = (0.267, 0.256, 0.276)
    cifar10_transforms = transforms.Compose(
        [transforms.ToTensor(), transforms.Normalize(mean, std)]
    )

    data_directory = "~/data"
    train_dataset = torch.utils.data.Subset(
        datasets.CIFAR10(
            data_directory, train=True, download=True, transform=cifar10_transforms
        ),
        list(range(BATCH_SIZE * 10)),
    )
    test_dataset = torch.utils.data.Subset(
        datasets.CIFAR10(
            data_directory, train=False, download=True, transform=cifar10_transforms
        ),
        list(range(BATCH_SIZE * 10)),
    )

    batch_size_per_worker = BATCH_SIZE // session.get_world_size()
    train_dataloader = torch.utils.data.DataLoader(
        train_dataset, batch_size=batch_size_per_worker, shuffle=True
    )
    test_dataloader = torch.utils.data.DataLoader(
        test_dataset, batch_size=batch_size_per_worker, shuffle=True
    )

    train_dataloader = train.torch.prepare_data_loader(train_dataloader)
    test_dataloader = train.torch.prepare_data_loader(test_dataloader)

    evaluator = Evaluator(
        dataloader=test_dataloader, label="my_evaluator", metrics=Accuracy()
    )

    # prepare optimizer
    optimizer = composer.optim.DecoupledSGDW(
        model.parameters(),
        lr=0.05,
        momentum=0.9,
        weight_decay=2.0e-3,
    )

    if config.pop("should_eval", False):
        config["eval_dataloader"] = evaluator

    return composer.trainer.Trainer(
        model=model, train_dataloader=train_dataloader, optimizers=optimizer, **config
    )


def train_mosaic_cifar10(num_workers=2, use_gpu=False, max_duration="5ep"):
    from composer.algorithms import LabelSmoothing
    from ray.train.mosaic import MosaicTrainer

    trainer_init_config = {
        "max_duration": max_duration,
        "algorithms": [LabelSmoothing()],
        "should_eval": False,
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config=trainer_init_config,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
    )
    result = trainer.fit()
    print(f"Results: {result.metrics}")

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=2,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="Enables GPU training"
    )

    args, _ = parser.parse_known_args()

    runtime_env = {"pip": ["mosaicml==0.10.1"]}
    ray.init(address=args.address, runtime_env=runtime_env)
    train_mosaic_cifar10(num_workers=args.num_workers, use_gpu=args.use_gpu)
