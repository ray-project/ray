import pytest

import torch
import torch.utils.data

import torchvision
from torchvision import transforms, datasets

from torchmetrics.classification.accuracy import Accuracy
from composer.core.evaluator import Evaluator
from composer.models.tasks import ComposerClassifier
import composer.optim
from composer.loggers import InMemoryLogger
from composer.algorithms import LabelSmoothing

import ray
from ray.air.config import ScalingConfig
import ray.train as train
from ray.air import session
from ray.train.mosaic import MosaicTrainer


scaling_config = ScalingConfig(num_workers=2, use_gpu=False)


def trainer_init_per_worker(**config):
    BATCH_SIZE = 32
    # prepare the model for distributed training and wrap with ComposerClassifier for
    # Composer Trainer compatibility
    model = config.pop("model", torchvision.models.resnet18(num_classes=10))
    model = ComposerClassifier(ray.train.torch.prepare_model(model))

    # prepare train/test dataset
    mean = (0.507, 0.487, 0.441)
    std = (0.267, 0.256, 0.276)
    cifar10_transforms = transforms.Compose(
        [transforms.ToTensor(), transforms.Normalize(mean, std)]
    )

    data_directory = "./data"
    train_dataset = torch.utils.data.Subset(
        datasets.CIFAR10(
            data_directory, train=True, download=True, transform=cifar10_transforms
        ),
        list(range(64)),
    )
    test_dataset = torch.utils.data.Subset(
        datasets.CIFAR10(
            data_directory, train=False, download=True, transform=cifar10_transforms
        ),
        list(range(64)),
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

    if config.pop("eval", False):
        config["eval_dataloader"] = evaluator

    return composer.trainer.Trainer(
        model=model, train_dataloader=train_dataloader, optimizers=optimizer, **config
    )


trainer_init_per_worker.__test__ = False


def test_mosaic_e2e(ray_start_4_cpus):
    """Tests if the basic MosaicTrainer with minimum configuration runs and reports correct
    Checkpoint dictionary.
    """
    trainer_init_config = {
        "max_duration": "1ep",
        "loggers": [InMemoryLogger()],
        "algorithms": [LabelSmoothing()],
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    trainer.fit()


def test_fit_config(ray_start_4_cpus):
    trainer_init_config = {
        "max_duration": "2ba",
        "loggers": [InMemoryLogger()],
        "algorithms": [LabelSmoothing()],
        "fit_config": {"duration": "1ba"},
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    trainer.fit()

    # TODO : check the training duration once reporting/checkpoint has been integrated


def test_init_errors(ray_start_4_cpus):
    """Tests errors that may be raised when constructing MosaicTrainer. The error may
    be due to bad `trainer_init_per_worker` function or missing requirements in the
    `trainer_init_config` argument.
    """
    # invalid trainer init function -- no argument
    def bad_trainer_init_per_worker_1():
        pass

    trainer_init_config = {
        "max_duration": "1ba",
    }

    with pytest.raises(ValueError):
        _ = MosaicTrainer(
            trainer_init_per_worker=bad_trainer_init_per_worker_1,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
        )

    # invalid trainer init function -- more than one argument
    def bad_trainer_init_per_worker_1(a, b):
        pass

    with pytest.raises(ValueError):
        _ = MosaicTrainer(
            trainer_init_per_worker=bad_trainer_init_per_worker_1,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
        )

    # datasets is not supported
    with pytest.raises(ValueError) as e:
        _ = MosaicTrainer(
            trainer_init_per_worker=bad_trainer_init_per_worker_1,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
            datasets={"train": [1]},
        )

        error_msg = "MosaicTrainer does not support providing dataset shards to \
                    `trainer_init_per_worker`. Instead of passing in the dataset into \
                    MosaicTrainer, define a dataloader and use `prepare_dataloader` \
                    inside the `trainer_init_per_worker`."
        assert e == error_msg


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
