import pytest

import torch
import torch.utils.data

import torchvision
from torchvision import transforms, datasets

from ray.air.config import ScalingConfig
import ray.train as train
from ray.air import session


scaling_config = ScalingConfig(num_workers=2, use_gpu=False)


def trainer_init_per_worker(config):
    from torchmetrics.classification.accuracy import Accuracy
    from composer.core.evaluator import Evaluator
    from composer.models.tasks import ComposerClassifier
    import composer.optim

    BATCH_SIZE = 32
    model = ComposerClassifier(
        config.pop("model", torchvision.models.resnet18(num_classes=10))
    )

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

    if config.pop("should_eval", False):
        config["eval_dataloader"] = evaluator

    return composer.trainer.Trainer(
        model=model, train_dataloader=train_dataloader, optimizers=optimizer, **config
    )


trainer_init_per_worker.__test__ = False


def test_mosaic_cifar10(ray_start_4_cpus):
    from ray.train.examples.mosaic_cifar10_example import train_mosaic_cifar10

    result = train_mosaic_cifar10(max_duration="5ep").metrics_dataframe

    # check the max epoch value
    assert result["epoch"][result.index[-1]] == 4

    # check train_iterations
    assert result["_training_iteration"][result.index[-1]] == 5

    # check metrics/train/Accuracy has increased
    acc = list(result["metrics/train/Accuracy"])
    assert acc[-1] > acc[0]


def test_init_errors(ray_start_4_cpus):
    from ray.train.mosaic import MosaicTrainer

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
    with pytest.raises(ValueError, match=r".*dataset shards.*`prepare_dataloader`.*"):
        _ = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
            datasets={"train": [1]},
        )


def test_loggers(ray_start_4_cpus):
    from ray.train.mosaic import MosaicTrainer

    from composer.loggers.logger_destination import LoggerDestination
    from composer.core.state import State
    from composer.loggers import Logger
    from composer.core.callback import Callback

    class DummyLogger(LoggerDestination):
        def fit_start(self, state: State, logger: Logger) -> None:
            raise ValueError("Composer Logger object exists.")

    class DummyCallback(Callback):
        def fit_start(self, state: State, logger: Logger) -> None:
            raise ValueError("Composer Callback object exists.")

    class DummyMonitorCallback(Callback):
        def fit_start(self, state: State, logger: Logger) -> None:
            logger.log_metrics({"dummy_callback": "test"})

    # DummyLogger should not throw an error since it should be removed before `fit` call
    trainer_init_config = {
        "max_duration": "1ep",
        "loggers": DummyLogger(),
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    trainer.fit()

    # DummyCallback should throw an error since it should not have been removed.
    trainer_init_config["callbacks"] = DummyCallback()
    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    with pytest.raises(ValueError) as e:
        trainer.fit()
        assert e == "Composer Callback object exists."

    trainer_init_config["callbacks"] = DummyMonitorCallback()
    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()

    assert "dummy_callback" in result.metrics
    assert result.metrics["dummy_callback"] == "test"


def test_log_count(ray_start_4_cpus):
    from ray.train.mosaic import MosaicTrainer

    trainer_init_config = {
        "max_duration": "1ep",
        "should_eval": False,
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()

    assert len(result.metrics_dataframe) == 1

    trainer_init_config["max_duration"] = "1ba"

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()

    assert len(result.metrics_dataframe) == 1


def test_metrics_key(ray_start_4_cpus):
    from ray.train.mosaic import MosaicTrainer

    """Tests if `log_keys` defined in `trianer_init_config` appears in result
    metrics_dataframe.
    """
    trainer_init_config = {
        "max_duration": "1ep",
        "should_eval": True,
        "log_keys": ["metrics/my_evaluator/Accuracy"],
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()

    # check if the passed in log key exists
    assert "metrics/my_evaluator/Accuracy" in result.metrics_dataframe.columns


def test_monitor_callbacks(ray_start_4_cpus):
    from ray.train.mosaic import MosaicTrainer

    # Test Callbacks involving logging (SpeedMonitor, LRMonitor)
    from composer.callbacks import SpeedMonitor, LRMonitor, GradMonitor

    trainer_init_config = {
        "max_duration": "1ep",
        "should_eval": True,
    }
    trainer_init_config["log_keys"] = [
        "grad_l2_norm/step",
    ]
    trainer_init_config["callbacks"] = [
        SpeedMonitor(window_size=3),
        LRMonitor(),
        GradMonitor(),
    ]

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()

    assert len(result.metrics_dataframe) == 1

    metrics_columns = result.metrics_dataframe.columns
    columns_to_check = [
        "wall_clock/train",
        "wall_clock/val",
        "wall_clock/total",
        "lr-DecoupledSGDW/group0",
        "grad_l2_norm/step",
    ]
    for column in columns_to_check:
        assert column in metrics_columns, column + " is not found"
        assert result.metrics_dataframe[column].isnull().sum() == 0, (
            column + " column has a null value"
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
