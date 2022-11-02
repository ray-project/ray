import pytest

import torch
import torch.utils.data

import torchvision
from torchvision import transforms, datasets

import ray
from ray.air.config import ScalingConfig
import ray.train as train
from ray.air import session
from ray.train.mosaic.mosaic_predictor import MosaicPredictor
from ray.train.batch_predictor import BatchPredictor


scaling_config = ScalingConfig(num_workers=2, use_gpu=False)

mean = (0.507, 0.487, 0.441)
std = (0.267, 0.256, 0.276)
cifar10_transforms = transforms.Compose(
    [transforms.ToTensor(), transforms.Normalize(mean, std)]
)

data_directory = "~/data"


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

    result = train_mosaic_cifar10().metrics_dataframe

    # check the max epoch value
    assert result["epoch"][result.index[-1]] == 4

    # check train_iterations
    assert result["_training_iteration"][result.index[-1]] == 5
    assert len(result) == 5

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


def test_checkpoint_model(ray_start_4_cpus):
    from ray.train.mosaic import MosaicTrainer

    model = torchvision.models.resnet18(num_classes=10)

    trainer_init_config = {
        "model": model,
        "max_duration": "5ep",
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()
    checkpoint_dict = result.checkpoint.to_dict()
    assert "state" in checkpoint_dict
    print(checkpoint_dict.keys())
    print(checkpoint_dict["state"].keys())

    loaded_model = result.checkpoint.get_model(model)
    trainer_init_config = {
        "model": loaded_model,
        "max_duration": "2ep",
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result2 = trainer.fit()

    # Accuracy should increase after two additional epochs of training
    acc_key = "metrics/train/Accuracy"
    assert result.metrics[acc_key] <= result2.metrics[acc_key]


class TestResumedTraining:
    """Tests resuming training from a checkpoint. When resuming training, the max
    duration should be properly set.
    """

    def test_resume_from_checkpoint(self, ray_start_4_cpus):
        """
        Resuming training without changing the max duration results in ValueError
        """
        from ray.train.mosaic import MosaicTrainer

        trainer_init_config = {"max_duration": "1ep"}

        trainer = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
        )

        result = trainer.fit()

        # Same `max_duration`` results in `ValueError`
        trainer = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
            resume_from_checkpoint=result.checkpoint,
        )

        with pytest.raises(ValueError):
            trainer.fit().error

        # Train for 1 more epoch, so a total of 2 epochs.
        trainer_init_config["max_duration"] = "2ep"
        trainer = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
            resume_from_checkpoint=result.checkpoint,
        )

        result2 = trainer.fit()
        assert (
            result.metrics_dataframe["iterations_since_restore"].max()
            == result2.metrics_dataframe["iterations_since_restore"].max()
        )

    def test_resume_from_checkpoint_after_early_stopping(self, ray_start_4_cpus):
        # Test Early Stopper Callback
        # Note that composer library epoch starts at 1 when a stopper callback is
        # applied
        from ray.train.mosaic import MosaicTrainer

        from composer.callbacks.threshold_stopper import ThresholdStopper

        # the training should stop after the 2nd epoch
        trainer_init_config = {
            "max_duration": "5ep",
            "should_eval": True,
            "log_keys": ["metrics/my_evaluator/Accuracy"],
            "callbacks": ThresholdStopper("Accuracy", "my_evaluator", 0.0),
        }

        trainer = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
        )

        result = trainer.fit()

        assert list(result.metrics_dataframe["epoch"])[0] == 1
        assert result.metrics["epoch"] == 1

        # Remove callbacks and resume -- finish up to 5 epochs
        trainer_init_config["callbacks"] = []

        trainer = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
            resume_from_checkpoint=result.checkpoint,
        )

        result = trainer.fit()

        assert result.metrics["epoch"] == 5


def test_batch_predict(ray_start_4_cpus):
    """
    Use BatchPredictor to make predictions
    """
    from ray.train.mosaic import MosaicTrainer

    model = torchvision.models.resnet18(num_classes=10)

    trainer_init_config = {
        "model": model,
        "max_duration": "5ep",
        "should_eval": True,
        "log_keys": ["metrics/my_evaluator/Accuracy"],
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()

    # prediction mapping functions
    def convert_logits_to_classes(df):
        best_class = df["predictions"].map(lambda x: x.argmax())
        df["prediction"] = best_class
        return df

    def calculate_prediction_scores(df):
        df["correct"] = df["prediction"] == df["label"]
        return df[["prediction", "label", "correct"]]

    predictor = BatchPredictor.from_checkpoint(
        checkpoint=result.checkpoint,
        predictor_cls=MosaicPredictor,
        model=model,
    )

    # prepare prediction dataset
    data_directory = "~/data"

    test_dataset = torch.utils.data.Subset(
        datasets.CIFAR10(
            data_directory, train=False, download=True, transform=cifar10_transforms
        ),
        list(range(64)),
    )
    test_images = [x.numpy() for x, _ in test_dataset]
    test_input = ray.data.from_items(test_images)

    # make prediction
    outputs = predictor.predict(test_input)
    predictions = outputs.map_batches(convert_logits_to_classes, batch_format="pandas")

    # add label column
    prediction_df = predictions.to_pandas()
    prediction_df["label"] = [y for _, y in test_dataset]
    predictions = ray.data.from_pandas(prediction_df)

    # score prediction
    scores = predictions.map_batches(calculate_prediction_scores)
    score = scores.sum(on="correct") / scores.count()

    assert score == result.metrics["metrics/my_evaluator/Accuracy"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
