import pytest
from typing import Tuple
import pandas as pd

# torch libraries
import torch
import torch.utils.data

# torchvision libraries
import torchvision
from torchvision import transforms

# Mosaic libraries
from composer.loggers import InMemoryLogger

# Import Ray libraries
import ray
from ray.data.datasource import SimpleTorchDatasource
from ray.air.config import ScalingConfig

# import ray-mosaic integration libraries
from ray.train.mosaic import MosaicTrainer

# import composer training libraries
from torchmetrics.classification.accuracy import Accuracy
from composer.core.evaluator import Evaluator
from composer.models.tasks import ComposerClassifier
import composer.optim


BATCH_SIZE = 1024


@pytest.fixture(autouse=True, scope="session")
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture(autouse=True, scope="module")
def prepare_dataset():
    transform = transforms.Compose(
        [transforms.ToTensor(), transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))]
    )

    def train_dataset_factory():
        return torchvision.datasets.CIFAR10(
            root="./data", download=True, train=True, transform=transform
        )

    def test_dataset_factory():
        return torchvision.datasets.CIFAR10(
            root="./data", download=True, train=False, transform=transform
        )

    train_dataset_raw: ray.data.Dataset = ray.data.read_datasource(
        SimpleTorchDatasource(), dataset_factory=train_dataset_factory
    )
    test_dataset_raw: ray.data.Dataset = ray.data.read_datasource(
        SimpleTorchDatasource(), dataset_factory=test_dataset_factory
    )

    # Map into pandas
    def convert_batch_to_pandas(batch: Tuple[torch.Tensor, int]) -> pd.DataFrame:
        images = [image.numpy() for image, _ in batch]
        labels = [label for _, label in batch]
        return pd.DataFrame({"image": images, "label": labels}).head(10)

    global train_dataset
    train_dataset = train_dataset_raw.map_batches(convert_batch_to_pandas)
    global test_dataset
    test_dataset = test_dataset_raw.map_batches(convert_batch_to_pandas)


scaling_config = ScalingConfig(num_workers=2, use_gpu=False)


def trainer_init_per_worker(train_dataset, eval_dataset=None, **config):
    model = config.pop("model", torchvision.models.resnet18(num_classes=10))

    # prepare the model for distributed training and wrap with ComposerClassifier for
    # Composer Trainer compatibility
    model = ComposerClassifier(ray.train.torch.prepare_model(model))

    optimizer = composer.optim.DecoupledSGDW(
        model.parameters(),
        lr=0.05,
        momentum=0.9,
        weight_decay=2.0e-3,
    )

    lr_scheduler = composer.optim.LinearWithWarmupScheduler(
        t_warmup="1ep",
        alpha_i=1.0,
        alpha_f=1.0,
    )

    evaluator = Evaluator(
        dataloader=eval_dataset, label="my_evaluator", metrics=Accuracy()
    )

    return composer.trainer.Trainer(
        model=model,
        train_dataloader=train_dataset,
        eval_dataloader=evaluator,
        optimizers=optimizer,
        schedulers=lr_scheduler,
        **config
    )


trainer_init_per_worker.__test__ = False


def test_mosaic_e2e():
    """Tests if the basic MosaicTrainer with minimum configuration runs and reports correct
    Checkpoint dictionary.
    """
    trainer_init_config = {
        "batch_size": BATCH_SIZE,
        "max_duration": "2ep",
        "labels": ["image", "label"],
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        datasets={"train": train_dataset, "evaluation": test_dataset},
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()

    checkpoint_dict = result.checkpoint.to_dict()

    # check checkpoint keys:
    assert "last_checkpoint" in checkpoint_dict
    assert "in_memory_logger" in checkpoint_dict
    assert "all_checkpoints" in checkpoint_dict
    assert len(checkpoint_dict["last_checkpoint"]) > 0


def test_metrics_key():
    """Tests if `log_keys` defined in `trianer_init_config` appears in result
    metrics_dataframe.
    """
    trainer_init_config = {
        "batch_size": BATCH_SIZE,
        "max_duration": "2ep",
        "labels": ["image", "label"],
        "loggers": [],
        "log_keys": ["metrics/my_evaluator/Accuracy"],
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        datasets={"train": train_dataset, "evaluation": test_dataset},
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()

    # check
    assert "metrics/my_evaluator/Accuracy" in result.metrics_dataframe.columns


def test_in_memory_logger():
    """Tests whether the in_memory_logger value of training checkpoint is correctly
    reported.
    """
    # trainer with no InMemoryLogger
    trainer_init_config = {
        "batch_size": BATCH_SIZE,
        "max_duration": "1ep",
        "labels": ["image", "label"],
        "loggers": [],
        "log_keys": ["metrics/my_evaluator/Accuracy"],
        "algorithms": [],
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        datasets={"train": train_dataset, "evaluation": test_dataset},
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()
    checkpoint_dict = result.checkpoint.to_dict()

    assert len(checkpoint_dict["in_memory_logger"]) == 0

    # trainer with one InMemoryLogger
    trainer_init_config["loggers"] = [InMemoryLogger()]
    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        datasets={"train": train_dataset, "evaluation": test_dataset},
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()
    checkpoint_dict = result.checkpoint.to_dict()

    assert len(checkpoint_dict["in_memory_logger"]) == 1
    assert len(checkpoint_dict["in_memory_logger"][0].data.keys()) > 1

    # trainer with two InMemoryLoggers
    trainer_init_config["loggers"] = [InMemoryLogger(), InMemoryLogger()]
    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        datasets={"train": train_dataset, "evaluation": test_dataset},
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()
    checkpoint_dict = result.checkpoint.to_dict()

    assert len(checkpoint_dict["in_memory_logger"]) == 2
    assert len(checkpoint_dict["in_memory_logger"][0].data.keys()) > 1
    assert len(checkpoint_dict["in_memory_logger"][1].data.keys()) > 1


def test_callback():
    """Tests composer callbacks. Callbacks can involve logging (SpeedMonitor, LRMonitor,
    etc), or early stopping (EarlyStopper, ThresholdStopper, etc).
    """
    # Test Callbacks involving logging (SpeedMonitor, LRMonitor)
    from composer.callbacks import SpeedMonitor, LRMonitor

    trainer_init_config = {
        "batch_size": BATCH_SIZE,
        "max_duration": "1ep",
        "labels": ["image", "label"],
        "loggers": [],
        "log_keys": ["metrics/my_evaluator/Accuracy"],
        "callbacks": [SpeedMonitor(window_size=3), LRMonitor()],
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        datasets={"train": train_dataset, "evaluation": test_dataset},
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()

    assert "wall_clock/train" in result.metrics_dataframe.columns
    assert "wall_clock/val" in result.metrics_dataframe.columns
    assert "wall_clock/total" in result.metrics_dataframe.columns
    assert "lr-DecoupledSGDW/group0" in result.metrics_dataframe.columns

    # Test Early Stopper Callback
    from composer.callbacks.early_stopper import EarlyStopper

    # the training should stop after the 2nd epoch
    trainer_init_config["callbacks"] = [
        EarlyStopper("Accuracy", "my_evaluator", min_delta=0.9, patience=0)
    ]
    trainer_init_config["max_duration"] = "5ep"

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        datasets={"train": train_dataset, "evaluation": test_dataset},
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()

    assert "epoch" in result.metrics_dataframe.columns
    assert int(result.metrics_dataframe["epoch"].max(skipna=True)) == 2


def test_algorithm():
    """Tests different Mosaic's acceleration algorithms. Some algorithms can be passed
    in as trainer constuctor's argument, whereas some can be directly applied to the
    model being passed in to the constructor via model surgery
    """
    # algorithms via Composer Trainer
    from composer.algorithms import CutOut, LabelSmoothing

    trainer_init_config = {
        "batch_size": BATCH_SIZE,
        "max_duration": "1ep",
        "labels": ["image", "label"],
        "loggers": [],
        "log_keys": ["metrics/my_evaluator/Accuracy"],
        "algorithms": [CutOut(num_holes=1, length=0.5), LabelSmoothing(0.1)],
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        datasets={"train": train_dataset, "evaluation": test_dataset},
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    trainer.fit()

    # algorithms via model surgery
    from composer import functional as cf

    model = torchvision.models.resnet18(num_classes=10)
    cf.apply_blurpool(model)
    trainer_init_config["model"] = model
    trainer_init_config["algorithms"] = []

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        datasets={"train": train_dataset, "evaluation": test_dataset},
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    trainer.fit()


def test_resumed_training():
    """Tests resuming training from a checkpoint. When resuming training, the max
    duration should be properly set. Checkpoint can be loaded either via
    `resume_from_checkpoint` or `load_path` in `trainer_init_config`. Total training
    time can be reset by configuring `fit_config` as well.
    """
    # test if the resumed training works -- need to check the error for max duration.
    trainer_init_config = {
        "batch_size": BATCH_SIZE,
        "max_duration": "1ep",
        "labels": ["image", "label"],
        "log_keys": ["metrics/my_evaluator/Accuracy"],
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        datasets={"train": train_dataset, "evaluation": test_dataset},
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()

    # Same `max_duration`` results in `ValueError`
    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        datasets={"train": train_dataset, "evaluation": test_dataset},
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
        datasets={"train": train_dataset, "evaluation": test_dataset},
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
        resume_from_checkpoint=result.checkpoint,
    )

    result2 = trainer.fit()
    assert (
        result.metrics_dataframe["iterations_since_restore"].max()
        == result2.metrics_dataframe["iterations_since_restore"].max()
    )

    # Train for 1 more epoch, now loading using `load_path` instead of resume from
    # checkpoint
    trainer_init_config["load_path"] = str(
        result.checkpoint.to_dict()["last_checkpoint"][0]
    )
    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        datasets={"train": train_dataset, "evaluation": test_dataset},
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result2 = trainer.fit()
    assert (
        result.metrics_dataframe["iterations_since_restore"].max()
        == result2.metrics_dataframe["iterations_since_restore"].max()
    )

    # Reset time and train for 2 epochs
    trainer_init_config["fit_config"] = {"reset_time": True}
    trainer_init_config["max_duration"] = "2ep"
    trainer_init_config.pop("load_path")

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        datasets={"train": train_dataset, "evaluation": test_dataset},
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
        resume_from_checkpoint=result.checkpoint,
    )

    result2 = trainer.fit()
    assert (
        result.metrics_dataframe["iterations_since_restore"].max()
        < result2.metrics_dataframe["iterations_since_restore"].max()
    )


def test_init_errors():
    """Tests errors that may be raised when constructing MosaicTrainer. The error may
    be due to bad `trainer_init_per_worker` function or missing requirements in the
    `trainer_init_config` argument.
    """
    # invalid trainer init function
    def bad_trainer_init_per_worker():
        pass

    trainer_init_config = {
        "batch_size": BATCH_SIZE,
        "max_duration": "1ep",
        "labels": ["image", "label"],
        "log_keys": ["metrics/my_evaluator/Accuracy"],
    }

    with pytest.raises(ValueError):
        _ = MosaicTrainer(
            trainer_init_per_worker=bad_trainer_init_per_worker,
            datasets={"train": train_dataset, "evaluation": test_dataset},
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
        )

    # Missing `batch_size` key
    bad_trainer_init_config = trainer_init_config.copy()
    bad_trainer_init_config.pop("batch_size")

    with pytest.raises(KeyError) as error_msg:
        _ = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            datasets={"train": train_dataset, "evaluation": test_dataset},
            trainer_init_config=bad_trainer_init_config,
            scaling_config=scaling_config,
        )
        assert (
            error_msg
            == 'batch size for the training dataset (key: "batch_size") '
            + "should be provided in `trainer_init_config`"
        )

    # Missing `labels` key
    bad_trainer_init_config = trainer_init_config.copy()
    bad_trainer_init_config.pop("labels")

    with pytest.raises(KeyError) as error_msg:
        _ = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            datasets={"train": train_dataset, "evaluation": test_dataset},
            trainer_init_config=bad_trainer_init_config,
            scaling_config=scaling_config,
        )
        assert (
            error_msg
            == 'labels for the columns to include in batch (key: "labels" '
            + "should be provided in `trainer_init_config`"
        )

    # Missing `batch_size` and `labels` keys
    bad_trainer_init_config = trainer_init_config.copy()
    bad_trainer_init_config.pop("batch_size")

    with pytest.raises(KeyError) as error_msg:
        _ = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            datasets={"train": train_dataset, "evaluation": test_dataset},
            trainer_init_config=bad_trainer_init_config,
            scaling_config=scaling_config,
        )
        assert (
            error_msg
            == 'batch size for the training dataset (key: "batch_size") '
            + 'and labels for the columns to include in batch (key: "labels" '
            + "should be provided in `trainer_init_config`"
        )


def test_checkpoint_location():
    """Tests the location of the saved checkpoints. If the `save_folder` is defined in
    the `trainer_init_config`, the checkpoints are saved in the folder; otherwise,
    `ray_tmp` directory is created and the checkpoints are saved there.
    """
    # check if the save path is ray_tmp if the save folder is not provided.
    trainer_init_config = {
        "batch_size": BATCH_SIZE,
        "max_duration": "1ep",
        "labels": ["image", "label"],
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        datasets={"train": train_dataset, "evaluation": test_dataset},
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()

    checkpoint_file = result.checkpoint.to_dict()["last_checkpoint"][0]
    assert checkpoint_file.parent.name == "ray_tmp"

    trainer_init_config["save_folder"] = "test_save_folder"
    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        datasets={"train": train_dataset, "evaluation": test_dataset},
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()

    checkpoint_file = result.checkpoint.to_dict()["last_checkpoint"][0]
    assert checkpoint_file.parent.name == trainer_init_config["save_folder"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
