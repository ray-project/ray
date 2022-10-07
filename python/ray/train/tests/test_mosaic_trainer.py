import pytest

from pathlib import Path
import os

# torch libraries
import torch
import torch.utils.data

# torchvision libraries
import torchvision
from torchvision import transforms, datasets

# Import Ray libraries
import ray
from ray.air.config import ScalingConfig
import ray.train as train
from ray.air import session

# import ray-mosaic integration libraries
from ray.train.mosaic import MosaicTrainer

# import composer training libraries
from torchmetrics.classification.accuracy import Accuracy
from composer.core.evaluator import Evaluator
from composer.models.tasks import ComposerClassifier
import composer.optim
from composer.loggers import InMemoryLogger

mean = (0.507, 0.487, 0.441)
std = (0.267, 0.256, 0.276)
cifar10_transforms = transforms.Compose(
    [transforms.ToTensor(), transforms.Normalize(mean, std)]
)
# data_directory = "./data" ## TODO : remove the following line
data_directory = "~/Desktop/workspace/data"
train_dataset = datasets.CIFAR10(
    data_directory, train=True, download=True, transform=cifar10_transforms
)
test_dataset = datasets.CIFAR10(
    data_directory, train=False, download=True, transform=cifar10_transforms
)

scaling_config = ScalingConfig(num_workers=2, use_gpu=False)


@pytest.fixture(autouse=True, scope="session")
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def trainer_init_per_worker(**config):
    BATCH_SIZE = 1024
    # prepare the model for distributed training and wrap with ComposerClassifier for
    # Composer Trainer compatibility
    model = config.pop("model", torchvision.models.resnet18(num_classes=10))
    model = ComposerClassifier(ray.train.torch.prepare_model(model))

    # prepare train/test dataset
    train_dataset = config.pop("train_dataset")
    test_dataset = config.pop("test_dataset")

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


def test_mosaic_e2e():
    """Tests if the basic MosaicTrainer with minimum configuration runs and reports correct
    Checkpoint dictionary.
    """
    trainer_init_config = {
        "max_duration": "1ba",
        "train_dataset": train_dataset,
        "test_dataset": test_dataset,
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()

    checkpoint_dict = result.checkpoint.to_dict()

    # check checkpoint keys:
    assert "working_directory" in checkpoint_dict
    assert "in_memory_logger" in checkpoint_dict
    assert "all_checkpoints" in checkpoint_dict
    assert "remote_dir" in checkpoint_dict
    assert len(checkpoint_dict["all_checkpoints"]) > 0


def test_metrics_key():
    """Tests if `log_keys` defined in `trianer_init_config` appears in result
    metrics_dataframe.
    """
    trainer_init_config = {
        "max_duration": "1ba",
        "train_dataset": train_dataset,
        "test_dataset": test_dataset,
        "loggers": [],
        "eval": True,
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


def test_in_memory_logger():
    """Tests whether the in_memory_logger value of training checkpoint is correctly
    reported.
    """
    # trainer with no InMemoryLogger
    trainer_init_config = {
        "max_duration": "1ba",
        "train_dataset": train_dataset,
        "test_dataset": test_dataset,
        "log_keys": ["metrics/my_evaluator/Accuracy"],
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
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
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()
    checkpoint_dict = result.checkpoint.to_dict()

    assert len(checkpoint_dict["in_memory_logger"]) == 2
    assert len(checkpoint_dict["in_memory_logger"][0].data.keys()) > 1
    assert len(checkpoint_dict["in_memory_logger"][1].data.keys()) > 1


class TestCallbacks:
    """Tests composer callbacks. Callbacks can involve logging (SpeedMonitor, LRMonitor,
    etc), or early stopping (EarlyStopper, ThresholdStopper, etc).
    """

    @pytest.fixture(scope="class")
    def trainer_init_config(self):
        return {
            "max_duration": "1ba",
            "train_dataset": train_dataset,
            "test_dataset": test_dataset,
        }

    def test_monitor_callbacks(self, trainer_init_config):
        # Test Callbacks involving logging (SpeedMonitor, LRMonitor)
        from composer.callbacks import SpeedMonitor, LRMonitor, GradMonitor

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

        metrics_columns = result.metrics_dataframe.columns
        assert "wall_clock/train" in metrics_columns
        assert "wall_clock/val" in metrics_columns
        assert "wall_clock/total" in metrics_columns
        assert "lr-DecoupledSGDW/group0" in metrics_columns
        assert "grad_l2_norm/step" in metrics_columns

    def test_early_stopper_(self, trainer_init_config):
        # Test Early Stopper Callback
        from composer.callbacks.early_stopper import EarlyStopper

        # the training should stop after the 2nd epoch
        trainer_init_config["callbacks"] = [
            EarlyStopper("Accuracy", "my_evaluator", min_delta=0.9, patience=0)
        ]
        trainer_init_config["max_duration"] = "5ep"
        trainer_init_config["eval"] = True

        trainer = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
        )

        result = trainer.fit()

        assert "epoch" in result.metrics_dataframe.columns
        assert int(result.metrics_dataframe["epoch"].max(skipna=True)) == 2

    def test_threshold_stopper(self, trainer_init_config):
        # Test Threshold Stopper Callback
        from composer.callbacks.threshold_stopper import ThresholdStopper

        # the training should stop after the 2nd epoch
        trainer_init_config["callbacks"] = [
            ThresholdStopper("Accuracy", "my_evaluator", 0.0)
        ]
        trainer_init_config["max_duration"] = "5ep"
        trainer_init_config["eval"] = True

        trainer = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
        )

        result = trainer.fit()

        assert "epoch" in result.metrics_dataframe.columns
        assert int(result.metrics_dataframe["epoch"].max(skipna=True)) == 1


class TestAlgorithms:
    """Tests different Mosaic's acceleration algorithms. Some algorithms can be passed
    in as trainer constuctor's argument, whereas some can be directly applied to the
    model being passed in to the constructor via model surgery
    """

    from composer.algorithms import (
        CutOut,
        LabelSmoothing,
        CutMix,
        ColOut,
        EMA,
        ChannelsLast,
        GhostBatchNorm,
        GradientClipping,
        LayerFreezing,
        MixUp,
        ProgressiveResizing,
        RandAugment,
        SWA,
        StochasticDepth,
    )

    trainer_arg_algos = [
        # CutOut(num_holes=1, length=0.5),
        # LabelSmoothing(0.1),
        # CutMix(alpha=1.0),
        # ColOut(p_row=0.15, p_col=0.15, batch=True),
        # EMA(half_life="50ba"),
        # ChannelsLast(),
        GhostBatchNorm(ghost_batch_size=32),
        GradientClipping(clipping_type="norm", clipping_threshold=0.1),
        LayerFreezing(freeze_start=0.0, freeze_level=1.0),
        MixUp(alpha=0.2, interpolate_loss=True),
        ProgressiveResizing(mode="resize"),
        RandAugment(severity=9, depth=2, augmentation_set="all"),
        SWA(
            swa_start="1ep",
            swa_end="2ep",
            update_interval="1ba",
        ),
        StochasticDepth(
            target_layer_name="ResNetBottleneck",
            stochastic_method="block",
            drop_rate=0.2,
            drop_distribution="linear",
        ),
    ]

    from composer import functional as cf

    model_surgery_algos = [
        cf.apply_blurpool,
        cf.apply_channels_last,
        cf.apply_factorization,
    ]

    @pytest.fixture()
    def trainer_init_config(self):
        model = torchvision.models.resnet18(num_classes=10)

        return {
            "model": model,
            "max_duration": "1ba",
            "train_dataset": train_dataset,
            "test_dataset": test_dataset,
        }

    @pytest.mark.parametrize("algo", trainer_arg_algos)
    def test_trainer_arg_algorithms(self, trainer_init_config, algo):
        """
        Test passing in algorithms as composer trainer init argument
        """
        # algorithms via Composer Trainer
        trainer_init_config["algorithms"] = [algo]
        trainer = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
        )

        trainer.fit()

    @pytest.mark.parametrize("algo", model_surgery_algos)
    def test_model_surgery_algorithms(self, trainer_init_config, algo):
        """
        Test applying algorithms via model surgery
        """
        # algorithms via model surgery
        algo(trainer_init_config["model"])
        trainer_init_config["algorithms"] = []

        trainer = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
        )

        trainer.fit()


class TestResumedTraining:
    """Tests resuming training from a checkpoint. When resuming training, the max
    duration should be properly set. Checkpoint can be loaded either via
    `resume_from_checkpoint` or `load_path` in `trainer_init_config`. Total training
    time can be reset by configuring `fit_config` as well.
    """

    @pytest.fixture(scope="class")
    def train_first_iteration(
        self,
    ):
        trainer_init_config = {
            "max_duration": "1ep",
            "train_dataset": train_dataset,
            "test_dataset": test_dataset,
            "log_keys": ["metrics/my_evaluator/Accuracy"],
        }

        trainer = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
        )

        result = trainer.fit()

        return result, trainer_init_config.copy()

    def test_same_max_duration_error(self, train_first_iteration):
        """
        Resuming training without changing the max duration results in ValueError
        """
        result, trainer_init_config = train_first_iteration

        # Same `max_duration`` results in `ValueError`
        trainer = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
            resume_from_checkpoint=result.checkpoint,
        )

        with pytest.raises(ValueError):
            trainer.fit().error

    def test_resume_from_checkpoint(self, train_first_iteration):
        """
        Test resuming training from AIR Checkpoint
        """
        result, trainer_init_config = train_first_iteration

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

    def test_resume_from_load_path(self, train_first_iteration):
        """
        Test loading a saved checkpoint and resuming from the saved composer checkpoint
        file
        """
        result, trainer_init_config = train_first_iteration

        # Train for 1 more epoch, now loading using `load_path` instead of resume from
        # checkpoint
        trainer_init_config["max_duration"] = "2ep"

        checkpoint_dict = result.checkpoint.to_dict()
        trainer_init_config["load_path"] = os.path.join(
            checkpoint_dict["working_directory"], checkpoint_dict["all_checkpoints"][0]
        )

        trainer = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
        )

        result2 = trainer.fit()
        assert (
            result.metrics_dataframe["iterations_since_restore"].max()
            == result2.metrics_dataframe["iterations_since_restore"].max()
        )

    def test_resume_reset_time(self, train_first_iteration):
        """
        Resuming training with the same max duration after resetting time should not
        throw errors
        """
        result, trainer_init_config = train_first_iteration

        # Reset time and train for 2 epochs
        trainer_init_config["fit_config"] = {"reset_time": True}

        trainer = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
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
    def bad_trainer_init_per_worker(a, b, c):
        pass

    trainer_init_config = {
        "max_duration": "1ba",
        "train_dataset": train_dataset,
        "test_dataset": test_dataset,
        "log_keys": ["metrics/my_evaluator/Accuracy"],
    }

    with pytest.raises(ValueError):
        _ = MosaicTrainer(
            trainer_init_per_worker=bad_trainer_init_per_worker,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
        )


def test_checkpoint_location():
    """Tests the location of the saved checkpoints. If the `save_folder` is defined in
    the `trainer_init_config`, the checkpoints are saved in the folder; otherwise,
    `ray_tmp` directory is created and the checkpoints are saved there.
    """
    # check if the save path is ray_tmp if the save folder is not provided.
    trainer_init_config = {
        "max_duration": "1ba",
        "train_dataset": train_dataset,
        "test_dataset": test_dataset,
    }

    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()

    checkpoint_file = Path(result.checkpoint.to_dict()["all_checkpoints"][0])
    assert checkpoint_file.parent.name == "ray_tmp"

    trainer_init_config["save_folder"] = "test_save_folder"
    trainer = MosaicTrainer(
        trainer_init_per_worker=trainer_init_per_worker,
        trainer_init_config=trainer_init_config,
        scaling_config=scaling_config,
    )

    result = trainer.fit()

    checkpoint_file = Path(result.checkpoint.to_dict()["all_checkpoints"][0])
    assert checkpoint_file.parent.name == trainer_init_config["save_folder"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
