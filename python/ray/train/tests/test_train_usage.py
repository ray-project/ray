import pytest
import torch

import ray
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer


@pytest.fixture
def shutdown_only():
    yield None
    ray.shutdown()


def run_torch():
    from torch.utils.data import DataLoader, TensorDataset

    from ray.train.torch import (
        get_device,
        get_devices,
        prepare_data_loader,
        prepare_model,
    )

    def train_func():
        # Create dummy model and data loader
        model = torch.nn.Linear(10, 10)
        inputs, targets = torch.randn(128, 10), torch.randn(128, 1)
        dataloader = DataLoader(TensorDataset(inputs, targets), batch_size=32)

        # Test Torch Utilities
        prepare_data_loader(dataloader)
        prepare_model(model)
        get_device()
        get_devices()

    trainer = TorchTrainer(
        train_func, scaling_config=ScalingConfig(num_workers=2, use_gpu=False)
    )
    trainer.fit()


def run_lightning():
    import pytorch_lightning as pl

    from ray.train.lightning import (
        RayDDPStrategy,
        RayDeepSpeedStrategy,
        RayFSDPStrategy,
        RayLightningEnvironment,
        RayTrainReportCallback,
        prepare_trainer,
    )

    def train_func():
        # Test Lighting utilites
        strategy = RayFSDPStrategy()
        strategy = RayDeepSpeedStrategy()
        strategy = RayDDPStrategy()
        ray_environment = RayLightningEnvironment()
        report_callback = RayTrainReportCallback()

        trainer = pl.Trainer(
            devices="auto",
            accelerator="auto",
            strategy=strategy,
            plugins=[ray_environment],
            callbacks=[report_callback],
        )
        trainer = prepare_trainer(trainer)

    trainer = TorchTrainer(
        train_func, scaling_config=ScalingConfig(num_workers=2, use_gpu=False)
    )

    trainer.fit()


def run_transformers():
    from datasets import Dataset
    from transformers import Trainer, TrainingArguments

    from ray.train.huggingface.transformers import (
        RayTrainReportCallback,
        prepare_trainer,
    )

    def train_func():
        # Create dummy model and datasets
        dataset = Dataset.from_dict({"text": ["text1", "text2"], "label": [0, 1]})
        model = torch.nn.Linear(10, 10)

        # Test Transformers utilites
        training_args = TrainingArguments(output_dir="./results", no_cuda=True)
        trainer = Trainer(model=model, args=training_args, train_dataset=dataset)

        trainer.add_callback(RayTrainReportCallback())
        trainer = prepare_trainer(trainer)

    trainer = TorchTrainer(
        train_func, scaling_config=ScalingConfig(num_workers=2, use_gpu=False)
    )

    trainer.fit()


@pytest.mark.parametrize("framework", ["torch", "lightning", "transformers"])
def test_torch_utility_usage_tags(shutdown_only, framework):
    from ray._private.usage.usage_lib import TagKey, get_extra_usage_tags_to_report

    ctx = ray.init()
    gcs_client = ray._raylet.GcsClient(address=ctx.address_info["gcs_address"])

    if framework == "torch":
        run_torch()
        expected_tags = [
            TagKey.TRAIN_TORCH_GET_DEVICE,
            TagKey.TRAIN_TORCH_GET_DEVICES,
            TagKey.TRAIN_TORCH_PREPARE_MODEL,
            TagKey.TRAIN_TORCH_PREPARE_DATALOADER,
        ]
    elif framework == "lightning":
        run_lightning()
        expected_tags = [
            TagKey.TRAIN_LIGHTNING_PREPARE_TRAINER,
            TagKey.TRAIN_LIGHTNING_RAYTRAINREPORTCALLBACK,
            TagKey.TRAIN_LIGHTNING_RAYDDPSTRATEGY,
            TagKey.TRAIN_LIGHTNING_RAYFSDPSTRATEGY,
            TagKey.TRAIN_LIGHTNING_RAYDEEPSPEEDSTRATEGY,
            TagKey.TRAIN_LIGHTNING_RAYLIGHTNINGENVIRONMENT,
        ]
    elif framework == "transformers":
        run_transformers()
        expected_tags = [
            TagKey.TRAIN_TRANSFORMERS_PREPARE_TRAINER,
            TagKey.TRAIN_TRANSFORMERS_RAYTRAINREPORTCALLBACK,
        ]

    result = get_extra_usage_tags_to_report(gcs_client)
    assert set(result.keys()).issuperset(
        {TagKey.Name(tag).lower() for tag in expected_tags}
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
