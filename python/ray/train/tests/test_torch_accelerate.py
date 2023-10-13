import os
from tempfile import TemporaryDirectory

import pytest
import torch
import torch.nn as nn
from accelerate import Accelerator

import ray
import ray.train as train
from ray.train import Checkpoint, ScalingConfig
from ray.train.examples.pytorch.torch_linear_example import LinearDataset
from ray.train.torch import TorchTrainer

DEEPSPEED_CONFIG = {
    "fp16": {
        "enabled": "auto",
        "loss_scale": 0,
        "loss_scale_window": 1000,
        "initial_scale_power": 16,
        "hysteresis": 2,
        "min_loss_scale": 1,
    },
    "bf16": {"enabled": "auto"},
    "optimizer": {
        "type": "AdamW",
        "params": {
            "lr": "auto",
            "weight_decay": "auto",
            "torch_adam": True,
            "adam_w_mode": True,
        },
    },
    "zero_optimization": {
        "stage": 2,
        "offload_optimizer": {"device": "cpu", "pin_memory": True},
        "allgather_partitions": True,
        "allgather_bucket_size": 2e8,
        "overlap_comm": True,
        "reduce_scatter": True,
        "contiguous_gradients": True,
    },
    "gradient_accumulation_steps": 1,
    "gradient_clipping": "auto",
    "steps_per_print": 2000,
    "train_batch_size": "auto",
    "train_micro_batch_size_per_gpu": "auto",
    "wall_clock_breakdown": False,
}


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def linear_train_func(accelerator: Accelerator, config):
    from accelerate.utils import DummyOptim
    from deepspeed.ops.adam import DeepSpeedCPUAdam

    data_size = config.get("data_size", 1000)
    val_size = config.get("val_size", 400)
    batch_size = config.get("batch_size", 32)
    hidden_size = config.get("hidden_size", 1)
    lr = config.get("lr", 1e-2)
    epochs = config.get("epochs", 3)

    train_dataset = LinearDataset(2, 5, size=data_size)
    val_dataset = LinearDataset(2, 5, size=val_size)
    train_loader = torch.utils.data.DataLoader(train_dataset, batch_size=batch_size)
    validation_loader = torch.utils.data.DataLoader(val_dataset, batch_size=batch_size)

    model = nn.Linear(1, hidden_size)

    loss_fn = nn.MSELoss()
    if (
        accelerator.state.deepspeed_plugin
        and "optimizer" in accelerator.state.deepspeed_plugin.deepspeed_config
    ):
        optimizer_cls = DummyOptim
    elif accelerator.state.deepspeed_plugin:
        optimizer_cls = DeepSpeedCPUAdam
    else:
        optimizer_cls = torch.optim.SGD

    # Accelerate boilerplate
    no_decay = ["bias", "LayerNorm.weight"]
    optimizer_grouped_parameters = [
        {
            "params": [
                p
                for n, p in model.named_parameters()
                if not any(nd in n for nd in no_decay)
            ],
            "weight_decay": 0.0,
        },
        {
            "params": [
                p
                for n, p in model.named_parameters()
                if any(nd in n for nd in no_decay)
            ],
            "weight_decay": 0.0,
        },
    ]
    optimizer = optimizer_cls(optimizer_grouped_parameters, lr=lr)
    train_loader, validation_loader, model, optimizer = accelerator.prepare(
        train_loader, validation_loader, model, optimizer
    )

    results = []
    for _ in range(epochs):
        for X, y in train_loader:
            # Compute prediction error
            pred = model(X)
            loss = loss_fn(pred, y)

            # Backpropagation
            accelerator.backward(loss)
            optimizer.step()
            optimizer.zero_grad()

        num_batches = len(validation_loader)
        model.eval()
        loss = 0
        with torch.no_grad():
            for X, y in validation_loader:
                pred = model(X)
                loss += loss_fn(pred, y).item()
        loss /= num_batches
        import copy

        model_copy = copy.deepcopy(accelerator.unwrap_model(model))
        state_dict, loss = model_copy.cpu().state_dict(), loss

        result = dict(loss=loss)
        results.append(result)

        with TemporaryDirectory() as tmpdir:
            torch.save(state_dict, os.path.join(tmpdir, "checkpoint.pt"))
            train.report(result, checkpoint=Checkpoint.from_directory(tmpdir))

    return results


@pytest.mark.parametrize("use_gpu", [True, False])
def test_accelerate_base(ray_2_node_2_gpu, use_gpu):
    def train_func(config):
        accelerator = Accelerator(cpu=not use_gpu)
        assert accelerator.device == train.torch.get_device()
        assert accelerator.process_index == train.get_context().get_world_rank()
        if accelerator.device.type != "cpu":
            assert (
                accelerator.local_process_index == train.get_context().get_local_rank()
            )
        result = linear_train_func(accelerator, config)
        assert len(result) == epochs
        assert result[-1]["loss"] < result[0]["loss"]

    epochs = 3
    scaling_config = ScalingConfig(num_workers=2, use_gpu=use_gpu)
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": epochs}

    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=scaling_config,
    )
    trainer.fit()


def test_accelerate_deepspeed(ray_2_node_2_gpu):
    from accelerate import DeepSpeedPlugin

    def train_func(config):
        deepspeed_plugin = DeepSpeedPlugin(hf_ds_config=DEEPSPEED_CONFIG)
        accelerator = Accelerator(deepspeed_plugin=deepspeed_plugin)

        assert accelerator.device == train.torch.get_device()
        assert accelerator.process_index == train.get_context().get_world_rank()
        assert accelerator.local_process_index == train.get_context().get_local_rank()
        result = linear_train_func(accelerator, config)
        assert len(result) == epochs
        assert result[-1]["loss"] < result[0]["loss"]

    epochs = 3
    scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": epochs}

    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=scaling_config,
    )
    trainer.fit()


# Using CPU on purpose
@pytest.mark.parametrize("num_workers", [1, 2])
def test_accelerate_e2e(ray_start_4_cpus, num_workers):
    def train_func():
        accelerator = Accelerator(cpu=True)
        assert accelerator.device == train.torch.get_device()
        assert accelerator.process_index == train.get_context().get_world_rank()
        model = torch.nn.Linear(3, 1)
        model = accelerator.prepare(model)
        with TemporaryDirectory() as tmpdir:
            torch.save(model, os.path.join(tmpdir, "checkpoint.pt"))
            train.report({}, checkpoint=Checkpoint.from_directory(tmpdir))

    scaling_config = ScalingConfig(num_workers=num_workers)
    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scaling_config,
    )
    trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
