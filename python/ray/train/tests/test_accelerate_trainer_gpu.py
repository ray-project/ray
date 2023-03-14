import contextlib
import pytest
import torch

import ray
import torch.nn as nn
from ray.train.examples.pytorch.torch_linear_example import (
    LinearDataset,
    train_epoch,
    validate_epoch,
)
from ray.train.batch_predictor import BatchPredictor
from ray.train.torch import TorchPredictor
from ray.air.config import ScalingConfig
import ray.train as train
from ray.cluster_utils import Cluster
from ray.air import session
from ray.train.tests.dummy_preprocessor import DummyPreprocessor
from ray.train.torch.torch_checkpoint import TorchCheckpoint
from ray.train.huggingface.accelerate_trainer import AccelerateTrainer
from accelerate import Accelerator

ACCELERATE_CONFIG_CPU = """compute_environment: LOCAL_MACHINE
deepspeed_config: {}
distributed_type: MULTI_CPU
downcast_bf16: 'no'
dynamo_config: {}
fsdp_config: {}
machine_rank: 0
main_process_ip: ''
main_process_port: 1
main_training_function: main
megatron_lm_config: {}
mixed_precision: 'no'
num_machines: 2
num_processes: 1
rdzv_backend: static
same_network: false
tpu_env: []
tpu_use_cluster: false
tpu_use_sudo: false
use_cpu: true
"""

ACCELERATE_CONFIG_GPU = """compute_environment: LOCAL_MACHINE
deepspeed_config: {}
distributed_type: MULTI_GPU
downcast_bf16: 'no'
dynamo_config: {}
fsdp_config: {}
gpu_ids: all
machine_rank: 0
main_process_ip: ''
main_process_port: 1
main_training_function: main
megatron_lm_config: {}
mixed_precision: 'no'
num_machines: 2
num_processes: 1
rdzv_backend: static
same_network: false
tpu_env: []
tpu_use_cluster: false
tpu_use_sudo: false
use_cpu: false
"""

ACCELERATE_CONFIG_DEEPSPEED = """compute_environment: LOCAL_MACHINE
deepspeed_config:
  deepspeed_hostfile: ''
  deepspeed_multinode_launcher: pdsh
  gradient_accumulation_steps: 1
  offload_optimizer_device: none
  offload_param_device: none
  zero3_init_flag: false
  zero_stage: 2
distributed_type: DEEPSPEED
downcast_bf16: 'no'
dynamo_config: {}
fsdp_config: {}
machine_rank: 0
main_process_ip: ''
main_process_port: 1
main_training_function: main
megatron_lm_config: {}
mixed_precision: 'no'
num_machines: 2
num_processes: 1
rdzv_backend: static
same_network: false
tpu_env: []
tpu_use_cluster: false
tpu_use_sudo: false
use_cpu: false
"""

ACCELERATE_CONFIG_DEEPSPEED_JSON = """compute_environment: LOCAL_MACHINE
deepspeed_config:
  deepspeed_config_file: deepspeed.json
  deepspeed_multinode_launcher: standard
  zero3_init_flag: false
distributed_type: DEEPSPEED
downcast_bf16: 'no'
dynamo_config: {}
fsdp_config: {}
machine_rank: 0
main_process_ip: ''
main_process_port: 1
main_training_function: main
megatron_lm_config: {}
num_machines: 2
num_processes: 1
rdzv_backend: static
same_network: true
tpu_env: []
tpu_use_cluster: false
tpu_use_sudo: false
use_cpu: false
"""

DEEPSPEED_JSON = """{
    "fp16": {
        "enabled": "auto",
        "loss_scale": 0,
        "loss_scale_window": 1000,
        "initial_scale_power": 16,
        "hysteresis": 2,
        "min_loss_scale": 1
    },
    "bf16": {
        "enabled": "auto"
    },
    "optimizer": {
        "type": "AdamW",
        "params": {
            "lr": "auto",
            "weight_decay": "auto",
            "torch_adam": true,
            "adam_w_mode": true
        }
    },
    "scheduler": {
        "type": "WarmupLR",
        "params": {
            "warmup_min_lr": "auto",
            "warmup_max_lr": "auto",
            "warmup_num_steps": "auto"
        }
    },
    "zero_optimization": {
        "stage": 2,
        "offload_optimizer": {
            "device": "cpu",
            "pin_memory": true
        },
        "allgather_partitions": true,
        "allgather_bucket_size": 2e8,
        "overlap_comm": true,
        "reduce_scatter": true,
        "reduce_bucket_size": "auto",
        "contiguous_gradients": true
    },
    "gradient_accumulation_steps": 1,
    "gradient_clipping": "auto",
    "steps_per_print": 2000,
    "train_batch_size": "auto",
    "train_micro_batch_size_per_gpu": "auto",
    "wall_clock_breakdown": false
}"""


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@contextlib.contextmanager
def ray_start_2_node_cluster(num_cpus_per_node: int, num_gpus_per_node: int):
    cluster = Cluster()
    for _ in range(2):
        cluster.add_node(num_cpus=num_cpus_per_node, num_gpus=num_gpus_per_node)

    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


def linear_train_func(accelerator, config):
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
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)
    train_loader, validation_loader, model, optimizer = accelerator.prepare(
        train_loader, validation_loader, model, optimizer
    )

    results = []
    for _ in range(epochs):
        train_epoch(train_loader, model, loss_fn, optimizer)
        state_dict, loss = validate_epoch(validation_loader, model, loss_fn)
        result = dict(loss=loss)
        results.append(result)
        session.report(result, checkpoint=TorchCheckpoint.from_state_dict(state_dict))

    return results


@pytest.mark.parametrize("num_workers", [1, 2])
@pytest.mark.parametrize(
    "accelerate_config_file_contents",
    [
        ACCELERATE_CONFIG_CPU,
        ACCELERATE_CONFIG_GPU,
        ACCELERATE_CONFIG_DEEPSPEED,
        ACCELERATE_CONFIG_DEEPSPEED_JSON,
    ],
)
def test_accelerate_linear(
    ray_2_node_2_gpu, num_workers, accelerate_config_file_contents, tmpdir
):
    def train_func(config):
        accelerator = Accelerator()
        assert accelerator.device == train.torch.get_device()
        assert accelerator.process_index == session.get_world_rank()
        assert accelerator.local_process_index == session.get_local_rank()
        result = linear_train_func(accelerator, config)
        assert len(result) == epochs
        assert result[-1]["loss"] < result[0]["loss"]

    accelerate_config_path = tmpdir / "accelerate_config.yaml"
    deepspeed_config_path = tmpdir / "deepspeed.json"

    accelerate_config_file_contents = accelerate_config_file_contents.replace(
        "deepspeed.json", f"'{deepspeed_config_path}'"
    )
    with open(accelerate_config_path, "w") as f:
        f.write(accelerate_config_file_contents)
    with open(deepspeed_config_path, "w") as f:
        f.write(DEEPSPEED_JSON)

    num_workers = num_workers
    epochs = 3
    scaling_config = ScalingConfig(
        num_workers=num_workers,
        use_gpu=accelerate_config_file_contents != ACCELERATE_CONFIG_CPU,
    )
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": epochs}

    trainer = AccelerateTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        accelerate_config=accelerate_config_path,
        scaling_config=scaling_config,
    )
    trainer.fit()


def test_accelerate_e2e(ray_start_4_cpus):
    def train_func():
        accelerator = Accelerator()
        assert accelerator.device == train.torch.get_device()
        assert accelerator.process_index == session.get_world_rank()
        model = torch.nn.Linear(3, 1)
        model = accelerator.prepare(model)
        session.report({}, checkpoint=TorchCheckpoint.from_model(model))

    scaling_config = ScalingConfig(num_workers=2)
    trainer = AccelerateTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scaling_config,
        accelerate_config={},
        preprocessor=DummyPreprocessor(),
    )
    result = trainer.fit()
    assert isinstance(result.checkpoint.get_preprocessor(), DummyPreprocessor)

    predict_dataset = ray.data.range(9)
    batch_predictor = BatchPredictor.from_checkpoint(result.checkpoint, TorchPredictor)
    predictions = batch_predictor.predict(
        predict_dataset, batch_size=3, dtype=torch.float
    )
    assert predictions.count() == 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
