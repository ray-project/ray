import sys
from unittest.mock import MagicMock, patch

import pytest
import torch
from torch.distributed.fsdp import FullyShardedDataParallel

import ray
from ray import train
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer
from ray.train.v2.torch.train_loop_utils import prepare_model


@pytest.fixture
def ray_start_4_cpus_2_gpus():
    address_info = ray.init(num_cpus=4, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def _ensure_tpu_device_registered(device_type: str) -> None:
    """Registers the 'tpu' privateuse1 device name when running on CPU-only hosts."""
    if (
        device_type == "tpu"
        and torch._C._get_privateuse1_backend_name() == "privateuseone"
    ):
        torch.utils.rename_privateuse1_backend("tpu")


def test_torch_fsdp(ray_start_4_cpus_2_gpus):
    """Tests if ``prepare_model`` correctly wraps in FSDP."""

    def train_fn():
        model = torch.nn.Linear(1, 1)

        # Wrap in FSDP.
        model = train.torch.prepare_model(model, parallel_strategy="fsdp")

        # Make sure model is wrapped in FSDP.
        assert isinstance(model, FullyShardedDataParallel)

        # Make sure the model is on cuda.
        assert next(model.parameters()).is_cuda

    trainer = TorchTrainer(
        train_fn, scaling_config=ScalingConfig(num_workers=2, use_gpu=True)
    )
    trainer.fit()


@pytest.mark.parametrize("device_type", ["cuda", "tpu"])
def test_prepare_model_simple_fsdp_default_mesh(device_type: str):
    """Verifies default 1D DeviceMesh creation and 'fully_shard' mode."""
    _ensure_tpu_device_registered(device_type)

    mock_mesh = MagicMock(name=f"{device_type}_mesh")
    mock_simple_fsdp = MagicMock()
    mock_simple_fsdp.data_parallel.__name__ = "data_parallel"
    mock_simple_fsdp.data_parallel.side_effect = lambda mod, **kwargs: mod
    mock_ctx = MagicMock()
    mock_ctx.get_local_rank.return_value = 0
    mock_ctx.get_world_size.return_value = 4

    model = torch.nn.Sequential(torch.nn.Linear(4, 4), torch.nn.Linear(4, 2))

    with (
        patch("ray.train.get_context", return_value=mock_ctx),
        patch.object(torch.nn.Module, "to", return_value=model),
        patch(
            "torch.distributed.device_mesh.init_device_mesh",
            return_value=mock_mesh,
        ) as mock_init_mesh,
        patch.dict(
            "sys.modules",
            {
                "torchtitan": MagicMock(),
                "torchtitan.experiments": MagicMock(),
                "torchtitan.experiments.graph_trainer": MagicMock(
                    simple_fsdp=mock_simple_fsdp
                ),
            },
        ),
    ):
        prepare_model(
            model,
            move_to_device=torch.device(device_type),
            parallel_strategy="simple_fsdp",
        )

    mock_init_mesh.assert_called_once_with(device_type, (4,))
    mock_simple_fsdp.data_parallel.assert_called_once_with(
        model, device_mesh=mock_mesh, mode="fully_shard"
    )


@pytest.mark.parametrize("device_type", ["cuda", "tpu"])
def test_prepare_model_simple_fsdp_custom_mesh_and_kwargs(device_type: str):
    """Verifies custom device_mesh and kwargs bypass default mesh creation."""
    _ensure_tpu_device_registered(device_type)

    custom_mesh = MagicMock(name=f"{device_type}_custom_mesh")
    mock_simple_fsdp = MagicMock()
    mock_simple_fsdp.data_parallel.__name__ = "data_parallel"
    mock_simple_fsdp.data_parallel.side_effect = lambda mod, **kwargs: mod
    mock_ctx = MagicMock()
    mock_ctx.get_local_rank.return_value = 0
    mock_ctx.get_world_size.return_value = 4

    model = torch.nn.Sequential(torch.nn.Linear(4, 4), torch.nn.Linear(4, 2))

    with (
        patch("ray.train.get_context", return_value=mock_ctx),
        patch.object(torch.nn.Module, "to", return_value=model),
        patch("torch.distributed.device_mesh.init_device_mesh") as mock_init_mesh,
        patch.dict(
            "sys.modules",
            {
                "torchtitan": MagicMock(),
                "torchtitan.experiments": MagicMock(),
                "torchtitan.experiments.graph_trainer": MagicMock(
                    simple_fsdp=mock_simple_fsdp
                ),
            },
        ),
    ):
        prepare_model(
            model,
            move_to_device=torch.device(device_type),
            parallel_strategy="simple_fsdp",
            parallel_strategy_kwargs={
                "device_mesh": custom_mesh,
                "mode": "replicate",
                "shard_dim": 1,
            },
        )

    mock_init_mesh.assert_not_called()
    mock_simple_fsdp.data_parallel.assert_called_once_with(
        model, device_mesh=custom_mesh, mode="replicate", shard_dim=1
    )


def test_prepare_model_invalid_strategy_raises():
    """Verifies an unsupported parallel_strategy raises ValueError."""
    mock_ctx = MagicMock()
    mock_ctx.get_local_rank.return_value = 0
    mock_ctx.get_world_size.return_value = 4
    model = torch.nn.Linear(4, 2)

    with (
        patch("ray.train.get_context", return_value=mock_ctx),
        patch.object(torch.nn.Module, "to", return_value=model),
        pytest.raises(ValueError, match="must be one of"),
    ):
        prepare_model(
            model,
            move_to_device=torch.device("cpu"),
            parallel_strategy="simplefsdp",
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
