import pytest
import torch
from torch.distributed.fsdp import FullyShardedDataParallel

import ray
from ray import train
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer


@pytest.fixture
def ray_start_4_cpus_2_gpus():
    address_info = ray.init(num_cpus=4, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


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
def test_torch_simple_fsdp(device_type):
    """Tests if V2 ``prepare_model`` wraps CUDA and TPU models with SimpleFSDP ('simple_fsdp')."""
    from unittest.mock import MagicMock, patch

    from ray.train.v2.torch.train_loop_utils import prepare_model

    if (
        device_type == "tpu"
        and torch._C._get_privateuse1_backend_name() == "privateuseone"
    ):
        torch.utils.rename_privateuse1_backend("tpu")

    mock_mesh = MagicMock(name=f"{device_type}_mesh")
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
        patch("torch.cuda.set_device"),
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
        # 1. Default behavior: initializes 1D DeviceMesh and uses mode="fully_shard"
        prepare_model(
            model,
            move_to_device=torch.device(f"{device_type}:0"),
            parallel_strategy="simple_fsdp",
        )
        mock_init_mesh.assert_called_once_with(device_type, (4,))
        mock_simple_fsdp.data_parallel.assert_called_once_with(
            model, device_mesh=mock_mesh, mode="fully_shard"
        )

        # 2. Custom kwargs & explicit device_mesh: overrides defaults without redundant init_device_mesh
        mock_init_mesh.reset_mock()
        mock_simple_fsdp.data_parallel.reset_mock()
        prepare_model(
            model,
            move_to_device=torch.device(f"{device_type}:0"),
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

        # 3. Explicit device_mesh=None falls back to init_device_mesh without being overwritten
        mock_init_mesh.reset_mock()
        mock_simple_fsdp.data_parallel.reset_mock()
        kwargs_with_none = {"device_mesh": None}
        prepare_model(
            model,
            move_to_device=torch.device(f"{device_type}:0"),
            parallel_strategy="simple_fsdp",
            parallel_strategy_kwargs=kwargs_with_none,
        )
        mock_init_mesh.assert_called_once_with(device_type, (4,))
        mock_simple_fsdp.data_parallel.assert_called_once_with(
            model, device_mesh=mock_mesh, mode="fully_shard"
        )
        assert kwargs_with_none == {"device_mesh": None}

        # 4. Invalid parallel_strategy (e.g., missing underscore typo 'simplefsdp') raises ValueError
        with pytest.raises(ValueError, match="must be one of"):
            prepare_model(
                model,
                move_to_device=torch.device(f"{device_type}:0"),
                parallel_strategy="simplefsdp",
            )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
