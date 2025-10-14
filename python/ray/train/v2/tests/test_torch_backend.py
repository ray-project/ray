import pytest
import torch

from ray.train import ScalingConfig
from ray.train.constants import TORCH_PROCESS_GROUP_SHUTDOWN_TIMEOUT_S
from ray.train.torch import TorchConfig, TorchTrainer


@pytest.mark.parametrize("init_method", ["env", "tcp"])
def test_torch_start_shutdown(ray_start_4_cpus, init_method):
    def check_process_group():
        assert (
            torch.distributed.is_initialized()
            and torch.distributed.get_world_size() == 2
        )

    torch_config = TorchConfig(backend="gloo", init_method=init_method)
    trainer = TorchTrainer(
        train_loop_per_worker=check_process_group,
        scaling_config=ScalingConfig(num_workers=2),
        torch_config=torch_config,
    )
    trainer.fit()


@pytest.mark.parametrize("timeout_s", [5, 0])
def test_torch_process_group_shutdown_timeout(ray_start_4_cpus, monkeypatch, timeout_s):
    """Tests that we don't more than a predefined timeout
    on Torch process group shutdown."""

    monkeypatch.setenv(TORCH_PROCESS_GROUP_SHUTDOWN_TIMEOUT_S, str(timeout_s))

    trainer = TorchTrainer(
        train_loop_per_worker=lambda: None,
        scaling_config=ScalingConfig(num_workers=2),
        torch_config=TorchConfig(backend="gloo"),
    )
    # Even if shutdown times out (timeout_s=0),
    # the training should complete successfully.
    trainer.fit()
