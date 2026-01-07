import sys

import pytest

from ray.train import RunConfig, ScalingConfig
from ray.train.v2._internal.constants import (
    HEALTH_CHECK_INTERVAL_S_ENV_VAR,
    is_v2_enabled,
)
from ray.train.v2.jax import JaxTrainer

assert is_v2_enabled()


@pytest.fixture(autouse=True)
def reduce_health_check_interval(monkeypatch):
    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, "0.2")
    yield


@pytest.mark.skipif(sys.platform == "darwin", reason="JAX GPU not supported on macOS")
@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Current jax version is not supported in python 3.12+",
)
def test_jax_distributed_gpu_training(ray_start_4_cpus_2_gpus, tmp_path):
    """Test multi-GPU JAX distributed training.

    This test verifies that JAX distributed initialization works correctly
    across multiple GPU workers and that they can coordinate.
    """

    def train_func():
        import jax

        from ray import train

        # Get JAX distributed info
        devices = jax.devices()
        world_rank = train.get_context().get_world_rank()
        world_size = train.get_context().get_world_size()

        # Verify distributed setup
        assert world_size == 2, f"Expected world size 2, got {world_size}"
        assert world_rank in [0, 1], f"Invalid rank {world_rank}"
        assert len(devices) == 2, f"Expected 2 devices, got {len(devices)}"

        train.report(
            {
                "world_rank": world_rank,
                "world_size": world_size,
                "num_devices": len(devices),
            }
        )

    trainer = JaxTrainer(
        train_func,
        scaling_config=ScalingConfig(num_workers=2, use_gpu=True),
        run_config=RunConfig(storage_path=str(tmp_path)),
    )

    result = trainer.fit()
    assert result.error is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
