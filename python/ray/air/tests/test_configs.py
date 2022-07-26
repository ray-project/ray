import pytest

from ray.air.config import (
    ScalingConfig,
    DatasetConfig,
    FailureConfig,
    CheckpointConfig,
    RunConfig,
)
from ray.air.constants import MAX_REPR_LENGTH


@pytest.mark.parametrize(
    "config",
    [
        ScalingConfig(),
        ScalingConfig(use_gpu=True),
        DatasetConfig(),
        DatasetConfig(fit=True),
        FailureConfig(),
        FailureConfig(max_failures=2),
        CheckpointConfig(),
        CheckpointConfig(num_to_keep=1),
        RunConfig(),
        RunConfig(name="experiment"),
        RunConfig(failure_config=FailureConfig()),
    ],
)
def test_repr(config):
    representation = repr(config)

    assert eval(representation) == config
    assert len(representation) < MAX_REPR_LENGTH


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
