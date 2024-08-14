import pyarrow.fs
import pytest

from ray.air.constants import MAX_REPR_LENGTH
from ray.train import CheckpointConfig, FailureConfig, RunConfig, ScalingConfig


@pytest.mark.parametrize(
    "config",
    [
        ScalingConfig(),
        ScalingConfig(use_gpu=True),
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


def test_storage_filesystem_repr():
    config = RunConfig(storage_filesystem=pyarrow.fs.S3FileSystem())
    repr(config)


def test_failure_config_init():
    FailureConfig(fail_fast=True)
    FailureConfig(fail_fast=False)
    FailureConfig(fail_fast="raise")

    with pytest.raises(ValueError):
        FailureConfig(fail_fast="fail")

    FailureConfig(fail_fast=True, max_failures=0)
    with pytest.raises(ValueError):
        FailureConfig(fail_fast=True, max_failures=1)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
