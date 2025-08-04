import pytest
from tblib import pickling_support

import ray
from ray import cloudpickle
from ray.air._internal.util import StartTraceback, exception_cause, skip_exceptions
from ray.train import ScalingConfig
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.tune import Tuner


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def _failing_recursive(levels: int = 0, start_traceback: int = -1):
    if levels > 0:
        if start_traceback == 0:
            try:
                _failing_recursive(
                    levels=levels - 1, start_traceback=start_traceback - 1
                )
            except Exception as e:
                raise StartTraceback from e
        else:
            _failing_recursive(levels=levels - 1, start_traceback=start_traceback - 1)
    else:
        raise RuntimeError("Failing")


@pytest.mark.parametrize("levels", [4, 5, 6, 7, 8, 9, 10])
def test_short_traceback(levels):
    start_traceback = 3
    with pytest.raises(StartTraceback) as exc_info:
        _failing_recursive(levels=levels, start_traceback=start_traceback)

    exc = skip_exceptions(exc_info.value)
    tb = exc.__traceback__
    i = 0
    while tb:
        i += 1
        tb = tb.tb_next

    assert i == levels - start_traceback + 1


def test_recursion():
    """Test that the skipped exception does not point to the original exception."""
    root_exception = None

    with pytest.raises(StartTraceback) as exc_info:
        try:
            raise Exception("Root Exception")
        except Exception as e:
            root_exception = e
            raise StartTraceback from root_exception

    assert root_exception, "Root exception was not captured."

    start_traceback = exc_info.value
    skipped_exception = skip_exceptions(start_traceback)

    assert (
        root_exception != skipped_exception
    ), "Skipped exception points to the original exception."


def test_tblib():
    """Test that tblib does not cause a maximum recursion error."""

    with pytest.raises(Exception) as exc_info:
        try:
            try:
                raise Exception("Root Exception")
            except Exception as root_exception:
                raise StartTraceback from root_exception
        except Exception as start_traceback:
            raise skip_exceptions(start_traceback) from exception_cause(start_traceback)

    pickling_support.install()
    reraised_exception = exc_info.value
    # This should not raise a RecursionError/PicklingError.
    cloudpickle.dumps(reraised_exception)


def test_traceback_tuner(ray_start_2_cpus):
    """Ensure that the Tuner's stack trace is not too long."""

    def failing(config):
        raise RuntimeError("Error")

    tuner = Tuner(failing)
    results = tuner.fit()
    assert len(str(results[0].error).split("\n")) <= 20


def test_traceback_trainer(ray_start_2_cpus):
    """Ensure that the Trainer's stack trace is not too long."""

    def failing(config):
        raise RuntimeError("Error")

    trainer = DataParallelTrainer(failing, scaling_config=ScalingConfig(num_workers=1))
    with pytest.raises(RuntimeError) as exc_info:
        trainer.fit()
    assert len(str(exc_info.value).split("\n")) <= 13


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
