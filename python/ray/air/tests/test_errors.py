"""
This test suite covers error handling and propagation in Ray AIR.

There are two main error types to test:
1. Trainable errors: These happen in the remote actor itself.
    -> Within this, we should test:
        - fail_fast=True/False/'raise'
        - AIR Trainer w/o Tuner, AIR Trainer w/ Tuner, Tuner w/ function trainable
2. Tune driver errors: These happen in the Tune event-handling loop.
    -> Within this, we should test:
        - Errors occurring at different points in the Tune loop
          (on_trial_result, on_checkpoint, on_step_begin, etc.)

These tests should:
- Assert how errors from the trainable/Trainer get propagated to the user.
- Assert how errors from the Tune driver get propagated to the user.
"""
import gc
import pytest

import ray
from ray import tune
from ray.air import FailureConfig, RunConfig, ScalingConfig
from ray.train.trainer import BaseTrainer, TrainingFailedError
from ray.tune import Tuner, TuneConfig


@pytest.fixture(scope="module")
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4, configure_logging=False)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture(autouse=True)
def gc_collect():
    # Make sure to cleanup as much as possible between
    # unit tests that share a Ray session
    yield
    gc.collect()


class _TestSpecificError(RuntimeError):
    pass


class FailingTrainer(BaseTrainer):
    _scaling_config_allowed_keys = BaseTrainer._scaling_config_allowed_keys + [
        "num_workers",
        "use_gpu",
        "resources_per_worker",
        "placement_strategy",
    ]

    def training_loop(self) -> None:
        raise _TestSpecificError("There is an error in trainer!")


def failing_fn(config):
    raise _TestSpecificError("Failing!")


trainable_map = {
    "function": failing_fn,
    "trainer": FailingTrainer(),
}


@pytest.mark.parametrize("fail_fast", [False, True, "raise"])
@pytest.mark.parametrize("trainable_type", ["function", "trainer"])
def test_trainable_error_with_tuner(ray_start_4_cpus, fail_fast, trainable_type):
    trainable = trainable_map[trainable_type]

    tuner = Tuner(
        trainable=trainable,
        run_config=RunConfig(
            name=f"test_tuner_errors-fail_fast={fail_fast}-trainable_type={trainable_type}",
            failure_config=FailureConfig(fail_fast=fail_fast),
        ),
        tune_config=TuneConfig(num_samples=2),
    )

    if fail_fast is False:
        # Both trials should complete with an error.
        results = tuner.fit()
        assert len(results) == 2
        for i in range(2):
            assert results[i].error
    elif fail_fast is True:
        # The first trial errors -> the experiment finishes immediately.
        results = tuner.fit()
        errors = [result.error for result in results if result.error]
        assert len(errors) == 1
    elif fail_fast == "raise":
        # The error gets raised to the user
        with pytest.raises(_TestSpecificError):
            tuner.fit()


@pytest.mark.parametrize("fail_fast", [False, True, "raise"])
def test_trainable_error_with_trainer(ray_start_4_cpus, fail_fast):
    trainer = FailingTrainer(
        run_config=RunConfig(
            name=f"test_trainer_errors-fail_fast={fail_fast}",
            failure_config=FailureConfig(fail_fast=fail_fast),
        ),
    )

    if fail_fast in [False, True]:
        # There is only 1 "trial" for a Trainer,
        # so fail_fast = True/False doesn't change the behavior
        # In both cases, the error should get wrapped and raised.
        with pytest.raises(TrainingFailedError) as exc_info:
            trainer.fit()

        # The cause of the error should be the trainable error
        assert isinstance(exc_info.value.__cause__, _TestSpecificError)
    elif fail_fast == "raise":
        # The error gets raised to the user
        with pytest.raises(_TestSpecificError):
            trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
