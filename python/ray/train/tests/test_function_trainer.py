import logging
import os
import time

import pytest

import ray
from ray import tune
from ray.air import ScalingConfig, Checkpoint, RunConfig
from ray.train.function_trainer import FunctionTrainer
from ray.tune import Callback, TuneError
from ray.tune.tuner import Tuner

logger = logging.getLogger(__name__)


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_8_cpus_2_gpus():
    address_info = ray.init(num_cpus=48, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_fn_trainer_invalid():
    def train_fn(config, invalid_param):
        pass

    with pytest.raises(ValueError):
        FunctionTrainer(train_fn)


def test_fn_trainer_fit(ray_start_4_cpus):
    def train_fn(config):
        tune.report(my_metric=1)

    trainer = FunctionTrainer(train_fn)
    result = trainer.fit()
    assert result.metrics["my_metric"] == 1


def test_fn_trainer_fit_return(ray_start_4_cpus):
    def train_fn(config):
        return 4

    trainer = FunctionTrainer(train_fn)
    result = trainer.fit()
    assert result.metrics["_metric"] == 4


def test_fn_trainer_scaling_config(ray_start_8_cpus_2_gpus):
    def train_fn(config):
        pg = ray.util.get_current_placement_group()
        return {
            "trainer_gpus": len(ray.get_gpu_ids()),
            "trainer_cpus": pg.bundle_specs[0]["CPU"],
            "worker_1_cpu": pg.bundle_specs[1]["CPU"],
            "worker_1_gpu": pg.bundle_specs[1]["GPU"],
            "worker_2_cpu": pg.bundle_specs[2]["CPU"],
            "worker_2_gpu": pg.bundle_specs[2]["GPU"],
        }

    trainer = FunctionTrainer(
        train_fn,
        scaling_config=ScalingConfig(
            trainer_resources={"CPU": 2},
            num_workers=2,
            resources_per_worker={"CPU": 3},
            use_gpu=True,
        ),
    )
    result = trainer.fit()
    assert result.metrics["trainer_gpus"] == 0
    assert result.metrics["worker_1_cpu"] == 3
    assert result.metrics["worker_1_gpu"] == 1
    assert result.metrics["worker_2_cpu"] == 3
    assert result.metrics["worker_2_gpu"] == 1


def test_fn_trainer_continue_checkpoint(ray_start_4_cpus):
    def train_fn(config, checkpoint_dir):
        state = Checkpoint.from_directory(checkpoint_dir).to_dict()

        return state["foo"]

    checkpoint = Checkpoint.from_dict({"foo": 7})

    trainer = FunctionTrainer(train_fn, resume_from_checkpoint=checkpoint)
    result = trainer.fit()
    assert result.metrics["_metric"] == 7


def test_fn_trainer_tune(ray_start_4_cpus):
    def train_fn(config):
        return config["foo"]

    trainer = FunctionTrainer(train_fn)
    tuner = Tuner(trainer, param_space={"foo": tune.grid_search([1, 2, 3, 4])})
    results = tuner.fit()

    assert [result.metrics["_metric"] for result in results] == [1, 2, 3, 4]


def test_fn_trainer_tune_checkpoint(ray_start_4_cpus):
    def train_fn(config, checkpoint_dir):
        state = Checkpoint.from_directory(checkpoint_dir).to_dict()

        return config["foo"] + state["bar"]

    checkpoint = Checkpoint.from_dict({"bar": 10})

    trainer = FunctionTrainer(train_fn, resume_from_checkpoint=checkpoint)
    tuner = Tuner(trainer, param_space={"foo": tune.grid_search([1, 2, 3, 4])})
    results = tuner.fit()

    assert [result.metrics["_metric"] for result in results] == [11, 12, 13, 14]


@pytest.mark.parametrize("pass_trainer", [True, False])
def test_fn_trainer_tune_resume(ray_start_4_cpus, tmpdir, pass_trainer):
    os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "0"

    def train_fn(config, checkpoint_dir=None):
        if checkpoint_dir:
            state = Checkpoint.from_directory(checkpoint_dir).to_dict()
            print("THIS IS A CHECKPOINT DIR", checkpoint_dir)
        else:
            state = {"it": 0}

        for i in range(0, 5):
            state["it"] += 1

            with tune.checkpoint_dir(step=i) as cp_dir:
                Checkpoint.from_dict(state).to_directory(cp_dir)

            tune.report(metric=state["it"])
            if (i + 1) == state["it"]:  # Only on first run (before restore)
                if state["it"] >= 3:  # Sleep so other trials can catch up
                    time.sleep(10)

    class FailureInjectionCallback(Callback):
        """Inject failure at the configured iteration number."""

        def __init__(self):
            self._die_soon = -1

        def on_step_end(self, iteration, trials, **kwargs):
            if iteration >= self._die_soon >= 0:
                raise RuntimeError("Failing after receiving results.")

            if len(trials) < 4:
                return

            if all(trial.last_result.get("metric") == 3 for trial in trials):
                self._die_soon = iteration + 1  # Leave time to process last result

    if pass_trainer:
        trainer = FunctionTrainer(train_fn)
    else:
        trainer = train_fn

    tuner = Tuner(
        trainer,
        param_space={"foo": tune.grid_search([1, 2, 3, 4])},
        run_config=RunConfig(
            name="fn_trainer_tune_resume",
            callbacks=[FailureInjectionCallback()],
            local_dir=str(tmpdir),
        ),
    )

    with pytest.raises(TuneError):
        tuner.fit()

    tuner = Tuner.restore(str(tmpdir / "fn_trainer_tune_resume"))
    # A hack before we figure out RunConfig semantics across resumes.
    tuner._local_tuner._run_config.callbacks = None
    results = tuner.fit()

    # Failure after step 3, continuing for another 5 steps
    assert all(result.metrics["metric"] > 5 for result in results)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
