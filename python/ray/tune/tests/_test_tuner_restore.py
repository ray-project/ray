import collections
import json
import os
from pathlib import Path
import random
import time
from typing import Dict, List, Optional

from ray import air, tune
from ray.air import Checkpoint, session
from ray.tune.experiment import Trial

FAIL_ON = os.environ.get("FAIL_ON", "")

STORAGE_PATH = os.environ.get("STORAGE_PATH", "/tmp/ray_results")

EXP_NAME = os.environ.get("EXP_NAME", "tuner_restore_integration_test")

TIME_PER_ITER_S = float(os.environ.get("TIME_PER_ITER_S", "0.5"))

CALLBACK_DUMP_DIR = os.environ.get("CALLBACK_DUMP_DIR", "/tmp/callback_dump_dir")

# 4 iterations per failure --> 2 seconds per failure
# 11 failure hooks + 1 "no failure" --> 12 total failures
# ~24 seconds, 48 iterations at the last failure

TOTAL_FAILURES = 12
MAX_CONCURRENT_TRIALS = 1
ITERATIONS_PER_FAILURE = 4
FAILURES_PER_TRIAL = 2

ITERATIONS_PER_TRIAL = ITERATIONS_PER_FAILURE * FAILURES_PER_TRIAL  # 8
NUM_TRIALS = MAX_CONCURRENT_TRIALS * (TOTAL_FAILURES // FAILURES_PER_TRIAL)  # 6


class StatefulCallback(tune.Callback):
    def __init__(self):
        self._trial_iterations = collections.defaultdict(list)

    def on_trial_result(
        self,
        iteration: int,
        trials: List["Trial"],
        trial: "Trial",
        result: Dict,
        **info,
    ):
        self._trial_iterations[trial.trial_id].append(result["training_iteration"])

    def on_experiment_end(self, trials: List["Trial"], **info):
        # Save to directory
        pass

    def get_state(self) -> Optional[Dict]:
        return {"trial_iters": self._trial_iterations.copy()}

    def set_state(self, state: Dict):
        self._trial_iterations = state["trial_iters"]


class StatefulSearcher(tune.search.Searcher):
    def __init__(
        self,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
    ):
        super().__init__(metric=metric, mode=mode)
        self._trial_count = 0

    def suggest(self, trial_id: str) -> Optional[Dict]:
        self._trial_count += 1
        # Have a few trials error occasionally.
        should_error = self._trial_count % MAX_CONCURRENT_TRIALS == 0
        return {"id": self._trial_count, "should_error": should_error}

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict] = None, error: bool = False
    ) -> None:
        pass

    def save(self, checkpoint_path: str):
        with open(checkpoint_path, "w") as f:
            json.dump({"trial_count": self._trial_count}, f)

    def restore(self, checkpoint_path: str):
        with open(checkpoint_path, "r") as f:
            state = json.load(f)
        self._trial_count = state["trial_count"]


class FailingCallback(tune.Callback):
    def __init__(self):
        self._trial_iters_since_restore = {}

    @property
    def fail_on(self) -> str:
        return os.environ.get("FAIL_ON", "")

    def should_fail(self, trials: List["Trial"]) -> bool:
        if len(self._trial_iters_since_restore) < MAX_CONCURRENT_TRIALS:
            return False

        should_fail = all(
            iter >= ITERATIONS_PER_FAILURE
            for iter in self._trial_iters_since_restore.values()
        )
        return should_fail

    def on_step_begin(self, iteration: int, trials: List["Trial"], **info):
        if self.should_fail(trials) and self.fail_on == "on_step_begin":
            raise RuntimeError("on_step_begin")

    def on_step_end(self, iteration: int, trials: List["Trial"], **info):
        if self.should_fail(trials) and self.fail_on == "on_step_end":
            raise RuntimeError("on_step_end")

    def on_trial_start(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        if self.should_fail(trials) and self.fail_on == "on_trial_start":
            raise RuntimeError("on_trial_start")

    def on_trial_restore(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        if self.should_fail(trials) and self.fail_on == "on_trial_restore":
            raise RuntimeError("on_trial_restore")

    def on_trial_save(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        if self.should_fail(trials) and self.fail_on == "on_trial_save":
            raise RuntimeError("on_trial_save")

    def on_trial_result(
        self,
        iteration: int,
        trials: List["Trial"],
        trial: "Trial",
        result: Dict,
        **info,
    ):
        if self.should_fail(trials) and self.fail_on == "on_trial_result":
            raise RuntimeError("on_trial_result")
        self._trial_iters_since_restore[trial.trial_id] = result.get(
            "iterations_since_restore", 0
        )

    def on_trial_complete(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        if self.should_fail(trials) and self.fail_on == "on_trial_complete":
            raise RuntimeError("on_trial_complete")

    def on_trial_error(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        if self.should_fail(trials) and self.fail_on == "on_trial_error":
            raise RuntimeError("on_trial_error")

    def on_checkpoint(
        self,
        iteration: int,
        trials: List["Trial"],
        trial: "Trial",
        checkpoint,
        **info,
    ):
        if self.should_fail(trials) and self.fail_on == "on_checkpoint":
            raise RuntimeError("on_checkpoint")


def train_fn(config, data=None):
    checkpoint = session.get_checkpoint()
    start = checkpoint.to_dict()["iteration"] + 1 if checkpoint else 1

    training_started_marker = Path(os.environ.get("RUN_STARTED_MARKER", "asdf.py"))

    if training_started_marker.exists():
        training_started_marker.unlink()
    else:
        time.sleep(TIME_PER_ITER_S * 2)

    for iteration in range(start, ITERATIONS_PER_TRIAL + 1):
        time.sleep(TIME_PER_ITER_S)

        session.report(
            {"score": random.random()},
            checkpoint=Checkpoint.from_dict({"iteration": iteration}),
        )


trainable = tune.with_resources(train_fn, resources={"CPU": 1})
trainable = tune.with_parameters(trainable, data={"dummy_data": [1, 2, 3]})


experiment_path = os.path.join(STORAGE_PATH, EXP_NAME)

if tune.Tuner.can_restore(experiment_path):
    tuner = tune.Tuner.restore(
        experiment_path, trainable=trainable, resume_errored=True
    )
else:
    tuner = tune.Tuner(
        trainable,
        run_config=air.RunConfig(
            local_dir=STORAGE_PATH,
            name=EXP_NAME,
            checkpoint_config=air.CheckpointConfig(num_to_keep=1),
            failure_config=air.FailureConfig(max_failures=-1),
            callbacks=[StatefulCallback(), FailingCallback()],
        ),
        tune_config=tune.TuneConfig(
            num_samples=8,
            max_concurrent_trials=2,
            search_alg=StatefulSearcher(),
        ),
    )

result_grid = tuner.fit()
