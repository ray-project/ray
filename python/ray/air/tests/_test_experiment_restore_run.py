import collections
import json
import os
from pathlib import Path
import random
import time
from typing import Dict, List, Optional
from tempfile import TemporaryDirectory

import ray
from ray import train, tune
from ray.train import Checkpoint
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.tune.experiment import Trial


RUNNER_TYPE = os.environ.get("RUNNER_TYPE", "trainer")
STORAGE_PATH = os.environ.get("STORAGE_PATH", "/tmp/ray_results")
EXP_NAME = os.environ.get("EXP_NAME", "restore_integration_test")
CALLBACK_DUMP_FILE = os.environ.get(
    "CALLBACK_DUMP_FILE", "/tmp/callback_dump_file.json"
)
CSV_DATA_FILE = os.environ.get("CSV_DATA_FILE", "/tmp/dummy.csv")

TIME_PER_ITER_S = float(os.environ.get("TIME_PER_ITER_S", "0.5"))
NUM_TRIALS = int(os.environ.get("NUM_TRIALS", "1"))
MAX_CONCURRENT_TRIALS = int(os.environ.get("MAX_CONCURRENT_TRIALS", "2"))
ITERATIONS_PER_TRIAL = int(os.environ.get("ITERATIONS_PER_TRIAL", "64"))


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
        # Save callback contents to file
        with open(CALLBACK_DUMP_FILE, "w") as f:
            json.dump(self.get_state(), f, indent=2)

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
        return {"id": self._trial_count}

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


def train_fn(config: dict, data: Optional[dict] = None):
    checkpoint = train.get_checkpoint()
    start = checkpoint.to_dict()["iteration"] + 1 if checkpoint else 1

    training_started_marker = Path(
        os.environ.get("RUN_STARTED_MARKER", "/tmp/does-not-exist")
    )
    if training_started_marker.exists():
        # Multiple workers may be trying to delete the same marker
        try:
            training_started_marker.unlink()
        except FileNotFoundError:
            pass

    for iteration in range(start, ITERATIONS_PER_TRIAL + 1):
        time.sleep(TIME_PER_ITER_S)

        with TemporaryDirectory() as tmpdir:
            train.report(
                {"score": random.random()}, checkpoint=Checkpoint.from_directory(tmpdir)
            )


def tuner(experiment_path: str, run_config: train.RunConfig) -> tune.ResultGrid:
    trainable = tune.with_resources(train_fn, resources={"CPU": 1})
    trainable = tune.with_parameters(trainable, data={"dummy_data": [1, 2, 3]})

    if tune.Tuner.can_restore(experiment_path):
        tuner = tune.Tuner.restore(
            experiment_path, trainable=trainable, resume_errored=True
        )
    else:
        tuner = tune.Tuner(
            trainable,
            run_config=run_config,
            tune_config=tune.TuneConfig(
                num_samples=8,
                max_concurrent_trials=2,
                search_alg=StatefulSearcher(),
            ),
        )

    result_grid = tuner.fit()
    return result_grid


def trainer(experiment_path: str, run_config: train.RunConfig) -> train.Result:
    dataset_size = 128
    num_workers = 4

    def train_loop_per_worker(config):
        # Wrap the other train_fn with a check for the dataset.
        assert train.get_dataset_shard("train")
        train_fn(config)

    datasets = {
        "train": ray.data.range(dataset_size),
        "valid": ray.data.read_csv(CSV_DATA_FILE),
    }

    if DataParallelTrainer.can_restore(experiment_path):
        trainer = DataParallelTrainer.restore(
            experiment_path,
            datasets=datasets,
            train_loop_per_worker=train_loop_per_worker,
        )
    else:
        trainer = DataParallelTrainer(
            train_loop_per_worker,
            datasets=datasets,
            scaling_config=train.ScalingConfig(
                num_workers=num_workers, trainer_resources={"CPU": 0}
            ),
            run_config=run_config,
        )

    result = trainer.fit()
    return result


if __name__ == "__main__":
    experiment_path = os.path.join(STORAGE_PATH, EXP_NAME)

    ray.init()

    run_config = train.RunConfig(
        storage_path=STORAGE_PATH,
        name=EXP_NAME,
        checkpoint_config=train.CheckpointConfig(num_to_keep=1),
        callbacks=[StatefulCallback()],
    )

    if RUNNER_TYPE == "tuner":
        tuner(experiment_path, run_config)
    elif RUNNER_TYPE == "trainer":
        trainer(experiment_path, run_config)
    else:
        raise NotImplementedError(
            "`RUNNER_TYPE` environment var must be one of ['tuner', 'trainer']"
        )
