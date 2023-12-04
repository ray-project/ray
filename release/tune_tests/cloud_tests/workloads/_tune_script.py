from typing import Optional

import argparse
import os
import pickle
import tempfile
import time

import ray
from ray import train, tune
from ray.train import Checkpoint
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.ppo import PPO

from run_cloud_test import ARTIFACT_FILENAME, CHECKPOINT_DATA_FILENAME


def fn_trainable(config):
    checkpoint = train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            with open(
                os.path.join(checkpoint_dir, CHECKPOINT_DATA_FILENAME), "rb"
            ) as f:
                checkpoint_dict = pickle.load(f)
        state = {"internal_iter": checkpoint_dict["internal_iter"] + 1}
    else:
        state = {"internal_iter": 1}

    for i in range(state["internal_iter"], config["max_iterations"] + 1):
        state["internal_iter"] = i
        time.sleep(config["sleep_time"])

        # Log artifacts to the trial dir.
        trial_dir = train.get_context().get_trial_dir()
        with open(os.path.join(trial_dir, ARTIFACT_FILENAME), "a") as f:
            f.write(f"{config['id']},")

        metrics = dict(
            score=i * 10 * config["score_multiplied"],
            internal_iter=state["internal_iter"],
        )
        if i % config["checkpoint_freq"] == 0:
            with tempfile.TemporaryDirectory() as tmpdir:
                with open(os.path.join(tmpdir, CHECKPOINT_DATA_FILENAME), "wb") as f:
                    pickle.dump({"internal_iter": i}, f)
                train.report(metrics, checkpoint=Checkpoint.from_directory(tmpdir))
        else:
            train.report(metrics)


class RLlibCallback(DefaultCallbacks):
    def on_train_result(self, *, algorithm, result: dict, **kwargs) -> None:
        result["internal_iter"] = result["training_iteration"]

        # Log artifacts to the trial dir.
        with open(os.path.join(algorithm.logdir, ARTIFACT_FILENAME), "a") as f:
            f.write(f"{algorithm.config['id']},")


class IndicatorCallback(tune.Callback):
    def __init__(self, indicator_file):
        self.indicator_file = indicator_file

    def on_step_begin(self, iteration, trials, **info):
        with open(self.indicator_file, "wt") as fp:
            fp.write("1")


def run_tune(
    no_syncer: bool,
    storage_path: Optional[str] = None,
    experiment_name: str = "cloud_test",
    indicator_file: str = "/tmp/tune_cloud_indicator",
    trainable: str = "function",
    num_cpus_per_trial: int = 2,
):
    if trainable == "function":
        train_fn = fn_trainable
        config = {
            "max_iterations": 100,
            "sleep_time": 5,
            "checkpoint_freq": 2,
            "score_multiplied": tune.randint(0, 100),
            "id": tune.grid_search([0, 1, 2, 3]),
        }
        kwargs = {"resources_per_trial": {"cpu": num_cpus_per_trial}}
    elif trainable == "rllib_str" or trainable == "rllib_trainer":
        if trainable == "rllib_str":
            train_fn = "PPO"
        else:
            train_fn = PPO

        config = {
            "env": "CartPole-v1",
            "num_workers": 1,
            "num_envs_per_worker": 1,
            "callbacks": RLlibCallback,
            "id": tune.grid_search([0, 1, 2, 3]),
        }
        kwargs = {
            "stop": {"training_iteration": 100},
            "checkpoint_freq": 2,
            "checkpoint_at_end": True,
        }
    else:
        raise RuntimeError(f"Unknown trainable: {trainable}")

    tune.run(
        train_fn,
        name=experiment_name,
        resume="AUTO",
        num_samples=1,  # 4 trials from the grid search
        config=config,
        storage_path=storage_path,
        sync_config=train.SyncConfig(
            syncer="auto" if not no_syncer else None,
            sync_on_checkpoint=True,
            sync_period=0.5,
            sync_artifacts=True,
        ),
        keep_checkpoints_num=2,
        callbacks=[IndicatorCallback(indicator_file=indicator_file)],
        verbose=2,
        **kwargs,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--no-syncer", action="store_true", default=False)

    parser.add_argument("--storage-path", required=False, default=None, type=str)

    parser.add_argument("--experiment-name", required=False, default=None, type=str)

    parser.add_argument(
        "--indicator-file",
        required=False,
        default="/tmp/tune_cloud_indicator",
        type=str,
    )

    args = parser.parse_args()

    trainable = str(os.environ.get("TUNE_TRAINABLE", "function"))
    num_cpus_per_trial = int(os.environ.get("TUNE_NUM_CPUS_PER_TRIAL", "2"))

    run_kwargs = dict(
        no_syncer=args.no_syncer or False,
        storage_path=args.storage_path or None,
        experiment_name=args.experiment_name or "cloud_test",
        indicator_file=args.indicator_file,
        trainable=trainable,
        num_cpus_per_trial=num_cpus_per_trial,
    )

    if not ray.is_initialized:
        ray.init(address="auto")

    print(f"Running on node {ray.util.get_node_ip_address()} with settings:")
    print(run_kwargs)

    run_tune(**run_kwargs)
