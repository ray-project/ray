from typing import Optional

import argparse
import json
import os
import time

import ray
from ray import tune


def train(config, checkpoint_dir=None):
    if checkpoint_dir:
        with open(os.path.join(checkpoint_dir, "checkpoint.json"), "rt") as fp:
            state = json.load(fp)
    else:
        state = {"internal_iter": 0}

    for i in range(state["internal_iter"], config["max_iterations"]):
        state["internal_iter"] = i
        time.sleep(config["sleep_time"])

        if i % config["checkpoint_freq"] == 0:
            with tune.checkpoint_dir(step=i) as cd:
                with open(os.path.join(cd, "checkpoint.json"), "wt") as fp:
                    json.dump(state, fp)

        tune.report(
            score=i * 10 * config["score_multiplied"],
            internal_iter=state["internal_iter"])


class IndicatorCallback(tune.Callback):
    def __init__(self, indicator_file):
        self.indicator_file = indicator_file

    def on_step_begin(self, iteration, trials, **info):
        with open(self.indicator_file, "wt") as fp:
            fp.write("1")


def run_tune(
        no_syncer: bool,
        upload_dir: Optional[str] = None,
        experiment_name: str = "cloud_test",
        indicator_file: str = "/tmp/tune_cloud_indicator",
):
    num_cpus_per_trial = int(os.environ.get("TUNE_NUM_CPUS_PER_TRIAL", "2"))

    tune.run(
        train,
        name=experiment_name,
        resume="AUTO",
        num_samples=4,
        config={
            "max_iterations": 30,
            "sleep_time": 5,
            "checkpoint_freq": 2,
            "score_multiplied": tune.randint(0, 100),
        },
        sync_config=tune.SyncConfig(
            syncer="auto" if not no_syncer else None,
            upload_dir=upload_dir,
            sync_on_checkpoint=True,
            sync_period=0.5,
        ),
        keep_checkpoints_num=2,
        resources_per_trial={"cpu": num_cpus_per_trial},
        callbacks=[IndicatorCallback(indicator_file=indicator_file)],
        verbose=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--no-syncer", action="store_true", default=False)

    parser.add_argument("--upload-dir", required=False, default=None, type=str)

    parser.add_argument(
        "--experiment-name", required=False, default=None, type=str)

    parser.add_argument(
        "--indicator-file",
        required=False,
        default="/tmp/tune_cloud_indicator",
        type=str)

    args = parser.parse_args()

    run_kwargs = dict(
        no_syncer=args.no_syncer or False,
        upload_dir=args.upload_dir or None,
        experiment_name=args.experiment_name or "cloud_test",
        indicator_file=args.indicator_file,
    )

    if not ray.is_initialized:
        ray.init(address="auto")

    print(f"Running on node {ray.util.get_node_ip_address()} with settings:")
    print(run_kwargs)

    run_tune(**run_kwargs)
