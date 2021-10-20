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


def run_tune(experiment_name: str = "tune_cloud_test"):
    tune.run(
        train,
        name=experiment_name,
        resume="AUTO",
        num_samples=4,
        config={
            "max_iterations": 30,
            "sleep_time": 10,
            "checkpoint_freq": 2,
            "score_multiplied": tune.randint(0, 100)
        },
        verbose=2)


if __name__ == "__main__":
    if not ray.is_initialized:
        ray.init(address="auto")

    run_tune()
