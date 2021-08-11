"""Learning regression tests for RLlib (torch and tf).

Runs Atari/PyBullet benchmarks for all major algorithms.
"""

from collections import Counter
import copy
import json
import os
from pathlib import Path
import re
import time
import yaml

import ray
from ray.tune import run_experiments

if __name__ == "__main__":
    # Get path of this very script to look for yaml files.
    abs_yaml_path = Path(__file__).parent
    print("abs_yaml_path={}".format(abs_yaml_path))

    # This pattern match is kind of hacky. Avoids cluster.yaml to get sucked
    # into this.
    yaml_files = abs_yaml_path.rglob("*test*.yaml")
    yaml_files = sorted(
        map(lambda path: str(path.absolute()), yaml_files), reverse=True)

    print("Will run the following regression tests:")
    for yaml_file in yaml_files:
        print("->", yaml_file)

    # How many times should we repeat a failed experiment?
    max_num_repeats = 2

    # All trials we'll ever run in this test script.
    all_trials = []
    # The experiments (by name) we'll run up to `max_num_repeats` times.
    experiments = {}
    # The results per experiment.
    checks = {}

    start_time = time.monotonic()

    # Loop through all collected files and gather experiments.
    # Augment all by `torch` framework.
    for yaml_file in yaml_files:
        tf_experiments = yaml.load(open(yaml_file).read())

        # Add torch version of all experiments to the list.
        for k, e in tf_experiments.items():
            e["config"]["framework"] = "tf"
            # We also stop early, once we reach the desired reward.
            e["stop"]["episode_reward_mean"] = \
                e["pass_criteria"]["episode_reward_mean"]

            # Generate the torch copy of the experiment.
            e_torch = copy.deepcopy(e)
            e_torch["config"]["framework"] = "torch"
            k_tf = re.sub("^(\\w+)-", "\\1-tf-", k)
            k_torch = re.sub("-tf-", "-torch-", k_tf)
            experiments[k_tf] = e
            experiments[k_torch] = e_torch
            # Generate `checks` dict.
            for k_ in [k_tf, k_torch]:
                checks[k_] = {
                    "min_reward": e["pass_criteria"]["episode_reward_mean"],
                    "min_timesteps": e["pass_criteria"]["timesteps_total"],
                    "time_total_s": e["stop"]["time_total_s"],
                    "failures": 0,
                    "passed": False,
                }
            # This key would break tune.
            del e["pass_criteria"]
            del e_torch["pass_criteria"]

    # Print out the actual config.
    print("== Test config ==")
    print(yaml.dump(experiments))

    # Keep track of those experiments we still have to run.
    # If an experiment passes, we'll remove it from this dict.
    experiments_to_run = experiments.copy()

    try:
        ray.init(address="auto")
    except ConnectionError:
        ray.init()

    for i in range(max_num_repeats):
        print(f"Starting learning test iteration {0}...")
        # We are done.
        if len(experiments_to_run) == 0:
            print("All experiments finished.")
            break

        # Run remaining experiments.
        trials = run_experiments(experiments_to_run, resume=False, verbose=2)
        all_trials.extend(trials)

        # Check each trial for whether we passed.
        # Criteria is to a) reach reward AND b) to have reached the throughput
        # defined by `timesteps_total` / `time_total_s`.
        for t in trials:
            experiment = t.trainable_name.lower() + "-" + \
                         t.config["framework"] + "-" + \
                         t.config["env"].lower()

            if t.status == "ERROR":
                checks[experiment]["failures"] += 1
            else:
                desired_reward = checks[experiment]["min_reward"]
                desired_timesteps = checks[experiment]["min_timesteps"]

                throughput = t.last_result["timesteps_total"] / \
                    t.last_result["time_total_s"]

                desired_throughput = \
                    desired_timesteps / t.stopping_criterion["time_total_s"]

                if t.last_result["episode_reward_mean"] < desired_reward or \
                        desired_throughput and throughput < desired_throughput:
                    checks[experiment]["failures"] += 1
                else:
                    checks[experiment]["passed"] = True
                    del experiments_to_run[experiment]

    ray.shutdown()

    time_taken = time.monotonic() - start_time

    # Create results dict and write it to disk.
    result = {
        "time_taken": time_taken,
        "trial_states": dict(Counter([trial.status for trial in all_trials])),
        "last_update": time.time(),
        "passed": [k for k, exp in checks.items() if exp["passed"]],
        "failures": {
            k: exp["failures"]
            for k, exp in checks.items() if exp["failures"] > 0
        }
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                      "/tmp/rllib_learning_test.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("PASSED.")
