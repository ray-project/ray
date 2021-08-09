"""Stress tests for RLlib (torch and tf).

Runs IMPALA on 4 GPUs and 100s of CPUs.
"""

from collections import Counter
import json
import os
from pathlib import Path
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
    yaml_files = abs_yaml_path.rglob("*tests.yaml")
    yaml_files = sorted(
        map(lambda path: str(path.absolute()), yaml_files), reverse=True)

    print("Will run the following regression tests:")
    for yaml_file in yaml_files:
        print("->", yaml_file)

    # How many times should we repeat a failed experiment?
    max_num_repeats = 1

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
        experiment_configs = yaml.load(open(yaml_file).read())

        # Add torch version of all experiments to the list.
        # for k, e in experiment.items():
        #     checks[k] = {
        #         "time_total_s": e["stop"]["time_total_s"],
        #         "timesteps_total": e["stop"]["timesteps_total"],
        #         "failures": 0,
        #         "passed": False,
        #     }
        experiments.update(experiment_configs)

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
        # We are done.
        if len(experiments_to_run) == 0:
            break

        # Run remaining experiments.
        trials = run_experiments(experiments_to_run, resume=False, verbose=2)
        all_trials.extend(trials)

        # Check each trial for whether we passed.
        # Criteria is to a) reach reward AND b) to have reached the throughput
        # defined by `timesteps_total` / `time_total_s`.
        for t in trials:
            exp_key = t.trainable_name.lower() + "-" + \
                      t.config["framework"] + "-" + t.config["env"].lower()

            if exp_key not in checks:
                checks[exp_key] = {
                    "time_total_s": t.last_result["time_total_s"],
                    "timesteps_total": t.last_result["timesteps_total"],
                    "failures": 0,
                    "passed": False,
                }

            if t.status == "ERROR":
                checks[exp_key]["failures"] += 1
            else:
                desired_reward = t.stopping_criterion.get(
                    "episode_reward_mean")
                desired_timesteps = t.stopping_criterion.get("timesteps_total")
                desired_time = t.stopping_criterion.get("time_total_s")

                throughput = t.last_result["timesteps_total"] / \
                    t.last_result["time_total_s"]

                desired_throughput = None
                if desired_time is not None and desired_timesteps is not None:
                    desired_throughput = desired_timesteps / desired_time

                if (desired_reward and t.last_result["episode_reward_mean"] <
                        desired_reward) or (desired_throughput and
                                            throughput < desired_throughput):
                    checks[exp_key]["failures"] += 1
                else:
                    checks[exp_key]["passed"] = True
                    # Todo: Delete from `experiments_to_run`
                    # Disabled here because we only try once at the moment
                    # del experiments_to_run[FIX]

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
                                      "/tmp/rllib_stress_test.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("PASSED.")
