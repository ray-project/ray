"""
Tests that measure throughput of different algorithms on a simple 1 GPU
setup, then compare this throughput between frameworks.
"""

import copy
import json
import os
import time

from ray.rllib.agents.ppo import appo
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.utils.test_utils import framework_iterator


if __name__ == "__main__":

    config = copy.deepcopy(appo.DEFAULT_CONFIG)
    config["num_workers"] = 5

    config["min_iter_time_s"] = 0
    config["num_gpus"] = 0#TODO

    config["env"] = RandomEnv
    config["env_config"] = {"p_done": 0.005}

    config["train_batch_size"] = 150
    # Test with compression.
    config["compress_observations"] = True

    num_iterations = 100
    num_actions = 100000

    run_times = {}

    for fw in framework_iterator(config, with_eager_tracing=True):
        trainer = appo.APPOTrainer(config=config)
        input_dict = trainer.get_policy()._dummy_batch

        # Run one iteration before timing so we can ignore initializations and
        # tracings.
        trainer.train()

        # Only time the runs, not trainer initialization or initial tracing.
        start_t = time.time()
        for i in range(num_iterations):
            results = trainer.train()
        for i in range(num_actions):
            trainer.get_policy().compute_actions_from_input_dict(input_dict=input_dict)
        total_t = time.time() - start_t
        run_times[fw + ("+tracing" if config["eager_tracing"] else "")] = total_t

        trainer.stop()

    passed = True
    # Torch should be slower than tf (for now!).
    if run_times["torch"] < run_times["tf"]:
        passed = False
    # Tf2-eager should be slower than tf.
    if run_times["tf2"] < run_times["tf"]:
        passed = False
    # Tf2-eager-tracing should be faster than tf.
    if run_times["tf2+tracing"] > run_times["tf"]:
        passed = False

    results = {
        "time_taken": sum(run_times.values()),
        "last_update": time.time(),
        "passed": passed,
        "run_times": run_times,
    }

    print(results)#TODO

    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON",
        "/tmp/rllib_simple_performance_tests_with_random_env.json")
    with open(test_output_json, "wt") as f:
        json.dump(results, f)

    print("Ok.")
