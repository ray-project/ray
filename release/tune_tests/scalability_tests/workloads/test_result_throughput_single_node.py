"""Result throughput on a single node

In this run, we will start 96 trials concurrently that report very often
(50 results per second). We thus measure the amount of overhead incurred when
dealing with a large number of results.

Cluster: cluster_1x96.yaml

Test owner: krfricke

Acceptance criteria: Should run faster than 120 seconds.

Theoretical minimum time: 100 seconds
"""
import os

import ray

from ray.tune.utils.release_test_util import timed_tune_run


def main():
    os.environ["TUNE_DISABLE_AUTO_CALLBACK_LOGGERS"] = "1"  # Tweak
    os.environ["TUNE_RESULT_BUFFER_LENGTH"] = "1000"

    ray.init(address="auto")

    num_samples = 96
    results_per_second = 50
    trial_length_s = 100

    max_runtime = 120

    timed_tune_run(
        name="result throughput single node",
        num_samples=num_samples,
        results_per_second=results_per_second,
        trial_length_s=trial_length_s,
        max_runtime=max_runtime,
    )


if __name__ == "__main__":
    main()
