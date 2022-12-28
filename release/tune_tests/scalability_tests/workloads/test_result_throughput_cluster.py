"""Result throughput on a cluster

In this run, we will start 1000 trials concurrently that report often
(1 result/2 seconds). We thus measure the amount of overhead incurred when
dealing with a large number of results from distributed trials.

Cluster: cluster_16x64.yaml

Test owner: krfricke

Acceptance criteria: Should run faster than 135 seconds.

Theoretical minimum time: 100 seconds
"""
import os

import ray
from ray import tune
from ray.tune.execution.cluster_info import _is_ray_cluster

from ray.tune.utils.release_test_util import timed_tune_run


def main():
    os.environ["TUNE_DISABLE_AUTO_CALLBACK_LOGGERS"] = "1"  # Tweak
    os.environ["TUNE_RESULT_BUFFER_LENGTH"] = "1000"

    ray.init(address="auto")

    num_samples = 1000
    results_per_second = 0.5
    trial_length_s = 100

    max_runtime = 130

    if _is_ray_cluster():
        # Add constant overhead for SSH connection
        max_runtime = 130

    timed_tune_run(
        name="result throughput cluster",
        num_samples=num_samples,
        results_per_second=results_per_second,
        trial_length_s=trial_length_s,
        max_runtime=max_runtime,
        sync_config=tune.SyncConfig(syncer=None),
    )  # Tweak!


if __name__ == "__main__":
    main()
