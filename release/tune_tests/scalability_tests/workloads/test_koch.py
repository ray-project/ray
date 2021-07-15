"""Result throughput on a cluster

In this run, we will start 1000 trials concurrently that report often
(1 result/2 seconds). We thus measure the amount of overhead incurred when
dealing with a large number of results from distributed trials.

Cluster: cluster_16x64.yaml

Test owner: krfricke

Acceptance criteria: Should run faster than 120 seconds.

Theoretical minimum time: 100 seconds
"""
import os

import ray
from ray import tune

from ray.tune.utils.release_test_util import timed_tune_run


def main():
    # Tweak
    os.environ["TUNE_DISABLE_AUTO_CALLBACK_LOGGERS"] = "1"
    os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "10000"
    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1600"
    os.environ["TUNE_PROCESS_TRIALS_PER_ITER"] = "1600"

    ray.init(address="auto")

    num_samples = 10000
    results_per_second = 1
    trial_length_s = 1

    max_runtime = 500

    timed_tune_run(
        name="koch",
        num_samples=num_samples,
        results_per_second=results_per_second,
        trial_length_s=trial_length_s,
        max_runtime=max_runtime,
        sync_config=tune.SyncConfig(sync_to_driver=False),
        reuse_actors=True)  # Tweak!


if __name__ == "__main__":
    main()
