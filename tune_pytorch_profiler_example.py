"""Bookkeeping overhead (1 node, 10k trials)

In this run, we will start a large number of trials (10k) that take just a
second to run. We thus measure overhead that comes with dealing with a
large number of trials, e.g. experiment checkpointing.

Cluster: cluster_1x16.yaml

Test owner: krfricke

Acceptance criteria: Should run faster than 800 seconds.

Theoretical minimum time: 10000/16 = 625 seconds
"""
import os

import ray
import time

from ray.tune.utils.release_test_util import timed_tune_run
from torch.profiler import profile, ProfilerActivity, tensorboard_trace_handler


def main():
    os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "100"  # Tweak

    ray.init()

    num_samples = 10
    results_per_second = 1
    trial_length_s = 1

    max_runtime = 800

    with profile(activities=[ProfilerActivity.CPU], with_stack=True) as prof:
        timed_tune_run(
            name="bookkeeping overhead",
            num_samples=num_samples,
            results_per_second=results_per_second,
            trial_length_s=trial_length_s,
            max_runtime=max_runtime)
    tensorboard_trace_handler(f"./trace/{int(time.time())}")(prof)


if __name__ == "__main__":
    main()
