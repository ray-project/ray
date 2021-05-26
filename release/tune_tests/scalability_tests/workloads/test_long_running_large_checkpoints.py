"""Large checkpoints in long running trials (16 trials, 4 GB checkpoints).

In this run, we will start 16 trials on a single node. The trials create
4 GB checkpoints every 15 minutes and should only keep 2 of these. This test
ensures that handling large checkpoints don't lead to much overhead.

Cluster: cluster_1x32_hd.yaml

Test owner: krfricke

Acceptance criteria: Should run faster than 90,000 seconds.

Theoretical minimum time: 86,400 seconds
"""
import json
import os
import time

import ray
from ray import tune

from ray.tune.utils.release_test_util import timed_tune_run


class _ProgressCallback(ray.tune.callback.Callback):
    def __init__(self):
        self.last_update = 0
        self.update_interval = 60

    def on_step_end(self, iteration, trials):
        if time.time() - self.last_update > self.update_interval:
            now = time.time()
            result = {"last_update": now, "iteration": iteration}
            test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                              "/tmp/tune_test.json")
            with open(test_output_json, "wt") as f:
                json.dump(result, f)

            self.last_update = now


def main():
    ray.init(address="auto")

    num_samples = 16
    results_per_second = 1 / 60
    trial_length_s = 86400

    max_runtime = 90000

    callback = _ProgressCallback()

    timed_tune_run(
        name="long running large checkpoints",
        num_samples=num_samples,
        results_per_second=results_per_second,
        trial_length_s=trial_length_s,
        max_runtime=max_runtime,
        checkpoint_freq_s=900,  # Once every 15 minutes
        checkpoint_size_b=int(0.75 * 1000**3),
        keep_checkpoints_num=2,  # 2 * 16 * 4 = 128 GB
        resources_per_trial={"cpu": 1},
        sync_config=tune.SyncConfig(sync_to_driver=True),
        callbacks=[callback])


if __name__ == "__main__":
    main()
