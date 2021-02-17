"""Large checkpoints in long running trials (16 trials, 4 GB checkpoints).

In this run, we will start 16 trials on a single node. The trials create
4 GB checkpoints every 15 minutes and should only keep 2 of these. This test
ensures that handling large checkpoints don't lead to much overhead.

Cluster: cluster_1x32_hd.yaml

Test owner: krfricke

Acceptance criteria: Should run faster than 90,000 seconds.

Theoretical minimum time: 86,400 seconds
"""
import ray
from ray import tune

from _trainable import timed_tune_run


def main():
    ray.init(address="auto")

    num_samples = 16
    results_per_second = 1 / 60
    trial_length_s = 86400

    max_runtime = 90000

    timed_tune_run(
        name="long running large checkpoints",
        num_samples=num_samples,
        results_per_second=results_per_second,
        trial_length_s=trial_length_s,
        max_runtime=max_runtime,
        checkpoint_freq_s=900,  # Once every 15 minutes
        checkpoint_size_b=int(3.75 * 1000**3),
        keep_checkpoints_num=2,  # 2 * 16 * 4 = 128 GB
        resources_per_trial={"cpu": 1},
        sync_config=tune.SyncConfig(sync_to_driver=True))


if __name__ == "__main__":
    main()
