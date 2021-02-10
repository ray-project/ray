"""Durable trainable (16 trials, checkpoint to cloud)

In this run, we will start 16 trials on a cluster. The trials create
10 MB checkpoints every 10 seconds and should only keep 2 of these. This test
ensures that durable checkpoints don't slow down experiment progress too much.

Cluster: cluster_16x2.yaml

Test owner: krfricke

Acceptance criteria: Should run faster than 500 seconds.

Theoretical minimum time: 300 seconds
"""
import ray
from ray import tune

from _trainable import timed_tune_run


def main():
    ray.init(address="auto")

    num_samples = 16
    results_per_second = 10 / 60
    trial_length_s = 300

    max_runtime = 500

    timed_tune_run(
        name="durable trainable",
        num_samples=num_samples,
        results_per_second=results_per_second,
        trial_length_s=trial_length_s,
        max_runtime=max_runtime,
        checkpoint_freq_s=10,  # Once every 10 seconds
        checkpoint_size_b=int(10 * 1000**2),  # 10 MB
        keep_checkpoints_num=2,
        resources_per_trial={"cpu": 2},
        sync_config=tune.SyncConfig(
            sync_to_driver=False,
            upload_dir="s3://ray-tune-scalability-test/durable/",
        ))


if __name__ == "__main__":
    main()
