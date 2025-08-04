"""Durable trainable with multi-file checkpoints (16 trials, checkpoint to cloud)

In this run, we will start 16 trials on a cluster. The trials create 16 files a
1 MB checkpoints every 12 seconds and should only keep 2 checkpoints. This test
ensures that durable checkpoints don't slow down experiment progress too much.

Cluster: cluster_16x2.yaml

Test owner: krfricke

Acceptance criteria: Should run faster than 750 seconds.

Theoretical minimum time: 300 seconds
"""
import argparse

import ray

from ray.tune.utils.release_test_util import timed_tune_run


def main(bucket):
    ray.init(address="auto")

    num_samples = 16
    results_per_second = 5 / 60  # 5 results per minute = 1 every 12 seconds
    trial_length_s = 300

    max_runtime = 750

    timed_tune_run(
        name="durable multi-file checkpoints",
        num_samples=num_samples,
        results_per_second=results_per_second,
        trial_length_s=trial_length_s,
        max_runtime=max_runtime,
        checkpoint_freq_s=12,  # Once every 12 seconds (once per result)
        checkpoint_size_b=int(1 * 1000**2),  # 1 MB
        checkpoint_num_files=16,
        keep_checkpoints_num=2,
        resources_per_trial={"cpu": 2},
        storage_path=bucket,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, help="Bucket name")
    args, _ = parser.parse_known_args()

    main(args.bucket or "ray-tune-scalability-test")
