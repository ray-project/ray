"""Durable trainable (16 trials, checkpoint to cloud)

In this run, we will start 16 trials on a cluster. The trials create
10 MB checkpoints every 10 seconds and should only keep 2 of these. This test
ensures that durable checkpoints don't slow down experiment progress too much.

Cluster: cluster_16x2.yaml

Test owner: krfricke

Acceptance criteria: Should run faster than 500 seconds.

Theoretical minimum time: 300 seconds
"""
import argparse
import os

import ray
from ray import tune

from ray.tune.utils.release_test_util import timed_tune_run


def main(bucket):
    secrets_file = os.path.join(os.path.dirname(__file__), "..", "aws_secrets.txt")
    if os.path.isfile(secrets_file):
        print(f"Loading AWS secrets from file {secrets_file}")

        from configparser import ConfigParser

        config = ConfigParser()
        config.read(secrets_file)

        for k, v in config.items():
            for x, y in v.items():
                var = str(x).upper()
                os.environ[var] = str(y)
    else:
        print("No AWS secrets file found. Loading from boto.")
        from boto3 import Session

        session = Session()
        credentials = session.get_credentials()
        current_credentials = credentials.get_frozen_credentials()

        os.environ["AWS_ACCESS_KEY_ID"] = current_credentials.access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = current_credentials.secret_key
        os.environ["AWS_SESSION_TOKEN"] = current_credentials.token

    if all(
        os.getenv(k, "")
        for k in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ]
    ):
        print("AWS secrets found in env.")
    else:
        print("Warning: No AWS secrets found in env!")

    ray.init(address="auto")

    num_samples = 16
    results_per_second = 10 / 60
    trial_length_s = 300

    max_runtime = 650

    timed_tune_run(
        name="durable trainable",
        num_samples=num_samples,
        results_per_second=results_per_second,
        trial_length_s=trial_length_s,
        max_runtime=max_runtime,
        checkpoint_freq_s=10,  # Once every 10 seconds
        checkpoint_size_b=int(10 * 1000**2),  # 10 MB
        keep_checkpoints_num=4,
        resources_per_trial={"cpu": 2},
        sync_config=tune.SyncConfig(
            upload_dir=f"s3://{bucket}/durable/",
        ),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, help="Bucket name")
    args, _ = parser.parse_known_args()

    main(args.bucket or "ray-tune-scalability-test")
