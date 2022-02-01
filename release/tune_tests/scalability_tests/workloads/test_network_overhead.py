"""Networking overhead (200 trials on 200 nodes)

In this run, we will start 100 trials and run them on 100 different nodes.
This test will thus measure the overhead that comes with network communication
and specifically log synchronization.

Cluster: cluster_100x2.yaml

Test owner: krfricke

Acceptance criteria: Should run faster than 500 seconds.

Theoretical minimum time: 300 seconds
"""
import argparse
import ray
from ray import tune

from ray.tune.utils.release_test_util import timed_tune_run


def main(smoke_test: bool = False):
    ray.init(address="auto")

    num_samples = 100 if not smoke_test else 20
    results_per_second = 0.01
    trial_length_s = 300

    max_runtime = 1000

    timed_tune_run(
        name="result network overhead",
        num_samples=num_samples,
        results_per_second=results_per_second,
        trial_length_s=trial_length_s,
        max_runtime=max_runtime,
        resources_per_trial={"cpu": 2},  # One per node
        sync_config=tune.SyncConfig(syncer="auto"),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for training.",
    )
    args = parser.parse_args()

    main(args.smoke_test)
