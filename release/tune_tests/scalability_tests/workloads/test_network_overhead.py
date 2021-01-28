import ray
from ray import tune

from _trainable import timed_tune_run


def main():
    ray.init(address="auto")

    num_samples = 200
    results_per_second = 1
    trial_length_s = 300

    max_runtime = 500

    timed_tune_run(
        name="result network overhead",
        num_samples=num_samples,
        results_per_second=results_per_second,
        trial_length_s=trial_length_s,
        max_runtime=max_runtime,
        resources_per_trial={"cpu": 2},  # One per node
        sync_config=tune.SyncConfig(sync_to_driver=True))


if __name__ == "__main__":
    main()
