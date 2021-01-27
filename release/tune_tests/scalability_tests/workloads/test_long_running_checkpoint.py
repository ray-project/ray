import ray
from ray import tune
from ray.tune.cluster_info import is_ray_cluster

from _trainable import timed_tune_run


def main():
    ray.init(address="auto")

    num_samples = 64
    results_per_second = 1 / 3600
    trial_length_s = 86400

    max_runtime = 90000

    if is_ray_cluster():
        # Add constant overhead for SSH connection
        max_runtime = 90000

    timed_tune_run(
        name="long running checkpoint",
        num_samples=num_samples,
        results_per_second=results_per_second,
        trial_length_s=trial_length_s,
        max_runtime=max_runtime,
        checkpoint_freq_s=results_per_second,
        checkpoint_size_b=4 * 1024**3,  # 4 GB
        sync_config=tune.SyncConfig(sync_to_driver=True))


if __name__ == "__main__":
    main()
