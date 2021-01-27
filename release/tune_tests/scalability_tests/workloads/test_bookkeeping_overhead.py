import os

import ray

from _trainable import timed_tune_run


def main():
    os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "100"  # Tweak

    ray.init(address="auto")

    num_samples = 10000
    results_per_second = 1
    trial_length_s = 1

    max_runtime = 800

    timed_tune_run(
        name="bookkeeping overhead",
        num_samples=num_samples,
        results_per_second=results_per_second,
        trial_length_s=trial_length_s,
        max_runtime=max_runtime)


if __name__ == "__main__":
    main()
