import time

import ray
from ray import tune
from ray.tune.cluster_info import is_ray_cluster


def my_naive_trainable(config):
    for i in range(int(config["num_iters"])):
        tune.report(score=i + config["score"])
        time.sleep(config["sleep_time"])


def main():
    ray.init(address="auto")

    num_samples = 1000

    sleep_time = 0.1
    num_iters = 300

    expected_run_time = num_iters * sleep_time

    # Allow minimum of 20 % overhead (or 10 seconds for short runs)
    expected_run_time += max(expected_run_time * 0.2, 10.)

    if is_ray_cluster():
        # Add constant overhead for SSH connection
        expected_run_time += 0.3 * num_samples

    start_time = time.time()
    tune.run(
        my_naive_trainable,
        config={
            "score": tune.uniform(0., 1.),
            "num_iters": num_iters,
            "sleep_time": sleep_time
        },
        reuse_actors=True,
        verbose=2,
        num_samples=num_samples)
    time_taken = time.time() - start_time

    assert time_taken < expected_run_time, \
        f"The buffering test took {time_taken:.2f} seconds, but should not " \
        f"have exceeded {expected_run_time:.2f} seconds. Test failed."

    print(f"The buffering test took {time_taken:.2f} seconds, which "
          f"is below the budget of {expected_run_time:.2f} seconds. "
          f"Test successful.")


if __name__ == "__main__":
    main()
