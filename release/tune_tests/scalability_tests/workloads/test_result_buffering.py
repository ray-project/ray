import time

import ray
from ray import tune


def my_naive_trainable(config):
    for i in range(int(config["num_iters"])):
        tune.report(score=i + config["score"])
        time.sleep(config["sleep_time"])


def main():
    # ray.init(address="auto")
    ray.init()

    num_samples = 1_000

    sleep_time = 0.1
    num_iters = 3_00

    expected_run_time = num_iters * sleep_time
    # Allow minimum of 10 % overhead (or 10 seconds for short runs)
    expected_run_time += max(expected_run_time * 0.1, 10.)

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

    assert time_taken < expected_run_time, "This took too long."


if __name__ == "__main__":
    main()
