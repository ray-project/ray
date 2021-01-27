import os
import time

import numpy as np
import pickle

from ray import tune


def function_trainable(config):
    num_iters = int(config["num_iters"])
    sleep_time = config["sleep_time"]
    score = config["score"]

    checkpoint_iters = config["checkpoint_iters"]
    checkpoint_size_b = config["checkpoint_size_b"]
    checkpoint_num_items = checkpoint_size_b // 8  # np.float64

    for i in range(num_iters):
        if checkpoint_iters >= 0 and checkpoint_size_b > 0 and \
           i % checkpoint_iters == 0:
            with tune.checkpoint_dir(step=i) as dir:
                checkpoint_file = os.path.join(dir, "bogus.ckpt")
                checkpoint_data = np.random.uniform(
                    0, 1, size=checkpoint_num_items)
                with open(checkpoint_file, "wb") as fp:
                    pickle.dump(checkpoint_data, fp)

        tune.report(score=i + score)
        time.sleep(sleep_time)


def timed_tune_run(name: str,
                   num_samples: int,
                   results_per_second: int = 1,
                   trial_length_s: int = 1,
                   max_runtime: int = 300,
                   checkpoint_freq_s: int = -1,
                   checkpoint_size_b: int = 0,
                   **tune_kwargs):
    sleep_time = 1. / results_per_second
    num_iters = int(trial_length_s / sleep_time)
    checkpoint_iters = -1
    if checkpoint_freq_s >= 0:
        checkpoint_iters = int(checkpoint_freq_s / sleep_time)

    config = {
        "score": tune.uniform(0., 1.),
        "num_iters": num_iters,
        "sleep_time": sleep_time,
        "checkpoint_iters": checkpoint_iters,
        "checkpoint_size_b": checkpoint_size_b
    }

    print(f"Starting benchmark with config: {config}")

    run_kwargs = {"reuse_actors": True, "verbose": 2}
    run_kwargs.update(tune_kwargs)

    start_time = time.monotonic()
    tune.run(
        function_trainable,
        config=config,
        num_samples=num_samples,
        **run_kwargs)
    time_taken = time.monotonic() - start_time

    assert time_taken < max_runtime, \
        f"The {name} test took {time_taken:.2f} seconds, but should not " \
        f"have exceeded {max_runtime:.2f} seconds. Test failed. \n\n" \
        f"--- FAILED: {name.upper()} ::: " \
        f"{time_taken:.2f} > {max_runtime:.2f} ---"

    print(f"The {name} test took {time_taken:.2f} seconds, which "
          f"is below the budget of {max_runtime:.2f} seconds. "
          f"Test successful. \n\n"
          f"--- PASSED: {name.upper()} ::: "
          f"{time_taken:.2f} <= {max_runtime:.2f} ---")
