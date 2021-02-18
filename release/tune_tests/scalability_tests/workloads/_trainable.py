import os
import time

import numpy as np
import pickle

from ray import tune

from ray.tune.durable_trainable import DurableTrainable


class TestDurableTrainable(DurableTrainable):
    def __init__(self, remote_checkpoint_dir, config, logger_creator=None):
        self.setup_env()

        super(TestDurableTrainable, self).__init__(
            remote_checkpoint_dir,
            config=config,
            logger_creator=logger_creator)

    def setup_env(self):
        pass

    def setup(self, config):
        self._num_iters = int(config["num_iters"])
        self._sleep_time = config["sleep_time"]
        self._score = config["score"]

        self._checkpoint_iters = config["checkpoint_iters"]
        self._checkpoint_size_b = config["checkpoint_size_b"]
        self._checkpoint_num_items = self._checkpoint_size_b // 8  # np.float64

        self._iter = 0

    def step(self):
        if self._iter > 0:
            time.sleep(self._sleep_time)

        res = dict(score=self._iter + self._score)

        if self._iter >= self._num_iters:
            res["done"] = True

        self._iter += 1
        return res

    def save_checkpoint(self, tmp_checkpoint_dir):
        checkpoint_file = os.path.join(tmp_checkpoint_dir, "bogus.ckpt")
        checkpoint_data = np.random.uniform(
            0, 1, size=self._checkpoint_num_items)
        with open(checkpoint_file, "wb") as fp:
            pickle.dump(checkpoint_data, fp)
        return checkpoint_file

    def load_checkpoint(self, checkpoint):
        pass


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
    durable = "sync_config" in tune_kwargs and \
              tune_kwargs["sync_config"].upload_dir.startswith("s3://")

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
        "checkpoint_size_b": checkpoint_size_b,
    }

    print(f"Starting benchmark with config: {config}")

    run_kwargs = {"reuse_actors": True, "verbose": 2}
    run_kwargs.update(tune_kwargs)

    _train = function_trainable

    aws_key_id = os.getenv("AWS_ACCESS_KEY_ID", "")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    aws_session = os.getenv("AWS_SESSION_TOKEN", "")

    if durable:

        class AwsDurableTrainable(TestDurableTrainable):
            AWS_ACCESS_KEY_ID = aws_key_id
            AWS_SECRET_ACCESS_KEY = aws_secret
            AWS_SESSION_TOKEN = aws_session

            def setup_env(self):
                os.environ["AWS_ACCESS_KEY_ID"] = self.AWS_ACCESS_KEY_ID
                os.environ[
                    "AWS_SECRET_ACCESS_KEY"] = self.AWS_SECRET_ACCESS_KEY
                os.environ["AWS_SESSION_TOKEN"] = self.AWS_SESSION_TOKEN

        _train = AwsDurableTrainable
        run_kwargs["checkpoint_freq"] = checkpoint_iters

    start_time = time.monotonic()
    tune.run(
        _train,
        config=config,
        num_samples=num_samples,
        raise_on_failed_trial=False,
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
