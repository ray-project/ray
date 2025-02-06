import json
import os
import pickle
import tempfile
import time
from collections import Counter

import numpy as np

from ray import train, tune
from ray._private.test_utils import safe_write_to_results_json
from ray.train import Checkpoint
from ray.tune.callback import Callback


class ProgressCallback(Callback):
    def __init__(self):
        self.last_update = 0
        self.update_interval = 60

    def on_step_end(self, iteration, trials, **kwargs):
        if time.time() - self.last_update > self.update_interval:
            now = time.time()
            result = {
                "last_update": now,
                "iteration": iteration,
                "trial_states": dict(Counter([trial.status for trial in trials])),
            }
            safe_write_to_results_json(result, "/tmp/release_test_out.json")

            self.last_update = now


class TestDurableTrainable(tune.Trainable):
    def __init__(self, *args, **kwargs):
        self.setup_env()

        super(TestDurableTrainable, self).__init__(*args, **kwargs)

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
        checkpoint_data = np.random.uniform(0, 1, size=self._checkpoint_num_items)
        with open(checkpoint_file, "wb") as fp:
            pickle.dump(checkpoint_data, fp)

    def load_checkpoint(self, checkpoint):
        pass


def function_trainable(config):
    num_iters = int(config["num_iters"])
    sleep_time = config["sleep_time"]
    score = config["score"]

    checkpoint_iters = config["checkpoint_iters"]
    checkpoint_size_b = config["checkpoint_size_b"]
    checkpoint_num_items = checkpoint_size_b // 8  # np.float64
    checkpoint_num_files = config["checkpoint_num_files"]

    for i in range(num_iters):
        metrics = {"score": i + score}
        if (
            checkpoint_iters >= 0
            and checkpoint_size_b > 0
            and i % checkpoint_iters == 0
        ):
            with tempfile.TemporaryDirectory() as tmpdir:
                for i in range(checkpoint_num_files):
                    checkpoint_file = os.path.join(tmpdir, f"bogus_{i}.ckpt")
                    checkpoint_data = np.random.uniform(0, 1, size=checkpoint_num_items)
                    with open(checkpoint_file, "wb") as fp:
                        pickle.dump(checkpoint_data, fp)
                train.report(metrics, checkpoint=Checkpoint.from_directory(tmpdir))
        else:
            train.report(metrics)

        time.sleep(sleep_time)


def timed_tune_run(
    name: str,
    num_samples: int,
    results_per_second: int = 1,
    trial_length_s: int = 1,
    max_runtime: int = 300,
    checkpoint_freq_s: int = -1,
    checkpoint_size_b: int = 0,
    checkpoint_num_files: int = 1,
    **tune_kwargs,
) -> bool:
    durable = (
        "storage_path" in tune_kwargs
        and tune_kwargs["storage_path"]
        and (
            tune_kwargs["storage_path"].startswith("s3://")
            or tune_kwargs["storage_path"].startswith("gs://")
        )
    )

    sleep_time = 1.0 / results_per_second
    num_iters = int(trial_length_s / sleep_time)
    checkpoint_iters = -1
    if checkpoint_freq_s >= 0:
        checkpoint_iters = int(checkpoint_freq_s / sleep_time)

    config = {
        "score": tune.uniform(0.0, 1.0),
        "num_iters": num_iters,
        "sleep_time": sleep_time,
        "checkpoint_iters": checkpoint_iters,
        "checkpoint_size_b": checkpoint_size_b,
        "checkpoint_num_files": checkpoint_num_files,
    }

    print(f"Starting benchmark with config: {config}")

    run_kwargs = {"reuse_actors": True, "verbose": 2}
    run_kwargs.update(tune_kwargs)

    _train = function_trainable

    if durable:
        _train = TestDurableTrainable
        run_kwargs["checkpoint_freq"] = checkpoint_iters

    start_time = time.monotonic()
    analysis = tune.run(
        _train,
        config=config,
        num_samples=num_samples,
        raise_on_failed_trial=False,
        **run_kwargs,
    )
    time_taken = time.monotonic() - start_time

    result = {
        "time_taken": time_taken,
        "trial_states": dict(Counter([trial.status for trial in analysis.trials])),
        "last_update": time.time(),
    }

    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/tune_test.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    success = time_taken <= max_runtime

    if not success:
        print(
            f"The {name} test took {time_taken:.2f} seconds, but should not "
            f"have exceeded {max_runtime:.2f} seconds. Test failed. \n\n"
            f"--- FAILED: {name.upper()} ::: "
            f"{time_taken:.2f} > {max_runtime:.2f} ---"
        )
    else:
        print(
            f"The {name} test took {time_taken:.2f} seconds, which "
            f"is below the budget of {max_runtime:.2f} seconds. "
            f"Test successful. \n\n"
            f"--- PASSED: {name.upper()} ::: "
            f"{time_taken:.2f} <= {max_runtime:.2f} ---"
        )

    return success
