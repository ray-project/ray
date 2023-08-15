import json
import os
import pickle
import tempfile
import time
from collections import Counter

import click
import numpy as np

from ray import train, tune
from ray.train import CheckpointConfig, RunConfig
from ray.train._checkpoint import Checkpoint
from ray.tune import Callback


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

            path = "/tmp/release_test_out.json"
            test_output_json_tmp = path + ".tmp"
            with open(test_output_json_tmp, "wt") as f:
                json.dump(result, f)
            os.replace(test_output_json_tmp, path)
            print(f"Wrote results to {test_output_json_tmp}")
            print(json.dumps(result))

            self.last_update = now


def function_trainable(config):
    sleep_time = config["sleep_time"]
    score = config["score"]

    checkpoint_num_files = config["checkpoint_num_files"]
    checkpoint_iters = config["checkpoint_iters"]
    checkpoint_size_b = config["checkpoint_size_b"] // checkpoint_num_files
    checkpoint_num_items = checkpoint_size_b // 8  # np.float64

    for i in range(int(10e12)):
        metrics = {"score": i + score}
        if (
            checkpoint_iters >= 0
            and checkpoint_size_b > 0
            and i % checkpoint_iters == 0
        ):
            with tempfile.TemporaryDirectory() as directory:
                for i in range(checkpoint_num_files):
                    checkpoint_file = os.path.join(directory, f"bogus_{i:02d}.ckpt")
                    checkpoint_data = np.random.uniform(0, 1, size=checkpoint_num_items)
                    with open(checkpoint_file, "wb") as fp:
                        pickle.dump(checkpoint_data, fp)

                checkpoint = Checkpoint.from_directory(directory)
                train.report(metrics, checkpoint=checkpoint)
        else:
            train.report(metrics)
        time.sleep(sleep_time)


@click.command()
@click.argument("bucket", type=str)
def main(bucket):
    tuner = tune.Tuner(
        function_trainable,
        param_space={
            "sleep_time": 30,
            "score": 0.5,
            "checkpoint_num_files": 16,
            "checkpoint_size_b": int(10**8),
            "checkpoint_iters": 10,
        },
        run_config=RunConfig(
            callbacks=[ProgressCallback()],
            sync_config=tune.SyncConfig(upload_dir=bucket),
            checkpoint_config=CheckpointConfig(num_to_keep=2),
        ),
    )
    tuner.fit()


if __name__ == "__main__":
    main()
