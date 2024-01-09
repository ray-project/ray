"""Run Tune with frequent pausing.
See context https://github.com/ray-project/ray/issues/34197.

m5.large node has memory of 7.2 GB. With `RAY_memory_usage_threshold=0.5`,
if the node's memory exceeds 3.6 GB, any new tasks would be killed.
Note this node memory is also shared by processes like ray dashboard etc.
Without ray object store reference leakage from application code, all these
background processes take less than 2 GB of memory all together.
With reference leakage, we reach 3.6 GB threshold within 5 minutes
at the time when this test was written.

success criteria: run through 10min without crash.

cost: A few dollars.
"""

import numpy as np
import os
import pickle
import tempfile

from ray import train
from ray.train import Checkpoint, RunConfig
from ray.tune.schedulers.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner


def func(config):
    starting_epoch = 0
    checkpoint = train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            with open(os.path.join(checkpoint_dir, "ckpt.pkl"), "rb") as f:
                checkpoint_dict = pickle.load(f)
        checkpoint_epoch = checkpoint_dict["epoch"]
        starting_epoch = checkpoint_epoch + 1

    for epoch in range(starting_epoch, 1000):
        checkpoint_dict = {"epoch": epoch, "large_data": np.zeros(10000000)}
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "ckpt.pkl"), "wb") as f:
                pickle.dump(checkpoint_dict, f)
            train.report({}, checkpoint=Checkpoint.from_directory(tmpdir))


class FrequentPausesScheduler(FIFOScheduler):
    def on_trial_result(self, tune_controller, trial, result):
        return TrialScheduler.PAUSE


tuner = Tuner(
    func,
    tune_config=TuneConfig(num_samples=2, scheduler=FrequentPausesScheduler()),
    run_config=RunConfig(storage_path="/mnt/cluster_storage", name="frequent_pausing"),
)

tuner.fit()
