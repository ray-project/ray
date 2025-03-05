import os
import time
from pathlib import Path

from ray import train, tune
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.tune.search import BasicVariantGenerator

# Hang full script until this marker is deleted
HANG_RUN_MARKER = os.environ.get("HANG_RUN_MARKER", "")

# Delete this marker when a trial is started
DELETE_TRIAL_MARKER = os.environ.get("DELETE_TRIAL_MARKER", "")

# Hang in trial until this marker is deleted
HANG_TRIAL_MARKER = os.environ.get("HANG_TRIAL_MARKER", "")

# Delete this marker after tuning finished
DELETE_RUN_MARKER = os.environ.get("DELETE_RUN_MARKER", "")

# Hang at end of run until this marker is deleted
HANG_END_MARKER = os.environ.get("HANG_END_MARKER", "")

# Report this val as the "fixed" metric in the trial.
# This value is captured in the trainer and will conflict when a trainable
# is overwritten!
FIXED_VAL = int(os.environ["FIXED_VAL"])

# Grid search over these vals and report as "param" metric in the trial.
# Even with conflicting trainables, these will be reported correctly as they
# are tracked by the driver, not the trainable.
VALS = [int(os.environ["VAL_1"]), int(os.environ["VAL_2"])]

# Wait for HANG_RUN_MARKER
while HANG_RUN_MARKER and Path(HANG_RUN_MARKER).exists():
    time.sleep(0.1)


def train_func(config):
    # Delete DELETE_TRIAL_MARKER
    delete_marker = config["delete_marker"]
    if delete_marker and Path(delete_marker).exists():
        Path(delete_marker).unlink()

    # Wait for HANG_TRIAL_MARKER
    hang_marker = config["hang_marker"]
    while hang_marker and Path(hang_marker).exists():
        time.sleep(0.1)

    # Finish trial
    train.report({"param": config["param"], "fixed": config["fixed"]})


if __name__ == "__main__":
    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func,
        train_loop_config={
            "fixed": FIXED_VAL,
        },
        scaling_config=train.ScalingConfig(
            num_workers=1, trainer_resources={"CPU": 0}, resources_per_worker={"CPU": 2}
        ),
    )

    tuner = tune.Tuner(
        trainer,
        param_space={
            "train_loop_config": {
                "param": tune.grid_search(VALS),
                "delete_marker": DELETE_TRIAL_MARKER,
                "hang_marker": HANG_TRIAL_MARKER,
            }
        },
        tune_config=tune.TuneConfig(search_alg=BasicVariantGenerator(max_concurrent=1)),
    )
    results = tuner.fit()

    # Delete DELETE_RUN_MARKER
    if DELETE_RUN_MARKER and Path(DELETE_RUN_MARKER).exists():
        Path(DELETE_RUN_MARKER).unlink()

    # Wait for HANG_END_MARKER
    while HANG_END_MARKER and Path(HANG_END_MARKER).exists():
        time.sleep(0.1)

    # Put assertions last, so we don't finish early because of failures
    assert sorted([result.metrics["param"] for result in results]) == VALS
    assert [result.metrics["fixed"] for result in results] == [FIXED_VAL, FIXED_VAL]
