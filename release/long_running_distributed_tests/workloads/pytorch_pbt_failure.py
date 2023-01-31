import argparse
import numpy as np
import sys
import time
from typing import List, TYPE_CHECKING

import ray
from ray import tune
from ray._private.test_utils import safe_write_to_results_json
from ray.air.config import RunConfig, ScalingConfig, FailureConfig
from ray.train.examples.pytorch.tune_cifar_torch_pbt_example import train_func
from ray.train.torch import TorchConfig, TorchTrainer
from ray.tune.callback import Callback
from ray.tune.schedulers import PopulationBasedTraining
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner
from ray.tune.utils.mock import FailureInjectorCallback
from ray.tune.utils.release_test_util import ProgressCallback

if TYPE_CHECKING:
    from ray.tune.experiment import Trial
    from ray.air._internal.checkpoint_manager import _TrackedCheckpoint

"""Stress test trial save and restore in the following senses:

* Random nodes are being killed through callback. The checking freq is 90s
with a 10% chance of actually firing the killing command.
* PBT is at play here. The perturbation interval is every iteration.
Checkpoint freq is also every iteration.

Behavior description:

Only 2 out of the 3 trials can be run at any given time. Trials are paused
and restored/restored + mutated every iteration. So that no trials will be
starving.
After all trials have reached certain iteration(say 5), we don't expect to
see any trial to go back to iteration1 again(meaning they start from fresh).
After one hour, all trials should at least reach iteration 10.

This is a necessary but not sufficient assertion (adding more later).

This test also dumps checkpoint_ids in chronological order for all trials.
This should show up in the release test's artifact tab.

Last updated by xwjiang@ on 1/31/2023.
"""


parser = argparse.ArgumentParser()
parser.add_argument(
    "--smoke-test",
    action="store_true",
    default=False,
    help="Finish quickly for training.",
)
args = parser.parse_args()

ray.init(address="auto" if not args.smoke_test else None, log_to_driver=True)
num_training_workers = 1 if args.smoke_test else 3

trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(
        num_workers=num_training_workers,
        use_gpu=not args.smoke_test,
    ),
    torch_config=TorchConfig(backend="gloo"),
)


pbt_scheduler = PopulationBasedTraining(
    time_attr="training_iteration",
    perturbation_interval=1,
    hyperparam_mutations={
        "train_loop_config": {
            # distribution for resampling
            "lr": lambda: np.random.uniform(0.001, 1),
            # allow perturbations within this set of categorical values
            "momentum": [0.8, 0.9, 0.99],
        }
    },
)

_NO_RESTORE_FROM_FRESH_CKPT_ID_THRESHOLD = 5

_RESTORE_FROM_FRESH_ERR_MSG = """
All trials have at least reached checkpoint id 5. Not expecting any trial to
go back to checkpoint id 0 again. This error could indicate an incorrect checkpoint
and restore behavior.
"""

_CKPT_ID_10 = 10
_TIME_TO_REACH_CKPT_ID_10_IN_S = 3600

_NOT_REACH_CKPT_ID_10_ERR_MSG = """
Expecting to see all trials to reach a minimal checkpoint id of 10 by 1 hour.
"""


class InspectCheckpointIdCallback(Callback):
    """A callback for asserting the right checkpoint restore behavior.

    Also outputs an artifact that dumps `self._checkpoints` to `/tmp/ckpt_ids.txt`.
    """

    def __init__(self, trial_num: int):
        self._trial_num = trial_num
        # `trial_id` to a list of checkpoint ids in chronological order.
        self._checkpoints = dict()
        # If this condition is True, no trials are expected to restore from fresh.
        # Once it's on, it stays on.
        self._no_restore_from_fresh = False
        self._start_time = time.monotonic()
        # If this condition is True, all trials have reached a minimal ckpt id of 10.
        # Once it's on, it stays on.
        self._reached_ckpt_id_10 = False

        # TODO: make it configurable.
        self._output_path = "/tmp/ckpt_ids.txt"

    def _get_minimal_ckpt_id_for_all_trials(self) -> int:
        """Get the max checkpoint id for every trial, and return the minimal of them.

        If any trial does not have even one checkpoint coming in, return -1.
        """
        if len(self._checkpoints.keys()) < self._trial_num:
            return -1
        return min([max(ckpt_ids) for ckpt_ids in self._checkpoints.values()])

    def on_checkpoint(
        self,
        iteration: int,
        trials: List["Trial"],
        trial: "Trial",
        checkpoint: "_TrackedCheckpoint",
        **kwargs
    ):
        if trial.trial_id not in self._checkpoints:
            self._checkpoints[trial.trial_id] = []
        self._checkpoints[trial.trial_id].append(checkpoint.id)
        if not self._no_restore_from_fresh or not self._reached_ckpt_id_10:
            min_ckpt_id = self._get_minimal_ckpt_id_for_all_trials()
            if min_ckpt_id >= _NO_RESTORE_FROM_FRESH_CKPT_ID_THRESHOLD:
                self._no_restore_from_fresh = True
            if min_ckpt_id >= _CKPT_ID_10:
                self._reached_ckpt_id_10 = True
        if checkpoint.id == 0 and self._no_restore_from_fresh:
            raise RuntimeError(_RESTORE_FROM_FRESH_ERR_MSG)
        if (
            time.monotonic() - self._start_time > _TIME_TO_REACH_CKPT_ID_10_IN_S
            and not self._reached_ckpt_id_10
        ):
            raise RuntimeError(_NOT_REACH_CKPT_ID_10_ERR_MSG)
        # ``on_checkpoint`` is not very often. So we can afford to it for every call.
        safe_write_to_results_json(self._checkpoints, self._output_path)


tuner = Tuner(
    trainer,
    param_space={
        "train_loop_config": {
            "lr": tune.choice([0.001, 0.01, 0.1]),
            "momentum": 0.8,
            "test_mode": args.smoke_test,
            "batch_size": 128 * num_training_workers,
            # For the long running test, we want the training to run forever,
            # and it will be terminated by the release test infra.
            "epochs": 1 if args.smoke_test else sys.maxsize,
        }
    },
    tune_config=TuneConfig(
        num_samples=4, metric="loss", mode="min", scheduler=pbt_scheduler
    ),
    run_config=RunConfig(
        name="torch_pbt_failure",
        stop={"training_iteration": 1} if args.smoke_test else None,
        failure_config=FailureConfig(max_failures=-1),
        callbacks=[
            FailureInjectorCallback(time_between_checks=90),
            ProgressCallback(),
            InspectCheckpointIdCallback(3),
        ],
    ),
)

results = tuner.fit()

print(results.get_best_result(metric="loss", mode="min"))
