import json
from typing import Dict, List
import warnings

from ray.train.callbacks import TrainingCallback
from ray.train.callbacks.callback import _deprecation_msg
from ray.util.annotations import Deprecated


@Deprecated
class PrintCallback(TrainingCallback):
    """A callback that prints training results to STDOUT.

    Example:

        >>> from ray import train
        >>> from ray.train import Trainer
        >>> from ray.train.callbacks import PrintCallback
        >>> def train_func():
        ...    for i in range(2):
        ...        train.report(worker_idx=train.world_rank(), epoch=i)
        >>> trainer = Trainer(num_workers=2, backend="torch") # doctest: +SKIP
        >>> trainer.start() # doctest: +SKIP
        >>> trainer.run(train_func, callbacks=[PrintCallback()]) # doctest: +SKIP
        [
            {
                "worker_idx": 0,
                "epoch": 0,
                "_timestamp": 1641232964,
                "_time_this_iter_s": 0.0021491050720214844,
                "_training_iteration": 1
            },
            {
                "worker_idx": 1,
                "epoch": 0,
                "_timestamp": 1641232964,
                "_time_this_iter_s": 0.0013790130615234375,
                "_training_iteration": 1
            }
        ]
        [
            {
                "worker_idx": 0,
                "epoch": 1,
                "_timestamp": 1641232964,
                "_time_this_iter_s": 0.0025370121002197266,
                "_training_iteration": 2
            },
            {
                "worker_idx": 1,
                "epoch": 1,
                "_timestamp": 1641232964,
                "_time_this_iter_s": 0.002299070358276367,
                "_training_iteration": 2
            }
        ]
    """

    def __init__(self) -> None:
        warnings.warn(
            _deprecation_msg,
            DeprecationWarning,
        )

    def handle_result(self, results: List[Dict], **info):
        """Prints results to STDOUT.

        Args:
            results (List[Dict]): List of results from the training
                function. Each value in the list corresponds to the output of
                the training function from each worker.
            **info: kwargs dict for forward compatibility.
        """
        print(json.dumps(results, indent=4))
