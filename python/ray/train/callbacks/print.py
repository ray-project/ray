import json
from typing import Dict, List, Optional

from ray.train.callbacks import TrainingCallback
from ray.train.callbacks.results_preprocessors import ResultsPreprocessor
from ray.train.callbacks.results_preprocessors.preprocessor import (
    SequentialResultsPreprocessor,
)
from ray.train.utils import ResultsList


class PrintCallback(TrainingCallback):
    """A callback that prints training results to STDOUT.

    Example:

        >>> from ray import train
        >>> from ray.train import Trainer
        >>> from ray.train.callbacks import PrintCallback
        >>> def train_func():
        ...    for i in range(2):
        ...        train.report(worker_idx=train.world_rank(), epoch=i)
        >>> trainer = Trainer(num_workers=2, backend="torch")
        >>> trainer.start()
        >>> trainer.run(train_func, callbacks=[PrintCallback()])
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

    def __init__(
        self, results_preprocessors: Optional[List[ResultsPreprocessor]] = None
    ):
        if results_preprocessors:
            self.results_preprocessor = SequentialResultsPreprocessor(
                results_preprocessors
            )

    def handle_result(self, results: List[Dict], **info):
        """Prints results to STDOUT.

        Args:
            results (List[Dict]): List of results from the training
                function. Each value in the list corresponds to the output of
                the training function from each worker.
            **info: kwargs dict for forward compatibility.
        """

        output = list(results)

        if isinstance(results, ResultsList):
            output.append(results.aggregated_results)

        print(json.dumps(output, indent=4))
