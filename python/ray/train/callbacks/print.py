import json
from typing import Dict, List

from ray.train.callbacks import TrainingCallback


class PrintCallback(TrainingCallback):
    """A callback that prints training results to STDOUT.

    Example:
        >>> # Handle results from two workers.
        >>> callback = PrintCallback()
        >>> results = [{"loss": 2.290156}, {"loss": 2.275099}]
        >>> callback.handle_result(results)
        [
            {
                "loss": 2.290156
            },
            {
                "loss": 2.275099
            }
        ]
    """

    def handle_result(self, results: List[Dict], **info):
        """Prints results to STDOUT.

        Args:
            results (List[Dict]): List of results from the training
                function. Each value in the list corresponds to the output of
                the training function from each worker.
            **info: kwargs dict for forward compatibility.
        """
        print(json.dumps(results, indent=4))