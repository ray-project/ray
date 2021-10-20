import abc
from typing import List, Dict


class TrainingCallback(metaclass=abc.ABCMeta):
    """Abstract Train callback class."""

    def handle_result(self, results: List[Dict], **info):
        """Called every time train.report() is called.

        Args:
            results (List[Dict]): List of results from the training
                function. Each value in the list corresponds to the output of
                the training function from each worker.
            **info: kwargs dict for forward compatibility.
        """
        pass

    def start_training(self, logdir: str, **info):
        """Called once on training start.

        Args:
            logdir (str): Path to the file directory where logs
                should be persisted.
            **info: kwargs dict for forward compatibility.
        """
        pass

    def finish_training(self, error: bool = False, **info):
        """Called once after training is over.

        Args:
            error (bool): If True, there was an exception during training.
            **info: kwargs dict for forward compatibility.
        """
        pass
