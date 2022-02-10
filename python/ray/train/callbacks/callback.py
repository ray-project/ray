import abc
from typing import List, Dict

from ray.train.callbacks.results_preprocessors import ResultsPreprocessor


class TrainingCallback(abc.ABC):
    """Abstract Train callback class."""

    results_preprocessor: ResultsPreprocessor = None

    def start_training(self, logdir: str, config: Dict, **info):
        """Called once on training start.

        Args:
            logdir (str): Path to the file directory where logs
                should be persisted.
            config (Dict): The config dict passed into ``trainer.run()``.
            **info: kwargs dict for forward compatibility.
        """
        pass

    def process_results(self, results: List[Dict], **info):
        """Called every time train.report() is called.

        1. Preprocesses results. Subclasses can implement preprocessing by
           defining a ``ResultsPreprocessor``.
        2. Handles preprocessed results. Subclasses can implement handling by
           overriding the ``handle_result`` method.

        Args:
            results (List[Dict]): List of results from the training
                function. Each value in the list corresponds to the output of
                the training function from each worker.
            **info: kwargs dict for forward compatibility.
        """
        if self.results_preprocessor:
            results = self.results_preprocessor.preprocess(results)
        self.handle_result(results, **info)

    def handle_result(self, results: List[Dict], **info):
        """Called every time train.report() is called after preprocessing.

        For more information, see ``process_results``.

        Args:
            results (List[Dict]): List of results from the training
                function. Each value in the list corresponds to the output of
                the training function from each worker.
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
