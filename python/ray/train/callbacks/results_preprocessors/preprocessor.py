import abc
from typing import List, Dict

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class ResultsPreprocessor(abc.ABC):
    """Abstract class for preprocessing Train results."""

    @abc.abstractmethod
    def preprocess(self, results: List[Dict]) -> List[Dict]:
        """

        Args:
            results (List[Dict]): List of results from the training
                function. Each value in the list corresponds to the output of
                the training function from each worker.

        Returns:
            A list of dictionaries. Each item in the list does *not*
            need to correspond to a single worker, and it is expected for
            the corresponding caller to understand the semantics of the
            preprocessed results.

        """
        return results


@DeveloperAPI
class SequentialResultsPreprocessor(ResultsPreprocessor):
    """A processor that sequentially runs a series of preprocessing steps.

    - preprocessors: ``[A, B, C]``
    - preprocess: ``C.preprocess(B.preprocess(A.preprocess(results)``

    Args:
        preprocessors (List[ResultsPreprocessor]): The preprocessors that
            will be run in sequence.

    """

    def __init__(self, preprocessors: List[ResultsPreprocessor]):
        self.preprocessors = preprocessors

    def preprocess(self, results: List[Dict]) -> List[Dict]:
        for preprocessor in self.preprocessors:
            results = preprocessor.preprocess(results)
        return results
