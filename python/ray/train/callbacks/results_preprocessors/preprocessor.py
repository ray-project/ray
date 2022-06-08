import abc
import warnings
from typing import List, Dict

from ray.util.annotations import Deprecated

_deprecation_msg = (
    "`ray.train.callbacks.results_preprocessors` and the `ray.train.Trainer` API are "
    "deprecated in Ray "
    "2.0, and is replaced by Ray AI Runtime (Ray AIR). Ray AIR "
    "(https://docs.ray.io/en/latest/ray-air/getting-started.html) "
    "will provide greater functionality and a unified API "
    "compared to the current Ray Train API. "
    "This class will be removed in the future."
)


@Deprecated
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


@Deprecated
class SequentialResultsPreprocessor(ResultsPreprocessor):
    """A processor that sequentially runs a series of preprocessing steps.

    - preprocessors: ``[A, B, C]``
    - preprocess: ``C.preprocess(B.preprocess(A.preprocess(results)``

    Args:
        preprocessors (List[ResultsPreprocessor]): The preprocessors that
            will be run in sequence.

    """

    def __init__(self, preprocessors: List[ResultsPreprocessor]):
        warnings.warn(
            "The `ray.train.results_preprocessors` API is deprecated in Ray " "2.0",
            DeprecationWarning,
            stacklevel=2,
        )
        self.preprocessors = preprocessors

    def preprocess(self, results: List[Dict]) -> List[Dict]:
        for preprocessor in self.preprocessors:
            results = preprocessor.preprocess(results)
        return results
