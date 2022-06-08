import warnings
from typing import List, Dict, Iterable, Union, Optional

from ray.train._internal.results_preprocessors.preprocessor import (
    ResultsPreprocessor,
    _deprecation_msg,
)
from ray.util.annotations import Deprecated


@Deprecated
class IndexedResultsPreprocessor(ResultsPreprocessor):
    """Preprocesses results by filtering by index.

    Example:

    - indices: ``[0, 2]``
    - input: ``[a, b, c, d]``
    - output: ``[a, c]``

    Args:
        indices(Optional[int|List[int]]): The indices of the results to return.
            If ``None``, then all results will be returned (no-op).
    """

    def __init__(self, indices: Optional[Union[int, List[int]]]) -> None:
        warnings.warn(
            _deprecation_msg,
            DeprecationWarning,
            stacklevel=2,
        )
        self._indices = self._validate_indices(indices)

    def _validate_indices(self, indices) -> Optional[List[int]]:
        if indices is None:
            return None

        if isinstance(indices, int):
            return [indices]

        if not isinstance(indices, Iterable):
            raise TypeError("indices must be an Iterable, got " f"{type(indices)}.")
        indices = list(indices)

        if len(indices) < 1:
            raise ValueError("At least one index must be specified in indices.")

        if not all(isinstance(index, int) for index in indices):
            raise TypeError("All elements of indices must be integers.")

        return indices

    def preprocess(self, results: List[Dict]) -> List[Dict]:
        if self._indices is None:
            return results

        filtered_results = [results[i] for i in self._indices]
        return filtered_results
