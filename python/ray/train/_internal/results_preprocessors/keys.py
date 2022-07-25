from typing import List, Dict, Optional, Iterable
import warnings

from ray.train._internal.results_preprocessors.preprocessor import (
    ResultsPreprocessor,
    _deprecation_msg,
)
from ray.util.annotations import Deprecated


@Deprecated
class ExcludedKeysResultsPreprocessor(ResultsPreprocessor):
    """Preprocesses each result dictionary by excluding specified keys.

    Example:

    - excluded_keys: ``["a"]``
    - input: ``[{"a": 1, "b": 2}, {"a": 3, "b": 4}]``
    - output: ``[{"b": 2}, {"b": 4}]``

    Args:
        excluded_keys (Optional[Iterable[str]]): The keys to remove. If
            ``None`` then no keys will be removed.
    """

    def __init__(self, excluded_keys: Optional[Iterable[str]] = None) -> None:
        warnings.warn(
            _deprecation_msg,
            DeprecationWarning,
            stacklevel=2,
        )
        self.excluded_keys = set(excluded_keys) or {}

    def preprocess(self, results: List[Dict]) -> List[Dict]:
        new_results = [
            {
                key: value
                for key, value in result.items()
                if key not in self.excluded_keys
            }
            for result in results
        ]

        return new_results
