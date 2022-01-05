import logging
from typing import List, Dict, Optional, Iterable

from ray.train.callbacks.results_prepocessors.preprocessor import \
    ResultsPreprocessor
from ray.train.constants import PROFILER_KEY

logger = logging.getLogger(__name__)

DEFAULT_EXCLUDED_KEYS = [PROFILER_KEY]


class KeysResultsPreprocessor(ResultsPreprocessor):
    """Preprocesses results by key."""

    def __init__(self,
                 included_keys: Optional[Iterable[str]] = None,
                 excluded_keys: Optional[Iterable[str]] = None) -> None:
        included_keys = included_keys or {}
        excluded_keys = excluded_keys or {}

        keys_to_exclude = [
            key for key in DEFAULT_EXCLUDED_KEYS if key not in included_keys
        ]
        for key in excluded_keys:
            if key in included_keys:
                logger.warning(
                    f"Found key {key} in both {included_keys} and {excluded_keys}."
                    f" This key will be included.")
            keys_to_exclude.append(key)

        self._keys_to_exclude = keys_to_exclude

    def preprocess(self, results: List[Dict]) -> List[Dict]:
        new_results = [{
            key: value
            for key, value in result.items()
            if key not in self._keys_to_exclude
        } for result in results]

        return new_results
