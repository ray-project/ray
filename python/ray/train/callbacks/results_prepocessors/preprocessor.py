import abc
from typing import List, Dict


class ResultsPreprocessor(abc.ABC):
    """Abstract Train results preprocessor class."""

    def preprocess(self, results: List[Dict]) -> List[Dict]:
        return results
