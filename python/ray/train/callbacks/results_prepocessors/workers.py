from typing import List, Dict, Iterable, Optional, Union

from ray.train.callbacks.results_prepocessors.preprocessor import \
    ResultsPreprocessor


class WorkersResultsPreprocessor(ResultsPreprocessor):
    """Preprocesses results by worker."""

    def __init__(self,
                 workers_to_log: Optional[Union[int, List[int]]] = 0) -> None:
        self._workers_to_log = self._validate_workers_to_log(workers_to_log)

    def preprocess(self, results: List[Dict]) -> List[Dict]:
        if self._workers_to_log is None:
            return results

        results = [
            result for i, result in enumerate(results)
            if i in self._workers_to_log
        ]

        return results

    def _validate_workers_to_log(self, workers_to_log) -> Optional[List[int]]:
        if isinstance(workers_to_log, int):
            workers_to_log = [workers_to_log]

        if workers_to_log is not None:
            if not isinstance(workers_to_log, Iterable):
                raise TypeError("workers_to_log must be an Iterable, got "
                                f"{type(workers_to_log)}.")
            if not all(isinstance(worker, int) for worker in workers_to_log):
                raise TypeError(
                    "All elements of workers_to_log must be integers.")
            if len(workers_to_log) < 1:
                raise ValueError(
                    "At least one worker must be specified in workers_to_log.")
        return workers_to_log


class SingleWorkerResultsPreprocessor(WorkersResultsPreprocessor):
    def _validate_workers_to_log(self, workers_to_log) -> List[int]:
        worker_to_log = self._validate_worker_to_log(workers_to_log)
        return [worker_to_log]

    def _validate_worker_to_log(self, workers_to_log) -> int:
        if isinstance(workers_to_log, Iterable):
            workers_to_log = list(workers_to_log)
            if len(workers_to_log) > 1:
                raise ValueError(
                    f"{self.__class__.__name__} only supports logging "
                    "from a single worker.")
            elif len(workers_to_log) < 1:
                raise ValueError(
                    "At least one worker must be specified in workers_to_log.")
            workers_to_log = workers_to_log[0]
        if not isinstance(workers_to_log, int):
            raise TypeError("workers_to_log must be an integer.")
        return workers_to_log
