from typing import Iterable, List, Optional, Dict, Union
import abc

import json
from pathlib import Path

from ray.util.ml_utils.json import SafeFallbackEncoder
from ray.util.sgd.v2.callbacks import SGDCallback
from ray.util.sgd.v2.constants import RESULT_FILE_JSON


class SGDSingleFileLoggingCallback(SGDCallback, metaclass=abc.ABCMeta):
    """Abstract SGD logging callback class.

    Args:
        logdir (str): Path to directory where the results file should be.
        filename (str|None): Filename in logdir to save results to.
        workers_to_log (int|List[int]|None): Worker indices to log.
            If None, will log all workers.
    """

    # Defining it like this ensures it will be overwritten
    # in a subclass - otherwise an exception will be raised
    _default_filename: Union[str, Path]

    def __init__(self,
                 logdir: str,
                 filename: Optional[str] = None,
                 workers_to_log: Optional[Union[int, List[int]]] = 0) -> None:
        logdir_path = Path(logdir)

        if not logdir_path.is_dir():
            raise ValueError(f"logdir '{logdir}' must be a directory.")

        if filename is None:
            filename = self._default_filename

        self._log_path = logdir_path.joinpath(Path(filename))
        if isinstance(workers_to_log, int):
            workers_to_log = [workers_to_log]

        if workers_to_log is not None:
            if not isinstance(workers_to_log, Iterable):
                raise TypeError("workers_to_log must be an Iterable, got "
                                f"{type(workers_to_log)}.")
            if not all(isinstance(worker, int) for worker in workers_to_log):
                raise TypeError(
                    "All elements of workers_to_log must be integers.")

        self._workers_to_log = workers_to_log

    @property
    def log_path(self) -> Path:
        """Path to the log file."""
        return self._log_path

    def start_training(self):
        # Create a JSON file with an empty list
        # that will be latter appended to
        with open(self._log_path, "w") as f:
            json.dump([], f, cls=SafeFallbackEncoder)


class JsonLoggerCallback(SGDSingleFileLoggingCallback):
    """Logs SGD results in json format.

    Args:
        logdir (str): Path to directory where the results file should be.
        filename (str|None): Filename in logdir to save results to.
            Defaults to "results.json".
        workers_to_log (int|List[int]|None): Worker indices to log.
            If None, will log all workers.
    """

    _default_filename: Union[str, Path] = RESULT_FILE_JSON

    def handle_result(self, results: Optional[List[Dict]]):
        if self._workers_to_log is None or results is None:
            results_to_log = results
        else:
            results_to_log = [
                result for i, result in enumerate(results)
                if i in self._workers_to_log
            ]
        with open(self._log_path, "r+") as f:
            loaded_results = json.load(f)
            f.seek(0)
            json.dump(
                loaded_results + [results_to_log], f, cls=SafeFallbackEncoder)
