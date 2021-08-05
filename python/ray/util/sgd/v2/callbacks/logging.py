from typing import List, Optional, Dict, Union
import abc

import json
from pathlib import Path

from ray.util.ml_utils.json import SafeFallbackEncoder
from ray.util.sgd.v2.callbacks import SGDCallback
from ray.util.sgd.v2.constants import RESULT_FILE_JSON


class SGDLoggingCallback(SGDCallback, metaclass=abc.ABCMeta):
    def __init__(
            self,
            logdir: str,
            filename: str = RESULT_FILE_JSON,
            workers_to_log: Optional[Union[int, List[int]]] = None) -> None:
        self._log_path = Path(logdir, filename)
        if isinstance(workers_to_log, int):
            workers_to_log = [workers_to_log]
        self._workers_to_log = workers_to_log
        with open(self._log_path, "w") as f:
            json.dump([], f, cls=SafeFallbackEncoder)


class JsonLoggerCallback(SGDLoggingCallback):
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
            json.dump(
                loaded_results + [results_to_log], f, cls=SafeFallbackEncoder)
