import logging
from pathlib import Path
from typing import List, Dict, Optional, Union

from ray.train.callbacks import TrainingCallback
from ray.train.callbacks.logging import TrainCallbackLogdirManager
from ray.train.callbacks.results_preprocessors import IndexedResultsPreprocessor
from ray.train.constants import PYTORCH_PROFILER_KEY

logger = logging.getLogger(__name__)

DRIVER_TRACE_DIR_NAME = "pytorch_profiler"


class TorchTensorboardProfilerCallback(TrainingCallback):
    """Synchronizes PyTorch Profiler traces onto disk.

    This should typically be used in conjunction with ``TorchWorkerProfiler``,
    though the actual requirement is for the ``_train_torch_profiler`` key
    to be populated in the results from ``train.report()``.

    Args:
        logdir (Optional[str]): The directory to store traces. If ``None``,
            this will use a default temporary dir.
        workers_to_log (Optional[int|List[int]]): Worker indices to log.
            If ``None``, will log all workers. By default, this will log all
            workers.
    """

    RESERVED_KEYS = [PYTORCH_PROFILER_KEY]

    def __init__(
        self,
        logdir: Optional[str] = None,
        workers_to_log: Optional[Union[int, List[int]]] = None,
    ) -> None:
        super().__init__()
        self._logdir = logdir
        self._logdir_manager = TrainCallbackLogdirManager(logdir=logdir)
        self.results_preprocessor = IndexedResultsPreprocessor(indices=workers_to_log)

    def start_training(self, logdir: str, **info):
        default_logdir = Path(logdir).joinpath(DRIVER_TRACE_DIR_NAME)
        self._logdir_manager.setup_logdir(default_logdir=default_logdir)

    def handle_result(self, results: List[Dict], **info):
        for result in results:
            if PYTORCH_PROFILER_KEY in result and result[PYTORCH_PROFILER_KEY]:
                profile_traces = result[PYTORCH_PROFILER_KEY]
                for (name, data) in profile_traces:
                    path = self._logdir_manager.logdir_path.joinpath(name)
                    with path.open("w") as f:
                        f.write(data)
