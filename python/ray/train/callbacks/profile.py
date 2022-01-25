import logging
import tempfile
from pathlib import Path
from typing import List, Dict, Optional, Union

from torch.profiler import profile

from ray import train
from ray.train import TrainingCallback
from ray.train.callbacks.logging import TrainCallbackLogdirManager
from ray.train.callbacks.results_preprocessors import IndexedResultsPreprocessor
from ray.train.constants import PROFILER_KEY

logger = logging.getLogger(__name__)

WORKER_TRACE_DIR_NAME = "pytorch_profiler_worker_traces"
DRIVER_TRACE_DIR_NAME = "pytorch_profiler"


class TorchWorkerProfiler():
    def __init__(self, trace_dir: Optional[str] = None):
        default_dir = Path(
            tempfile.gettempdir()).joinpath(WORKER_TRACE_DIR_NAME)
        trace_dir = trace_dir or default_dir
        self.trace_dir = Path(trace_dir)
        self.trace_dir.mkdir(parents=True, exist_ok=True)
        # Accumulated traces.
        self.profiler_trace_filenames = []

    def trace_handler(self, p: profile):
        trace_filename = \
            f"worker_{train.world_rank()}_epoch_{p.step_num}.pt.trace.json"
        trace_path = self.trace_dir.joinpath(trace_filename)

        logger.debug(f"Writing worker trace to {trace_path}.")
        p.export_chrome_trace(str(trace_path))

        self.profiler_trace_filenames.append(trace_filename)

    def get_and_clear_profile_traces(self):
        def get_trace(filename):
            trace_path = self.trace_dir.joinpath(filename)
            return trace_path.read_text()

        traces = [(trace_filename, get_trace(trace_filename))
                  for trace_filename in self.profiler_trace_filenames]

        self.profiler_trace_files = []
        return {PROFILER_KEY: traces}


class TorchTensorboardProfilerCallback(TrainingCallback):
    RESERVED_KEYS = [PROFILER_KEY]

    def __init__(
            self,
            logdir: Optional[str] = None,
            workers_to_log: Optional[Union[int, List[int]]] = None) -> None:
        super().__init__()
        self._logdir = logdir
        self._logdir_manager = TrainCallbackLogdirManager(logdir=logdir)
        self.results_preprocessor = IndexedResultsPreprocessor(
            indices=workers_to_log)

    def start_training(self, logdir: str, **info):
        default_logdir = Path(logdir).joinpath(DRIVER_TRACE_DIR_NAME)
        self._logdir_manager.setup_logdir(default_logdir=default_logdir)

    def handle_result(self, results: List[Dict], **info):
        for result in results:
            if PROFILER_KEY in result and result[PROFILER_KEY]:
                profile_traces = result[PROFILER_KEY]
                for (name, data) in profile_traces:
                    path = self._logdir_manager.logdir_path.joinpath(name)
                    with path.open("w") as f:
                        f.write(data)
