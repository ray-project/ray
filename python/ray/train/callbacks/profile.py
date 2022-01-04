import logging
import pathlib
from typing import List, Dict, Optional, Union

from torch.profiler import profile, record_function, schedule

from ray import train
from ray.train import TrainingCallback
from ray.train.callbacks.logging import TrainingLogdirMixin
from ray.train.callbacks.results_prepocessors import KeysResultsPreprocessor, \
    WorkersResultsPreprocessor
from ray.train.constants import PROFILER_KEY

logger = logging.getLogger(__name__)

WORKER_TRACE_DIR = "worker_traces"
DRIVER_TRACE_DIR = "pytorch_profiler"


class TorchWorkerProfiler():
    def __init__(self, trace_dir: Optional[str] = None):
        self.profiler_traces = []
        self.recorded_functions = {}

        trace_dir = trace_dir or WORKER_TRACE_DIR
        self.trace_dir = pathlib.Path(trace_dir)
        self.trace_dir.mkdir(parents=True, exist_ok=True)

        self.p = profile(
            activities=[],  # default activities
            schedule=schedule(wait=0, warmup=0, active=1),
            on_trace_ready=self.trace_handler)

    def trace_handler(self, p: profile):
        trace_filename = \
            f"worker_{train.world_rank()}_epoch_{p.step_num}.pt.trace.json"
        trace_path = self.trace_dir.joinpath(trace_filename)

        logger.debug(f"Writing worker trace to {trace_path}.")

        p.export_chrome_trace(str(trace_path))
        with trace_path.open() as f:
            data = f.read()

        trace = (trace_filename, data)
        self.profiler_traces.append(trace)

    def __enter__(self):
        self.p.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.p.__exit__(exc_type, exc_val, exc_tb)

    def step(self):
        self.p.step()

    def record_function_enter(self, function_name: str):
        func = record_function(function_name).__enter__()
        self.recorded_functions[function_name] = func

    def record_function_exit(self, function_name: str):
        func = self.recorded_functions.pop(function_name)
        func.__exit__(None, None, None)

    def get_and_clear_profile_traces(self):
        traces = self.profiler_traces
        self.profiler_traces = []
        return {PROFILER_KEY: traces}


class TorchTensorboardProfilerCallback(TrainingLogdirMixin, TrainingCallback):
    def __init__(self,
                 logdir: Optional[str] = None,
                 workers_to_log: Optional[Union[int, List[int]]] = 0) -> None:
        super().__init__()
        self._logdir = logdir
        self._results_preprocessors = [
            WorkersResultsPreprocessor(workers_to_log=workers_to_log),
            KeysResultsPreprocessor(included_keys=[PROFILER_KEY])
        ]

    def start_training(self, logdir: str, **info):
        super().start_training(logdir)
        self.logdir.joinpath(DRIVER_TRACE_DIR).mkdir()

    def handle_result(self, results: List[Dict], **info):
        for result in results:
            if PROFILER_KEY in result and result[PROFILER_KEY]:
                profile_traces = result[PROFILER_KEY]
                for (name, data) in profile_traces:
                    path = self.logdir.joinpath(DRIVER_TRACE_DIR, name)
                    with open(path, "w") as f:
                        f.write(data)
