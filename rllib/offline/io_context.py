import os

from ray.rllib.utils.annotations import PublicAPI
from typing import Any


@PublicAPI
class IOContext:
    """Attributes to pass to input / output class constructors.

    RLlib auto-sets these attributes when constructing input / output classes.

    Attributes:
        log_dir (str): Default logging directory.
        config (dict): Configuration of the agent.
        worker_index (int): When there are multiple workers created, this
            uniquely identifies the current worker.
        worker (RolloutWorker): RolloutWorker object reference.
    """

    @PublicAPI
    def __init__(self,
                 log_dir: str = None,
                 config: dict = None,
                 worker_index: int = 0,
                 worker: Any = None):
        self.log_dir = log_dir or os.getcwd()
        self.config = config or {}
        self.worker_index = worker_index
        self.worker = worker

    @PublicAPI
    def default_sampler_input(self) -> Any:
        return self.worker.sampler
