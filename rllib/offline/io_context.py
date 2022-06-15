import os
from typing import Optional, TYPE_CHECKING

from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils.typing import AlgorithmConfigDict

if TYPE_CHECKING:
    from ray.rllib.evaluation.sampler import SamplerInput
    from ray.rllib.evaluation.rollout_worker import RolloutWorker


@PublicAPI
class IOContext:
    """Class containing attributes to pass to input/output class constructors.

    RLlib auto-sets these attributes when constructing input/output classes,
    such as InputReaders and OutputWriters.
    """

    @PublicAPI
    def __init__(
        self,
        log_dir: Optional[str] = None,
        config: Optional[AlgorithmConfigDict] = None,
        worker_index: int = 0,
        worker: Optional["RolloutWorker"] = None,
    ):
        """Initializes a IOContext object.

        Args:
            log_dir: The logging directory to read from/write to.
            config: The Algorithm's main config dict.
            worker_index: When there are multiple workers created, this
                uniquely identifies the current worker. 0 for the local
                worker, >0 for any of the remote workers.
            worker: The RolloutWorker object reference.
        """
        self.log_dir = log_dir or os.getcwd()
        self.config = config or {}
        self.worker_index = worker_index
        self.worker = worker

    @PublicAPI
    def default_sampler_input(self) -> Optional["SamplerInput"]:
        """Returns the RolloutWorker's SamplerInput object, if any.

        Returns None if the RolloutWorker has no SamplerInput. Note that local
        workers in case there are also one or more remote workers by default
        do not create a SamplerInput object.

        Returns:
            The RolloutWorkers' SamplerInput object or None if none exists.
        """
        return self.worker.sampler

    @property
    @PublicAPI
    def input_config(self):
        return self.config.get("input_config", {})

    @property
    @PublicAPI
    def output_config(self):
        return self.config.get("output_config", {})
