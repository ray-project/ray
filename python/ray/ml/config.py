from dataclasses import dataclass
from typing import Dict, Any, TypedDict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ray.tune.trainable import PlacementGroupFactory

# Right now, RunConfig is just an arbitrary dict that specifies tune.run
# kwargs.
# TODO(xwjiang): After Tuner is implemented, define the schema
RunConfig = Dict[str, Any]


class ScalingConfig(TypedDict):
    """Configuration for scaling data parallel training.

    num_workers (int): The number of workers (Ray actors) to launch.
        Each worker will reserve 1 CPU by default. The number of CPUs
        reserved by each worker can be overridden with the
        ``resources_per_worker`` argument.
    use_gpu (bool): If True, training will be done on GPUs (1 per
        worker). Defaults to False. The number of GPUs reserved by each
        worker can be overridden with the ``resources_per_worker``
        argument.
    resources_per_worker (Optional[Dict]): If specified, the resources
        defined in this Dict will be reserved for each worker. The
        ``CPU`` and ``GPU`` keys (case-sensitive) can be defined to
        override the number of CPU/GPUs used by each worker.
    max_retries (int): Number of retries when Ray actors fail.
        Defaults to 3. Set to -1 for unlimited retries.
    placement_strategy (str): The placement strategy to use for the
        placement group of the Ray actors.
    """

    num_workers: int
    use_gpu: bool
    resources_per_worker: Optional[Dict]
    max_retries: int
    placement_strategy: str


@dataclass
class _ScalingConfigDataClass:
    """Internal dataclass for ScalingConfig."""

    num_workers: int
    use_gpu: bool = False
    resources_per_worker: Optional[Dict] = None
    max_retries: int = 3
    placement_strategy: str = "PACK"

    def __post_init__(self):
        raise NotImplementedError

    @property
    def num_cpus_per_worker(self):
        """The number of CPUs to set per worker."""
        raise NotImplementedError

    @property
    def num_gpus_per_worker(self):
        """The number of GPUs to set per worker."""
        raise NotImplementedError

    @property
    def additional_resources_per_worker(self):
        """Resources per worker, not including CPU or GPU resources."""
        raise NotImplementedError

    def get_placement_group_factory(self) -> "PlacementGroupFactory":
        """Returns a PlacementGroupFactory to specify resources for Tune."""
        raise NotImplementedError
