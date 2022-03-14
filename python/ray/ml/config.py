from dataclasses import dataclass
from typing import Dict, Any, Optional, List, TYPE_CHECKING

from ray.util import PublicAPI

if TYPE_CHECKING:
    from ray.tune.trainable import PlacementGroupFactory
    from ray.tune.callback import Callback


ScalingConfig = Dict[str, Any]


@dataclass
class _ScalingConfigDataClass:
    """Configuration for scaling training.

    This is the schema for the scaling_config dict, and after beta, this will be the
    actual representation for Scaling config objects.

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
    placement_strategy (str): The placement strategy to use for the
        placement group of the Ray actors. See :ref:`Placement Group
        Strategies <pgroup-strategy>` for the possible options.
    """

    num_workers: int
    use_gpu: bool = False
    resources_per_worker: Optional[Dict] = None
    placement_strategy: str = "PACK"

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


@PublicAPI(stability="alpha")
class FailureConfig:
    """Configuration related to failure handling of each run/trial.
    Args:
        max_failures: Tries to recover a run at least this many times.
            Will recover from the latest checkpoint if present.
            Setting to -1 will lead to infinite recovery retries.
            Setting to 0 will disable retries. Defaults to 0.
    """

    max_failures: int = 0


@dataclass
@PublicAPI(stability="alpha")
class RunConfig:
    """Runtime configuration for individual trials that are run.

    This contains information that applies to individual runs of Trainable classes.
    This includes both running a Trainable by itself or running a hyperparameter
    tuning job on top of a Trainable (applies to each trial).

    Args:
        name: Name of the trial or experiment. If not provided, will be deduced
            from the Trainable.
        local_dir: Local dir to save training results to.
            Defaults to ``~/ray_results``.
        callbacks: Callbacks to invoke.
            Refer to ray.tune.callback.Callback for more info.
    """

    # TODO(xwjiang): Clarify RunConfig behavior across resume. Is one supposed to
    #  reapply some of the args here? For callbacks, how do we enforce only stateless
    #  callbacks?
    # TODO(xwjiang): Add more.
    name: Optional[str] = None
    local_dir: Optional[str] = None
    callbacks: Optional[List[Callback]] = None
    failure: Optional[FailureConfig] = None
