from dataclasses import dataclass
from typing import Dict, Any, Optional, List

from ray.util import PublicAPI

from ray.tune.trainable import PlacementGroupFactory
from ray.tune.callback import Callback


ScalingConfig = Dict[str, Any]


@dataclass
class ScalingConfigDataClass:
    """Configuration for scaling training.

    This is the schema for the scaling_config dict, and after beta, this will be the
    actual representation for Scaling config objects.

    trainer_resources: Resources to allocate for the trainer. If none is provided,
        will default to 1 CPU.
    num_workers: The number of workers (Ray actors) to launch.
        Each worker will reserve 1 CPU by default. The number of CPUs
        reserved by each worker can be overridden with the
        ``resources_per_worker`` argument.
    use_gpu: If True, training will be done on GPUs (1 per worker).
        Defaults to False. The number of GPUs reserved by each
        worker can be overridden with the ``resources_per_worker``
        argument.
    resources_per_worker: If specified, the resources
        defined in this Dict will be reserved for each worker. The
        ``CPU`` and ``GPU`` keys (case-sensitive) can be defined to
        override the number of CPU/GPUs used by each worker.
    placement_strategy: The placement strategy to use for the
        placement group of the Ray actors. See :ref:`Placement Group
        Strategies <pgroup-strategy>` for the possible options.
    """

    trainer_resources: Optional[Dict] = None
    num_workers: Optional[int] = None
    use_gpu: bool = False
    resources_per_worker: Optional[Dict] = None
    placement_strategy: str = "PACK"

    def __post_init__(self):
        self.resources_per_worker = (
            self.resources_per_worker if self.resources_per_worker else {}
        )
        if self.resources_per_worker:
            if not self.use_gpu and self.num_gpus_per_worker > 0:
                raise ValueError(
                    "`use_gpu` is False but `GPU` was found in "
                    "`resources_per_worker`. Either set `use_gpu` to True or "
                    "remove `GPU` from `resources_per_worker."
                )

            if self.use_gpu and self.num_gpus_per_worker == 0:
                raise ValueError(
                    "`use_gpu` is True but `GPU` is set to 0 in "
                    "`resources_per_worker`. Either set `use_gpu` to False or "
                    "request a positive number of `GPU` in "
                    "`resources_per_worker."
                )

    @property
    def num_cpus_per_worker(self):
        """The number of CPUs to set per worker."""
        return self.resources_per_worker.get("CPU", 1)

    @property
    def num_gpus_per_worker(self):
        """The number of GPUs to set per worker."""
        return self.resources_per_worker.get("GPU", int(self.use_gpu))

    @property
    def additional_resources_per_worker(self):
        """Resources per worker, not including CPU or GPU resources."""
        return {
            k: v
            for k, v in self.resources_per_worker.items()
            if k not in ["CPU", "GPU"]
        }

    def as_placement_group_factory(self) -> PlacementGroupFactory:
        """Returns a PlacementGroupFactory to specify resources for Tune."""
        trainer_resources = (
            self.trainer_resources if self.trainer_resources else {"CPU": 1}
        )
        trainer_bundle = [trainer_resources]
        worker_resources = {
            "CPU": self.num_cpus_per_worker,
            "GPU": self.num_gpus_per_worker,
        }
        worker_resources_extra = (
            {} if self.resources_per_worker is None else self.resources_per_worker
        )
        worker_bundles = [
            {**worker_resources, **worker_resources_extra}
            for _ in range(self.num_workers if self.num_workers else 0)
        ]
        bundles = trainer_bundle + worker_bundles
        return PlacementGroupFactory(bundles, strategy=self.placement_strategy)


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
