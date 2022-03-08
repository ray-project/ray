from dataclasses import dataclass
from typing import Any, Dict, Optional

from ray.tune import PlacementGroupFactory


class ScalingConfig:
    """Configuration for scaling training."""
    pass

@dataclass
class DataParallelScalingConfig:
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
    use_gpu: bool = False
    resources_per_worker: Optional[Dict] = None
    max_retries: int = 3
    placement_strategy: str = "PACK"

    def __post_init__(self):
        self.resources_per_worker = self.resources_per_worker if \
            self.resources_per_worker else {}

        if self.num_workers < 0:
            raise ValueError("`num_workers` must be a positive integer.")

        if self.resources_per_worker:
            # Override CPU and GPU resources and remove from dict.
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
        return self.resources_per_worker.get("CPU", 1)

    @property
    def num_gpus_per_worker(self):
        return self.resources_per_worker.get("GPU", int(self.use_gpu))

    @property
    def additional_resources_per_worker(self):
        """Resources per worker, not including CPU or GPU resources."""
        return {k: v for k, v in self.resources_per_worker.items() if k not
                in ["CPU", "GPU"]}

    def get_placement_group_factory(self) -> PlacementGroupFactory:
        """Returns a PlacementGroupFactory to specify resources for Tune."""
        trainer_bundle = [{"CPU": 1}]
        worker_resources = {"CPU": self.num_cpus_per_worker, "GPU": self.num_gpus_per_worker}
        worker_resources_extra = (
            {} if self.resources_per_worker is None else
            self.resources_per_worker
        )
        worker_bundles = [
            {**worker_resources, **worker_resources_extra}
            for _ in range(self.num_workers)
        ]
        bundles = trainer_bundle + worker_bundles
        return PlacementGroupFactory(bundles, strategy=self.placement_strategy)


# Right now, RunConfig is just an arbitrary dict that specifies tune.run
# kwargs.
# TODO: After Tuner is implemented, make this into an actual dataclass
RunConfig = Dict[str, Any]



