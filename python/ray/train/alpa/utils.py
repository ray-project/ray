from dataclasses import dataclass
from ray.util.annotations import PublicAPI
from typing import TYPE_CHECKING, List, Optional
import re  
from ray.air.config import ScalingConfig
if TYPE_CHECKING:
    from ray.tune.execution.placement_groups import PlacementGroupFactory
import jax
from jax._src.lib import xla_bridge as xb, xla_client as xc, xla_extension as xe


def is_ray_node_resource(resource_key):
    """Check if the current resource is the host ip."""
    ishost_regex = re.compile(r"^node:\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
    return ishost_regex.match(resource_key)


@dataclass
@PublicAPI(stability="alpha")
class ScalingConfigWithIPs(ScalingConfig):
    """Configuration for scaling training with ip address.

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
    ips: A list of IP addresses to use for the workers.
    """
    ips: Optional[List[str]] = None
    head_ip: Optional[str] = None

    def as_placement_group_factory(self) -> "PlacementGroupFactory":
        """Returns a PlacementGroupFactory to specify resources for Tune."""
        from ray.tune import PlacementGroupFactory

        trainer_resources = (
            self.trainer_resources if self.trainer_resources else {"CPU": 1, f"node:{self.head_ip}": 1e-3}
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
            {**worker_resources, **worker_resources_extra, **{f"node:{self.ips[_]}": 1e-3}}
            for _ in range(self.num_workers if self.num_workers else 0)
        ]
        daemon_worker_bundles = [
            {**{f"node:{self.ips[_]}": 1e-3}, **{"CPU": 1}}
            for _ in range(self.num_workers if self.num_workers else 0)
        ]
        bundles = trainer_bundle + worker_bundles + daemon_worker_bundles
        return PlacementGroupFactory(bundles, strategy=self.placement_strategy)
    
    
def update_jax_platform(platform):
    """Update the jax backend platform."""
    jax.config.update("jax_platform_name", platform)
    xb.get_backend.cache_clear()