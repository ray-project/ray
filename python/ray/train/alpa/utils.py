import logging
from dataclasses import dataclass
import ray
from ray.util.annotations import PublicAPI
from typing import TYPE_CHECKING, List, Optional
import re
from ray.air.config import ScalingConfig
from ray.util.placement_group import get_current_placement_group
from ray._private.ray_constants import env_integer

if TYPE_CHECKING:
    from ray.tune.execution.placement_groups import PlacementGroupFactory
import jax
from jax._src.lib import xla_bridge as xb
import alpa

from ray.train.constants import TRAIN_PLACEMENT_GROUP_TIMEOUT_S_ENV

try:
    from alpa.device_mesh import VirtualPhysicalMesh, DeviceCluster
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "alpa isn't installed. To install alpa, run 'pip install " "alpa'."
    )

logger = logging.getLogger(__name__)

########################################
# Ray Palcement Group API Utilities
########################################


def get_bundle2ip(pg=None):
    """get the ip address list from placement group
    The ordering of the ip address are aligned with each bundle index.
    """

    if pg:
        pg_id = pg.id.hex()
    # dictionary: bundle_group to node_ip
    dict_bg2ip = {}

    ray_state = try_import_ray_state()
    resources_list = ray_state.state._available_resources_per_node().values()

    for resource in resources_list:
        resource_name_list = resource.keys()

        # ic(resource_name_list, pg_id)
        node_ip = None
        bundle_index_list = []
        for resource_name in resource_name_list:
            # when bundles are created, pg resources are
            # specified as [resource]_[bundle_index]_[pg_id]
            if pg:
                try_bundle_index = re.findall(
                    rf"bundle_group_(\d+)_{pg_id}", resource_name
                )
            else:
                try_bundle_index = re.findall(r"bundle_group_(\d+)_.*", resource_name)

            try_node_ip = re.findall(
                r"^node:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$)", resource_name
            )

            if try_node_ip:
                node_ip = try_node_ip[0]

            if try_bundle_index:
                bundle_index_list.append(try_bundle_index[0])

        dict_bg2ip.update(
            **dict(zip(bundle_index_list, [node_ip] * len(bundle_index_list)))
        )

    ip_list = []
    for i in range(len(dict_bg2ip)):
        ip_list.append(dict_bg2ip[str(i)])

    return ip_list


def create_placement_group(
    num_hosts, num_devices_per_host, additional_resources_per_host=None
):
    """Creates a placement group if it does not exist.
    If a placement group is already detected (Tune) this will be a no-op.
    By default the placement group will be created with `SPREAD` strategy.
    This is optimized for colocating GPUs on different nodes.
    If a placement group is created it will be return the current
    placement group
    Args:
        num_hosts: the number of hosts to create the placement group for
        num_devices_per_host: the number of devices per host
        additional_resources_per_host: additional resources per host
    Returns:
        The placement group
    """
    current_placement_group = get_current_placement_group()
    ray_worker = try_import_ray_worker()
    worker = ray_worker.global_worker  # pylint: disable=protected-access
    should_capture_child_tasks_in_placement_group = (
        worker.should_capture_child_tasks_in_placement_group
    )
    should_create_placement_group = (
        current_placement_group is None
        or not should_capture_child_tasks_in_placement_group
    )

    if should_create_placement_group:
        additional_resources_per_host = additional_resources_per_host or {}
        bundle = {
            "CPU": 1,
            "GPU": num_devices_per_host,
            **additional_resources_per_host,
        }
        bundles = [bundle.copy() for _ in range(num_hosts)]

        # Alpa Placement Group: `SPREAD` strategy is required
        # https://docs.ray.io/en/latest/ray-core/placement-group.html#strategy-types
        # Each bundle must be scheduled in a separate node.
        strategy = "SPREAD"

        placement_group = ray.util.placement_group(bundles, strategy=strategy)
        logger.debug("Waiting for placement group to start.")
        timeout = env_integer(PLACEMENT_GROUP_TIMEOUT_S_ENV, 100)
        ready, _ = ray.wait([placement_group.ready()], timeout=timeout)
        if ready:
            logger.debug("Placement group has started.")
        else:
            raise TimeoutError(
                "Placement group creation timed out. Make sure your "
                "cluster either has enough resources or use an "
                "autoscaling cluster. If you are running on a cluster, "
                "make sure you specify an address in `ray.init()`, for example,"
                ' `ray.init("auto")`. You can also increase the timeout by '
                "setting the ALPA_PLACEMENT_GROUP_TIMEOUT_S environment "
                "variable. Current resources available: "
                f"{ray.available_resources()}, resources requested by "
                f"the placement group: {placement_group.bundle_specs}"
            )
        return placement_group
    else:
        return current_placement_group


def get_bundle_idx(placement_group, device_ips):
    """Get the bundle index for the placement group.
    The placement group is a list of resource bundles.
    Each bundle will be assigned to a device.
    First, we need to find the bundle index with GPU resources.
    Then, we can find the device IP for the bundle index.
    Lastly, we sort bundle index according to the device IP list given.
    Args:
        placement_group (placement group): The placement group.
        device_ips (list): The list of device IP addresses. # noqa
    Returns:
        list: The sorted bundle index list.
    """
    # get the device IP for the bundle index
    bundle_ips = get_bundle2ip(placement_group)
    bundle_specs = placement_group.bundle_specs

    # filter out the bundle index with device (GPUs)
    device_bundle_idx_list = [
        i for i, bundle_spec in enumerate(bundle_specs) if bundle_spec.get("GPU", 0) > 0
    ]

    if len(device_bundle_idx_list) != len(device_ips):
        raise ValueError(
            "The number of bundles with GPU resources "
            "is not equal to the number of device IPs."
        )

    # device IP -> bundle index
    bundle_ip2idx = {bundle_ips[i]: i for i in device_bundle_idx_list}

    # sorted bundle index according to the device IP list given
    sorted_bundle_idx = [bundle_ip2idx[ip] for ip in device_ips]

    return sorted_bundle_idx


########################################
# utils Utilities
########################################


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

    def as_placement_group_factory(self) -> "PlacementGroupFactory":
        """Returns a PlacementGroupFactory to specify resources for Tune."""
        from ray.tune import PlacementGroupFactory

        trainer_resources = (
            self.trainer_resources
            if self.trainer_resources
            else {"CPU": 1, "node:10.0.2.91": 1e-3}
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
            {
                **worker_resources,
                **worker_resources_extra,
                # **{f"node:{self.ips[_]}": 1e-3},
            }
            for _ in range(self.num_workers if self.num_workers else 0)
        ]

        # added here to gives the deamon resources
        # otherwise hang out forever when the trainer is killed
        daemon_worker_bundles = [
            {**{"CPU": 1}} for _ in range(self.num_workers if self.num_workers else 0)
        ]
        bundles = trainer_bundle + worker_bundles + daemon_worker_bundles
        return PlacementGroupFactory(bundles, strategy=self.placement_strategy)


def update_jax_platform(platform):
    """Update the jax backend platform."""
    jax.config.update("jax_platform_name", platform)
    xb.get_backend.cache_clear()


########################################
# Alpa Trainer Cluster Manager Utilities
########################################


class AlpaManager:
    """AlpaManager is responsible for cluster setup and management."""

    def __init__(self, scaling_config):
        # collect ray cluster info
        # constrain according to the ScalingConfig

        # scaling parameters
        self.scaling_config = scaling_config

        self.resources_per_worker = self.scaling_config.resources_per_worker

        num_workers = self.scaling_config.num_workers
        num_gpus = int(self.scaling_config.use_gpu)
        if "GPU" in self.resources_per_worker:
            num_gpus = self.resources_per_worker["GPU"]

        # head node info
        from ray._private.worker import _global_node as ray_global_node

        self.head_info = ray_global_node.address_info
        self.head_ip = self.head_info["node_ip_address"]

        # Gather host ids
        self.host_info = []
        self.host_ips = []

        for node in ray.nodes():
            for key in node["Resources"]:
                if is_ray_node_resource(key):
                    self.host_info.append(node)
                    self.host_ips.append(key.split("node:")[-1])

        # Gather device info
        self.host_num_devices = []
        for host_info in self.host_info:
            number = host_info["Resources"]["GPU"]
            assert number.is_integer()
            self.host_num_devices.append(int(number))

        # map: host ip to host info
        self.dict_host_ip2info = dict(zip(self.host_ips, self.host_info))

        # number of workers filter
        # the number of workers can not exceeed the number of devices
        num_workers = min(num_workers, len(self.host_info))
        node_ids = list(range(num_workers))

        # filter the number of gpus per worker
        self.host_num_devices = [self.host_num_devices[i] for i in range(num_workers)]
        num_devices_per_host = min(self.host_num_devices)
        num_devices_per_host = min(num_gpus, num_devices_per_host)

        self.num_devices_per_host = num_devices_per_host
        self.node_ids = node_ids
        self.num_workers = num_workers

        self.placement_group = None

    def init_global_cluster(self):
        # initilize the global cluster and virtual
        # physical mesh cluster

        # construction is same to `init_global_cluster`
        # https://github.com/alpa-projects/alpa/blob/388594f00d1ee0fe4dc0d51c2d8567da13226fdf/alpa/device_mesh.py#L2085-L2096
        # and adapt the cluster construction according to
        # placement group
        alpa.api.is_initialized = True
        update_jax_platform("cpu")

        # get the placement group
        self._create_placement_group()

        # get bundle's ip address
        ips = get_bundle2ip(self.placement_group)
        bundle_specs = self.placement_group.bundle_specs

        # filter out the bundle index with device (GPUs)
        device_bundle_idx_list = [
            i
            for i, bundle_spec in enumerate(bundle_specs)
            if bundle_spec.get("GPU", 0) > 0
        ]
        ips = [ips[bundle_idx] for bundle_idx in device_bundle_idx_list]

        # filter nodes according to the placment group
        node_info = [self.dict_host_ip2info[ip] for ip in ips]

        # setup Device Cluster
        cluster = DeviceCluster()
        cluster.host_info = node_info
        cluster.host_num_devices = [
            self.num_devices_per_host for i in range(self.num_workers)
        ]
        alpa.device_mesh.set_global_cluster(cluster)

        # setup Virtual Physical Mesh
        vp_mesh = VirtualPhysicalMesh(
            host_ids=self.node_ids,
            host_info=node_info,
            head_ip=self.head_ip,
            num_devices_per_host=self.num_devices_per_host,
            parent=None,
        )
        alpa.device_mesh.set_global_virtual_physical_mesh(vp_mesh)

    def _create_placement_group(self):
        """Creates a placement group if it does not exist.

        If a placement group is already detected (Tune) this will be a no-op.

        By default the placement group will be created with `SPREAD` strategy.
        This is optimized for colocating GPUs on different nodes.

        If a placement group is created it will be stored as
        self._placement_group.
        """
        current_placement_group = get_current_placement_group()
        worker = ray._private.worker.global_worker
        should_capture_child_tasks_in_placement_group = (
            worker.should_capture_child_tasks_in_placement_group
        )
        should_create_placement_group = (
            current_placement_group is None
            or not should_capture_child_tasks_in_placement_group
        )

        if should_create_placement_group:
            additional_resources_per_worker = (
                self._additional_resources_per_worker or {}
            )
            bundle = {
                "CPU": self._num_cpus_per_worker,
                "GPU": self._num_gpus_per_worker,
                **additional_resources_per_worker,
            }
            bundles = [bundle.copy() for _ in range(self._num_workers)]

            # AlpaTrainer: `SPREAD` strategy is required
            strategy = "SPREAD"

            placement_group = ray.util.placement_group(bundles, strategy=strategy)
            logger.debug("Waiting for placement group to start.")
            timeout = env_integer(TRAIN_PLACEMENT_GROUP_TIMEOUT_S_ENV, 100)
            ready, _ = ray.wait([placement_group.ready()], timeout=timeout)
            if ready:
                logger.debug("Placement group has started.")
            else:
                raise TimeoutError(
                    "Placement group creation timed out. Make sure your "
                    "cluster either has enough resources or use an "
                    "autoscaling cluster. If you are running on a cluster, "
                    "make sure you specify an address in `ray.init()`, for example, "
                    '`ray.init("auto")`. You can also increase the timeout by setting '
                    "the TRAIN_PLACEMENT_GROUP_TIMEOUT_S environment variable. "
                    "Current resources available: {}, resources requested by the "
                    "placement group: {}".format(
                        ray.available_resources(), placement_group.bundle_specs
                    )
                )
            self.placement_group = placement_group

        else:
            self.placement_group = current_placement_group
