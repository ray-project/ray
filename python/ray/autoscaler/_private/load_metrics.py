from collections import Counter
from functools import reduce
import logging
import time
from typing import Dict, List

import numpy as np
import ray.ray_constants
from ray.autoscaler._private.constants import (
    MEMORY_RESOURCE_UNIT_BYTES,
    AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE,
)
from ray._private.gcs_utils import PlacementGroupTableData
from ray.autoscaler._private.resource_demand_scheduler import NodeIP, ResourceDict
from ray.autoscaler._private.util import DictCount, LoadMetricsSummary
from ray.core.generated.common_pb2 import PlacementStrategy

logger = logging.getLogger(__name__)


def add_resources(dict1: Dict[str, float], dict2: Dict[str, float]) -> Dict[str, float]:
    """Add the values in two dictionaries.

    Returns:
        dict: A new dictionary (inputs remain unmodified).
    """
    new_dict = dict1.copy()
    for k, v in dict2.items():
        new_dict[k] = v + new_dict.get(k, 0)
    return new_dict


def freq_of_dicts(
    dicts: List[Dict], serializer=lambda d: frozenset(d.items()), deserializer=dict
) -> DictCount:
    """Count a list of dictionaries (or unhashable types).

    This is somewhat annoying because mutable data structures aren't hashable,
    and set/dict keys must be hashable.

    Args:
        dicts (List[D]): A list of dictionaries to be counted.
        serializer (D -> S): A custom serialization function. The output type S
            must be hashable. The default serializer converts a dictionary into
            a frozenset of KV pairs.
        deserializer (S -> U): A custom deserialization function. See the
            serializer for information about type S. For dictionaries U := D.

    Returns:
        List[Tuple[U, int]]: Returns a list of tuples. Each entry in the list
            is a tuple containing a unique entry from `dicts` and its
            corresponding frequency count.
    """
    freqs = Counter(map(lambda d: serializer(d), dicts))
    as_list = []
    for as_set, count in freqs.items():
        as_list.append((deserializer(as_set), count))
    return as_list


class LoadMetrics:
    """Container for cluster load metrics.

    Metrics here are updated from raylet heartbeats. The autoscaler
    queries these metrics to determine when to scale up, and which nodes
    can be removed.
    """

    def __init__(self):
        self.last_used_time_by_ip = {}
        self.last_heartbeat_time_by_ip = {}
        self.static_resources_by_ip = {}
        self.dynamic_resources_by_ip = {}
        self.raylet_id_by_ip = {}
        self.resource_load_by_ip = {}
        self.waiting_bundles = []
        self.infeasible_bundles = []
        self.pending_placement_groups = []
        self.resource_requests = []
        self.cluster_full_of_actors_detected = False

    def __bool__(self):
        """A load metrics instance is Falsey iff the autoscaler process
        has not received a resource message from the GCS.
        """
        return bool(self.raylet_id_by_ip)

    def update(
        self,
        ip: str,
        raylet_id: bytes,
        static_resources: Dict[str, Dict],
        dynamic_resources: Dict[str, Dict],
        resource_load: Dict[str, Dict],
        waiting_bundles: List[Dict[str, float]] = None,
        infeasible_bundles: List[Dict[str, float]] = None,
        pending_placement_groups: List[PlacementGroupTableData] = None,
        cluster_full_of_actors_detected: bool = False,
    ):
        self.resource_load_by_ip[ip] = resource_load
        self.static_resources_by_ip[ip] = static_resources
        self.raylet_id_by_ip[ip] = raylet_id
        self.cluster_full_of_actors_detected = cluster_full_of_actors_detected

        if not waiting_bundles:
            waiting_bundles = []
        if not infeasible_bundles:
            infeasible_bundles = []
        if not pending_placement_groups:
            pending_placement_groups = []

        # We are not guaranteed to have a corresponding dynamic resource
        # for every static resource because dynamic resources are based on
        # the available resources in the heartbeat, which does not exist
        # if it is zero. Thus, we have to update dynamic resources here.
        dynamic_resources_update = dynamic_resources.copy()
        for resource_name, capacity in self.static_resources_by_ip[ip].items():
            if resource_name not in dynamic_resources_update:
                dynamic_resources_update[resource_name] = 0.0
        self.dynamic_resources_by_ip[ip] = dynamic_resources_update

        now = time.time()
        if (
            ip not in self.last_used_time_by_ip
            or self.static_resources_by_ip[ip] != self.dynamic_resources_by_ip[ip]
        ):
            self.last_used_time_by_ip[ip] = now
        self.last_heartbeat_time_by_ip[ip] = now
        self.waiting_bundles = waiting_bundles
        self.infeasible_bundles = infeasible_bundles
        self.pending_placement_groups = pending_placement_groups

    def mark_active(self, ip):
        assert ip is not None, "IP should be known at this time"
        logger.debug("Node {} is newly setup, treating as active".format(ip))
        self.last_heartbeat_time_by_ip[ip] = time.time()

    def is_active(self, ip):
        return ip in self.last_heartbeat_time_by_ip

    def prune_active_ips(self, active_ips: List[str]):
        """The Raylet ips stored by LoadMetrics are obtained by polling
        the GCS in Monitor.update_load_metrics().

        On the other hand, the autoscaler gets a list of node ips from
        its NodeProvider.

        This method removes from LoadMetrics the ips unknown to the autoscaler.

        Args:
            active_ips (List[str]): The node ips known to the autoscaler.
        """
        active_ips = set(active_ips)

        def prune(mapping, should_log):
            unwanted_ips = set(mapping) - active_ips
            for unwanted_ip in unwanted_ips:
                if should_log:
                    logger.info("LoadMetrics: " f"Removed ip: {unwanted_ip}.")
                del mapping[unwanted_ip]
            if unwanted_ips and should_log:
                logger.info(
                    "LoadMetrics: "
                    "Removed {} stale ip mappings: {} not in {}".format(
                        len(unwanted_ips), unwanted_ips, active_ips
                    )
                )
            assert not (unwanted_ips & set(mapping))

        prune(self.last_used_time_by_ip, should_log=True)
        prune(self.static_resources_by_ip, should_log=False)
        prune(self.raylet_id_by_ip, should_log=False)
        prune(self.dynamic_resources_by_ip, should_log=False)
        prune(self.resource_load_by_ip, should_log=False)
        prune(self.last_heartbeat_time_by_ip, should_log=False)

    def get_node_resources(self):
        """Return a list of node resources (static resource sizes).

        Example:
            >>> metrics.get_node_resources()
            [{"CPU": 1}, {"CPU": 4, "GPU": 8}]  # for two different nodes
        """
        return self.static_resources_by_ip.values()

    def get_static_node_resources_by_ip(self) -> Dict[NodeIP, ResourceDict]:
        """Return a dict of node resources for every node ip.

        Example:
            >>> lm.get_static_node_resources_by_ip()
            {127.0.0.1: {"CPU": 1}, 127.0.0.2: {"CPU": 4, "GPU": 8}}
        """
        return self.static_resources_by_ip

    def get_resource_utilization(self):
        return self.dynamic_resources_by_ip

    def _get_resource_usage(self):
        num_nodes = 0
        num_nonidle = 0
        resources_used = {}
        resources_total = {}
        for ip, max_resources in self.static_resources_by_ip.items():
            # Nodes without resources don't count as nodes (e.g. unmanaged
            # nodes)
            if any(max_resources.values()):
                num_nodes += 1
            avail_resources = self.dynamic_resources_by_ip[ip]
            resource_load = self.resource_load_by_ip[ip]
            max_frac = 0.0
            for resource_id, amount in resource_load.items():
                if amount > 0:
                    max_frac = 1.0  # the resource is saturated
            for resource_id, amount in max_resources.items():
                used = amount - avail_resources[resource_id]
                if resource_id not in resources_used:
                    resources_used[resource_id] = 0.0
                    resources_total[resource_id] = 0.0
                resources_used[resource_id] += used
                resources_total[resource_id] += amount
                used = max(0, used)
                if amount > 0:
                    frac = used / float(amount)
                    if frac > max_frac:
                        max_frac = frac
            if max_frac > 0:
                num_nonidle += 1

        return resources_used, resources_total

    def get_resource_demand_vector(self, clip=True):
        if clip:
            # Bound the total number of bundles to
            # 2xMAX_RESOURCE_DEMAND_VECTOR_SIZE. This guarantees the resource
            # demand scheduler bin packing algorithm takes a reasonable amount
            # of time to run.
            return (
                self.waiting_bundles[:AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE]
                + self.infeasible_bundles[:AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE]
            )
        else:
            return self.waiting_bundles + self.infeasible_bundles

    def get_resource_requests(self):
        return self.resource_requests

    def get_pending_placement_groups(self):
        return self.pending_placement_groups

    def resources_avail_summary(self) -> str:
        """Return a concise string of cluster size to report to event logs.

        For example, "3 CPUs, 4 GPUs".
        """
        total_resources = (
            reduce(add_resources, self.static_resources_by_ip.values())
            if self.static_resources_by_ip
            else {}
        )
        out = "{} CPUs".format(int(total_resources.get("CPU", 0)))
        if "GPU" in total_resources:
            out += ", {} GPUs".format(int(total_resources["GPU"]))
        return out

    def summary(self):
        available_resources = (
            reduce(add_resources, self.dynamic_resources_by_ip.values())
            if self.dynamic_resources_by_ip
            else {}
        )
        total_resources = (
            reduce(add_resources, self.static_resources_by_ip.values())
            if self.static_resources_by_ip
            else {}
        )
        usage_dict = {}
        for key in total_resources:
            if key in ["memory", "object_store_memory"]:
                total = (
                    total_resources[key] * ray.ray_constants.MEMORY_RESOURCE_UNIT_BYTES
                )
                available = (
                    available_resources[key]
                    * ray.ray_constants.MEMORY_RESOURCE_UNIT_BYTES
                )
                usage_dict[key] = (total - available, total)
            else:
                total = total_resources[key]
                usage_dict[key] = (total - available_resources[key], total)

        summarized_demand_vector = freq_of_dicts(
            self.get_resource_demand_vector(clip=False)
        )
        summarized_resource_requests = freq_of_dicts(self.get_resource_requests())

        def placement_group_serializer(pg):
            bundles = tuple(
                frozenset(bundle.unit_resources.items()) for bundle in pg.bundles
            )
            return (bundles, pg.strategy)

        def placement_group_deserializer(pg_tuple):
            # We marshal this as a dictionary so that we can easily json.dumps
            # it later.
            # TODO (Alex): Would there be a benefit to properly
            # marshalling this (into a protobuf)?
            bundles = list(map(dict, pg_tuple[0]))
            return {
                "bundles": freq_of_dicts(bundles),
                "strategy": PlacementStrategy.Name(pg_tuple[1]),
            }

        summarized_placement_groups = freq_of_dicts(
            self.get_pending_placement_groups(),
            serializer=placement_group_serializer,
            deserializer=placement_group_deserializer,
        )
        nodes_summary = freq_of_dicts(self.static_resources_by_ip.values())

        return LoadMetricsSummary(
            usage=usage_dict,
            resource_demand=summarized_demand_vector,
            pg_demand=summarized_placement_groups,
            request_demand=summarized_resource_requests,
            node_types=nodes_summary,
        )

    def set_resource_requests(self, requested_resources):
        if requested_resources is not None:
            assert isinstance(requested_resources, list), requested_resources
        self.resource_requests = [
            request for request in requested_resources if len(request) > 0
        ]

    def info_string(self):
        return " - " + "\n - ".join(
            ["{}: {}".format(k, v) for k, v in sorted(self._info().items())]
        )

    def _info(self):
        resources_used, resources_total = self._get_resource_usage()

        now = time.time()
        idle_times = [now - t for t in self.last_used_time_by_ip.values()]
        heartbeat_times = [now - t for t in self.last_heartbeat_time_by_ip.values()]
        most_delayed_heartbeats = sorted(
            self.last_heartbeat_time_by_ip.items(), key=lambda pair: pair[1]
        )[:5]
        most_delayed_heartbeats = {ip: (now - t) for ip, t in most_delayed_heartbeats}

        def format_resource(key, value):
            if key in ["object_store_memory", "memory"]:
                return "{} GiB".format(
                    round(value * MEMORY_RESOURCE_UNIT_BYTES / (1024 * 1024 * 1024), 2)
                )
            else:
                return round(value, 2)

        return {
            "ResourceUsage": ", ".join(
                [
                    "{}/{} {}".format(
                        format_resource(rid, resources_used[rid]),
                        format_resource(rid, resources_total[rid]),
                        rid,
                    )
                    for rid in sorted(resources_used)
                    if not rid.startswith("node:")
                ]
            ),
            "NodeIdleSeconds": "Min={} Mean={} Max={}".format(
                int(np.min(idle_times)) if idle_times else -1,
                int(np.mean(idle_times)) if idle_times else -1,
                int(np.max(idle_times)) if idle_times else -1,
            ),
            "TimeSinceLastHeartbeat": "Min={} Mean={} Max={}".format(
                int(np.min(heartbeat_times)) if heartbeat_times else -1,
                int(np.mean(heartbeat_times)) if heartbeat_times else -1,
                int(np.max(heartbeat_times)) if heartbeat_times else -1,
            ),
            "MostDelayedHeartbeats": most_delayed_heartbeats,
        }
