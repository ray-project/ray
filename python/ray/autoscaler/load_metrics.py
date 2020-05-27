import logging
import time

import numpy as np
import ray.services as services
from ray.ray_constants import MEMORY_RESOURCE_UNIT_BYTES

logger = logging.getLogger(__name__)


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
        self.resource_load_by_ip = {}
        self.local_ip = services.get_node_ip_address()

    def update(self, ip, static_resources, dynamic_resources, resource_load):
        self.resource_load_by_ip[ip] = resource_load
        self.static_resources_by_ip[ip] = static_resources

        # We are not guaranteed to have a corresponding dynamic resource for
        # every static resource because dynamic resources are based on the
        # available resources in the heartbeat, which does not exist if it is
        # zero. Thus, we have to update dynamic resources here.
        dynamic_resources_update = dynamic_resources.copy()
        for resource_name, capacity in static_resources.items():
            if resource_name not in dynamic_resources_update:
                dynamic_resources_update[resource_name] = 0.0
        self.dynamic_resources_by_ip[ip] = dynamic_resources_update

        now = time.time()
        if ip not in self.last_used_time_by_ip or \
                static_resources != dynamic_resources:
            self.last_used_time_by_ip[ip] = now
        self.last_heartbeat_time_by_ip[ip] = now

    def mark_active(self, ip):
        assert ip is not None, "IP should be known at this time"
        logger.info("Node {} is newly setup, treating as active".format(ip))
        self.last_heartbeat_time_by_ip[ip] = time.time()

    def prune_active_ips(self, active_ips):
        active_ips = set(active_ips)
        active_ips.add(self.local_ip)

        def prune(mapping):
            unwanted = set(mapping) - active_ips
            for unwanted_key in unwanted:
                logger.info("LoadMetrics: "
                            "Removed mapping: {} - {}".format(
                                unwanted_key, mapping[unwanted_key]))
                del mapping[unwanted_key]
            if unwanted:
                logger.info(
                    "LoadMetrics: "
                    "Removed {} stale ip mappings: {} not in {}".format(
                        len(unwanted), unwanted, active_ips))
            assert not (unwanted & set(mapping))

        prune(self.last_used_time_by_ip)
        prune(self.static_resources_by_ip)
        prune(self.dynamic_resources_by_ip)
        prune(self.resource_load_by_ip)
        prune(self.last_heartbeat_time_by_ip)

    def approx_workers_used(self):
        return self._info()["NumNodesUsed"]

    def num_workers_connected(self):
        return self._info()["NumNodesConnected"]

    def get_node_resources(self):
        """Return a list of node resources (static resource sizes.

        Example:
            >>> metrics.get_node_resources()
            [{"CPU": 1}, {"CPU": 4, "GPU": 8}]  # for two different nodes
        """
        return self.static_resources_by_ip.values()

    def get_resource_usage(self):
        num_nodes = len(self.static_resources_by_ip)
        nodes_used = 0.0
        num_nonidle = 0
        has_saturated_node = False
        resources_used = {}
        resources_total = {}
        for ip, max_resources in self.static_resources_by_ip.items():
            avail_resources = self.dynamic_resources_by_ip[ip]
            resource_load = self.resource_load_by_ip[ip]
            max_frac = 0.0
            for resource_id, amount in resource_load.items():
                if amount > 0:
                    has_saturated_node = True
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
            nodes_used += max_frac
            if max_frac > 0:
                num_nonidle += 1

        # If any nodes have a queue buildup, assume all non-idle nodes are 100%
        # busy, plus the head node. This guards against the case of not scaling
        # up due to poor task packing.
        if has_saturated_node:
            nodes_used = min(num_nonidle + 1.0, num_nodes)

        return nodes_used, resources_used, resources_total

    def info_string(self):
        return ", ".join(
            ["{}={}".format(k, v) for k, v in sorted(self._info().items())])

    def _info(self):
        nodes_used, resources_used, resources_total = self.get_resource_usage()

        now = time.time()
        idle_times = [now - t for t in self.last_used_time_by_ip.values()]
        heartbeat_times = [
            now - t for t in self.last_heartbeat_time_by_ip.values()
        ]
        most_delayed_heartbeats = sorted(
            self.last_heartbeat_time_by_ip.items(),
            key=lambda pair: pair[1])[:5]
        most_delayed_heartbeats = {
            ip: (now - t)
            for ip, t in most_delayed_heartbeats
        }

        def format_resource(key, value):
            if key in ["object_store_memory", "memory"]:
                return "{} GiB".format(
                    round(value * MEMORY_RESOURCE_UNIT_BYTES / 1e9, 2))
            else:
                return round(value, 2)

        return {
            "ResourceUsage": ", ".join([
                "{}/{} {}".format(
                    format_resource(rid, resources_used[rid]),
                    format_resource(rid, resources_total[rid]), rid)
                for rid in sorted(resources_used)
            ]),
            "NumNodesConnected": len(self.static_resources_by_ip),
            "NumNodesUsed": round(nodes_used, 2),
            "NodeIdleSeconds": "Min={} Mean={} Max={}".format(
                int(np.min(idle_times)) if idle_times else -1,
                int(np.mean(idle_times)) if idle_times else -1,
                int(np.max(idle_times)) if idle_times else -1),
            "TimeSinceLastHeartbeat": "Min={} Mean={} Max={}".format(
                int(np.min(heartbeat_times)) if heartbeat_times else -1,
                int(np.mean(heartbeat_times)) if heartbeat_times else -1,
                int(np.max(heartbeat_times)) if heartbeat_times else -1),
            "MostDelayedHeartbeats": most_delayed_heartbeats,
        }
