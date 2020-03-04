import copy
import json
import threading
import redis

import ray
from ray.dashboard.closed_source.ingest_server \
    import NODE_INFO_CHANNEL, RAY_INFO_CHANNEL
from ray.dashboard.dashboard_controller_interface \
    import BaseDashboardController


class HostedDashboardController(BaseDashboardController):
    """Dashboard interface that is used in a hosted side."""

    def __init__(self, redis_address, redis_password, update_frequency=1.0):
        self.redis_client = redis.StrictRedis(host="127.0.0.1", port=6379)
        self.node_stats = HostedNodeStatsImporter(self.redis_client)
        self.raylet_stats = HostedRayletStatsImporter(self.redis_client)
        self.is_hosted = True

    def get_node_info(self):
        return self.node_stats.get_node_stats()

    def get_raylet_info(self):
        return self.raylet_stats.get_ralyet_stats()

    def launch_profiling(self, node_id, pid, duration):
        raise NotImplementedError("TODO")

    def check_profiling_status(self, profiling_id):
        raise NotImplementedError("TODO")

    def get_profiling_info(self, profiling_id):
        raise NotImplementedError("TODO")

    def kill_actor(self, actor_id, ip_address, port):
        raise NotImplementedError("TODO")

    def get_logs(self, hostname, pid):
        raise NotImplementedError("TODO")

    def get_errors(self, hostname, pid):
        raise NotImplementedError("TODO")

    def start_collecting_metrics(self):
        self.node_stats.start()
        self.raylet_stats.start()


class HostedNodeStatsImporter(threading.Thread):
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.node_stats_data = {}
        self._node_stats_lock = threading.Lock()

        super().__init__()

    def get_node_stats(self):
        with self._node_stats_lock:
            return copy.deepcopy(self.node_stats_data)

    def run(self):
        p = self.redis_client.pubsub(ignore_subscribe_messages=True)
        p.psubscribe(NODE_INFO_CHANNEL)
        # TODO(sang): Error handling
        for x in p.listen():
            with self._node_stats_lock:
                data = x["data"]
                data = json.loads(ray.utils.decode(data))
                self.node_stats_data = data


class HostedRayletStatsImporter(threading.Thread):
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.raylet_stats_data = {}
        self._raylet_stats_lock = threading.Lock()

        super().__init__()

    def get_ralyet_stats(self):
        with self._raylet_stats_lock:
            return copy.deepcopy(self.raylet_stats_data)

    def run(self):
        p = self.redis_client.pubsub(ignore_subscribe_messages=True)
        p.psubscribe(RAY_INFO_CHANNEL)
        # TODO(sang): Error handling
        for x in p.listen():
            with self._raylet_stats_lock:
                data = x["data"]
                data = json.loads(ray.utils.decode(data))
                self.raylet_stats_data = data
