from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from filelock import FileLock
from threading import RLock
import json
import os
import socket
import logging

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_NODE_TYPE

logger = logging.getLogger(__name__)

filelock_logger = logging.getLogger("filelock")
filelock_logger.setLevel(logging.WARNING)


class ClusterState(object):
    def __init__(self, lock_path, save_path, provider_config):
        self.lock = RLock()
        self.file_lock = FileLock(lock_path)
        self.save_path = save_path

        with self.lock:
            with self.file_lock:
                if os.path.exists(self.save_path):
                    workers = json.loads(open(self.save_path).read())
                else:
                    workers = {}
                logger.info("ClusterState: "
                            "Loaded cluster state: {}".format(workers))
                for worker_ip in provider_config["worker_ips"]:
                    if worker_ip not in workers:
                        workers[worker_ip] = {
                            "tags": {
                                TAG_RAY_NODE_TYPE: "worker"
                            },
                            "state": "terminated",
                        }
                    else:
                        assert workers[worker_ip]["tags"][
                            TAG_RAY_NODE_TYPE] == "worker"
                if provider_config["head_ip"] not in workers:
                    workers[provider_config["head_ip"]] = {
                        "tags": {
                            TAG_RAY_NODE_TYPE: "head"
                        },
                        "state": "terminated",
                    }
                else:
                    assert workers[provider_config["head_ip"]]["tags"][
                        TAG_RAY_NODE_TYPE] == "head"
                assert len(workers) == len(provider_config["worker_ips"]) + 1
                with open(self.save_path, "w") as f:
                    logger.info("ClusterState: "
                                "Writing cluster state: {}".format(workers))
                    f.write(json.dumps(workers))

    def get(self):
        with self.lock:
            with self.file_lock:
                workers = json.loads(open(self.save_path).read())
                return workers

    def put(self, worker_id, info):
        assert "tags" in info
        assert "state" in info
        with self.lock:
            with self.file_lock:
                workers = self.get()
                workers[worker_id] = info
                with open(self.save_path, "w") as f:
                    logger.info("ClusterState: "
                                "Writing cluster state: {}".format(workers))
                    f.write(json.dumps(workers))


class LocalNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.state = ClusterState("/tmp/cluster-{}.lock".format(cluster_name),
                                  "/tmp/cluster-{}.state".format(cluster_name),
                                  provider_config)

    def non_terminated_nodes(self, tag_filters):
        workers = self.state.get()
        matching_ips = []
        for worker_ip, info in workers.items():
            if info["state"] == "terminated":
                continue
            ok = True
            for k, v in tag_filters.items():
                if info["tags"].get(k) != v:
                    ok = False
                    break
            if ok:
                matching_ips.append(worker_ip)
        return matching_ips

    def is_running(self, node_id):
        return self.state.get()[node_id]["state"] == "running"

    def is_terminated(self, node_id):
        return not self.is_running(node_id)

    def node_tags(self, node_id):
        return self.state.get()[node_id]["tags"]

    def external_ip(self, node_id):
        return socket.gethostbyname(node_id)

    def internal_ip(self, node_id):
        return socket.gethostbyname(node_id)

    def set_node_tags(self, node_id, tags):
        with self.state.file_lock:
            info = self.state.get()[node_id]
            info["tags"].update(tags)
            self.state.put(node_id, info)

    def create_node(self, node_config, tags, count):
        node_type = tags[TAG_RAY_NODE_TYPE]
        with self.state.file_lock:
            workers = self.state.get()
            for node_id, info in workers.items():
                if (info["state"] == "terminated"
                        and info["tags"][TAG_RAY_NODE_TYPE] == node_type):
                    info["tags"] = tags
                    info["state"] = "running"
                    self.state.put(node_id, info)
                    return

    def terminate_node(self, node_id):
        workers = self.state.get()
        info = workers[node_id]
        info["state"] = "terminated"
        self.state.put(node_id, info)
