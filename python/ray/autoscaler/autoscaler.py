from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import base64
import json
import hashlib
import os
import time

from collections import defaultdict

from checksumdir import dirhash
from ray.autoscaler.cloud_provider import get_cloud_provider
from ray.autoscaler.updater import NodeUpdater
from ray.autoscaler.tags import TAG_RAY_LAUNCH_CONFIG, \
    TAG_RAY_APPLIED_CONFIG, TAG_RAY_WORKER_GROUP, TAG_RAY_WORKER_STATUS


DEFAULT_CLUSTER_CONFIG = {
    "provider": "aws",
    "worker_group": "default",
    "num_nodes": 50,
    "node": {
        "InstanceType": "t2.small",
        "ImageId": "ami-d04396aa",
        "KeyName": "ekl-laptop-thinkpad",
        "SubnetId": "subnet-20f16f0c",
        "SecurityGroupIds": ["sg-f3d40980"],
    },
    "file_mounts": {
        # "/home/ubuntu/data": "/home/eric/Desktop/data",
    },
    "init_commands": [
        "/home/ubuntu/.local/bin/ray stop",
        "/home/ubuntu/.local/bin/ray start "
        "--redis-address=172.30.0.147:35262",
    ],
}


# Abort autoscaling if more than this number of errors are encountered. This
# is a safety feature to prevent e.g. runaway node launches.
MAX_NUM_FAILURES = 5


class StandardAutoscaler(object):
    def __init__(self, config=DEFAULT_CLUSTER_CONFIG):
        self.config = config
        self.launch_hash = _hash_launch_conf(config)
        self.files_hash = _hash_files(config)
        self.cloud = get_cloud_provider(config)

        # Map from node_id to NodeUpdater processes
        self.updaters = {}
        self.num_failed_updates = defaultdict(int)
        self.num_failures = 0

        for local_dir in config["file_mounts"].values():
            assert os.path.isdir(local_dir)

        print("StandardAutoscaler: {}".format(self.config))

    def update(self):
        try:
            self._update()
        except Exception as e:
            print("StandardAutoscaler: Error during autoscaling: {}".format(e))
            time.sleep(5)
            self.num_failures += 1
            if self.num_failures > MAX_NUM_FAILURES:
                print("StandardAutoscaler: Too many errors, abort.")
                raise e

    def _update(self):
        nodes = self.cloud.nodes()
        target_num_nodes = self.config["num_nodes"]

        # Terminate nodes while there are too many
        while len(nodes) > target_num_nodes:
            print(
                "StandardAutoscaler: Terminating unneeded node: "
                "{}".format(nodes[-1]))
            self.cloud.terminate_node(nodes[-1])
            nodes = self.cloud.nodes()
            print(self.debug_string())

        if target_num_nodes == 0:
            return

        # Update nodes with out-of-date files
        for node_id in nodes:
            self.update_if_needed(node_id)

        # Launch a new node if needed
        if len(nodes) < target_num_nodes:
            self.launch_new_node()
            print(self.debug_string())
            return
        else:
            # If enough nodes, terminate an out-of-date node.
            for node_id in nodes:
                if not self.launch_config_ok(node_id):
                    print(
                        "StandardAutoscaler: Terminating outdated node: "
                        "{}".format(node_id))
                    self.cloud.terminate_node(node_id)
                    print(self.debug_string())
                    return

        # Process any completed updates
        completed = []
        for node_id, updater in self.updaters.items():
            if not updater.is_alive():
                completed.append(node_id)
        if completed:
            for node_id in completed:
                if self.updaters[node_id].exitcode != 0:
                    self.num_failed_updates[node_id] += 1
                del self.updaters[node_id]
            print(self.debug_string())

    def launch_config_ok(self, node_id):
        launch_conf = self.cloud.node_tags(node_id).get(TAG_RAY_LAUNCH_CONFIG)
        if self.launch_hash != launch_conf:
            return False
        return True

    def files_up_to_date(self, node_id):
        applied = self.cloud.node_tags(node_id).get(TAG_RAY_APPLIED_CONFIG)
        if applied != self.files_hash:
            print(
                "StandardAutoscaler: {} has file state {}, required {}".format(
                    node_id, applied, self.files_hash))
            return False
        return True

    def update_if_needed(self, node_id):
        if not self.cloud.is_running(node_id):
            return
        if not self.launch_config_ok(node_id):
            return
        if node_id in self.updaters:
            return
        if self.num_failed_updates.get(node_id, 0) > 0:  # TODO(ekl) retry?
            return
        if self.files_up_to_date(node_id):
            return
        updater = NodeUpdater(node_id, self.config, self.files_hash)
        updater.start()
        self.updaters[node_id] = updater

    def launch_new_node(self):
        print("StandardAutoscaler: Launching new node")
        num_before = len(self.cloud.nodes())
        self.cloud.create_node({
            TAG_RAY_WORKER_STATUS: "Uninitialized",
            TAG_RAY_WORKER_GROUP: self.config["worker_group"],
            TAG_RAY_LAUNCH_CONFIG: self.launch_hash,
        })
        # TODO(ekl) be less conservative in this check
        assert len(self.cloud.nodes()) > num_before, \
            "Num nodes failed to increase after creating a new node"

    def debug_string(self, nodes=None):
        if nodes is None:
            nodes = self.cloud.nodes()
        target_num_nodes = self.config["num_nodes"]
        suffix = ""
        if self.updaters:
            suffix += " ({} updating)".format(len(self.updaters))
        if self.num_failed_updates:
            suffix += " ({} failed to update)".format(
                len(self.num_failed_updates))
        return "StandardAutoscaler: Have {} / {} target nodes{}".format(
                len(nodes), target_num_nodes, suffix)


def _hash_launch_conf(config):
    hasher = hashlib.sha1()
    hasher.update(json.dumps(config["node"]).encode("utf-8"))
    return base64.encodestring(hasher.digest()).decode("utf-8").strip()


def _hash_files(config):
    hasher = hashlib.sha1()
    hasher.update(json.dumps([
        config["file_mounts"], config["init_commands"],
        [dirhash(d) for d in sorted(config["file_mounts"].values())]
    ]).encode("utf-8"))
    return base64.encodestring(hasher.digest()).decode("utf-8").strip()
