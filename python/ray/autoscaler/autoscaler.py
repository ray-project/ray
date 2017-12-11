from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import base64
import json
import hashlib
import os
import traceback

from collections import defaultdict

from checksumdir import dirhash

from ray.autoscaler.node_provider import get_node_provider
from ray.autoscaler.updater import NodeUpdater
from ray.autoscaler.tags import TAG_RAY_LAUNCH_CONFIG, \
    TAG_RAY_RUNTIME_CONFIG, TAG_RAY_WORKER_GROUP, TAG_RAY_WORKER_STATUS, \
    TAG_RAY_NODE_TYPE, TAG_NAME
import ray.services as services


DEFAULT_CLUSTER_CONFIG = {
    "provider": {
        "type": "aws",
        "region": "us-east-1",
    },
    "auth": {
        "ssh_user": "ubuntu",
        "ssh_private_key": "/home/eric/.ssh/ekl-laptop-thinkpad.pem",
    },
    "worker_group": "default",
    "num_nodes": 5,
    "head_node": {
        "InstanceType": "t2.small",
        "ImageId": "ami-8297fef8",
        "KeyName": "ekl-laptop-thinkpad",
        "SubnetId": "subnet-20f16f0c",
        "SecurityGroupIds": ["sg-f3d40980"],
    },
    "node": {
        "InstanceType": "t2.small",
        "ImageId": "ami-8297fef8",
        "KeyName": "ekl-laptop-thinkpad",
        "SubnetId": "subnet-20f16f0c",
        "SecurityGroupIds": ["sg-f3d40980"],
    },
    "file_mounts": {
        # "/home/ubuntu/data": "/home/eric/Desktop/data",
    },
    "init_commands": [
        "/home/ubuntu/.local/bin/ray stop",
        "/home/ubuntu/.local/bin/ray start --redis-address=$RAY_HEAD_IP:6379",
    ],
}


# Abort autoscaling if more than this number of errors are encountered. This
# is a safety feature to prevent e.g. runaway node launches.
MAX_NUM_FAILURES = 5

# Max number of nodes to launch at a time.
MAX_CONCURRENT_LAUNCHES = 10


class StandardAutoscaler(object):
    def __init__(self, config=DEFAULT_CLUSTER_CONFIG):
        self.config = config
        self.launch_hash = hash_launch_conf(config["node"])
        self.files_hash = hash_runtime_conf(
            config["file_mounts"], config["init_commands"])
        self.provider = get_node_provider(
            config["provider"], config["worker_group"])

        # Map from node_id to NodeUpdater processes
        self.updaters = {}
        self.num_failed_updates = defaultdict(int)
        self.num_failures = 0

        for local_path in config["file_mounts"].values():
            assert os.path.exists(local_path)

        print("StandardAutoscaler: {}".format(self.config))

    def workers(self):
        return self.provider.nodes(tag_filters={
            TAG_RAY_NODE_TYPE: "Worker",
        })

    def update(self):
        try:
            self._update()
        except Exception as e:
            print(
                "StandardAutoscaler: Error during autoscaling: {}",
                traceback.format_exc())
            self.num_failures += 1
            if self.num_failures > MAX_NUM_FAILURES:
                print("*** StandardAutoscaler: Too many errors, abort. ***")
                raise e

    def _update(self):
        nodes = self.workers()
        target_num_nodes = self.config["num_nodes"]

        # Terminate nodes while there are too many
        while len(nodes) > target_num_nodes:
            print(
                "StandardAutoscaler: Terminating unneeded node: "
                "{}".format(nodes[-1]))
            self.provider.terminate_node(nodes[-1])
            nodes = self.workers()
            print(self.debug_string())

        if target_num_nodes == 0:
            return

        # Update nodes with out-of-date files
        for node_id in nodes:
            self.update_if_needed(node_id)

        # Launch a new node if needed
        if len(nodes) < target_num_nodes:
            self.launch_new_node(
                min(MAX_CONCURRENT_LAUNCHES, target_num_nodes - len(nodes)))
            print(self.debug_string())
            return
        else:
            # If enough nodes, terminate an out-of-date node.
            for node_id in nodes:
                if not self.launch_config_ok(node_id):
                    print(
                        "StandardAutoscaler: Terminating outdated node: "
                        "{}".format(node_id))
                    self.provider.terminate_node(node_id)
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
        launch_conf = self.provider.node_tags(node_id).get(
            TAG_RAY_LAUNCH_CONFIG)
        if self.launch_hash != launch_conf:
            return False
        return True

    def files_up_to_date(self, node_id):
        applied = self.provider.node_tags(node_id).get(TAG_RAY_RUNTIME_CONFIG)
        if applied != self.files_hash:
            print(
                "StandardAutoscaler: {} has file state {}, required {}".format(
                    node_id, applied, self.files_hash))
            return False
        return True

    def update_if_needed(self, node_id):
        if not self.provider.is_running(node_id):
            return
        if not self.launch_config_ok(node_id):
            return
        if node_id in self.updaters:
            return
        if self.num_failed_updates.get(node_id, 0) > 0:  # TODO(ekl) retry?
            return
        if self.files_up_to_date(node_id):
            return
        updater = NodeUpdater(
            node_id,
            self.config["provider"],
            self.config["auth"],
            self.config["worker_group"],
            self.config["file_mounts"],
            with_head_node_ip(self.config["init_commands"]),
            self.files_hash)
        updater.start()
        self.updaters[node_id] = updater

    def launch_new_node(self, count):
        print("StandardAutoscaler: Launching {} new nodes".format(count))
        num_before = len(self.workers())
        self.provider.create_node(
            self.config["node"],
            {
                TAG_NAME: "ray-worker-{}".format(self.config["worker_group"]),
                TAG_RAY_NODE_TYPE: "Worker",
                TAG_RAY_WORKER_STATUS: "Uninitialized",
                TAG_RAY_WORKER_GROUP: self.config["worker_group"],
                TAG_RAY_LAUNCH_CONFIG: self.launch_hash,
            },
            count)
        # TODO(ekl) be less conservative in this check
        assert len(self.workers()) > num_before, \
            "Num nodes failed to increase after creating a new node"

    def debug_string(self, nodes=None):
        if nodes is None:
            nodes = self.workers()
        target_num_nodes = self.config["num_nodes"]
        suffix = ""
        if self.updaters:
            suffix += " ({} updating)".format(len(self.updaters))
        if self.num_failed_updates:
            suffix += " ({} failed to update)".format(
                len(self.num_failed_updates))
        return "StandardAutoscaler: Have {} / {} target nodes{}".format(
                len(nodes), target_num_nodes, suffix)


def with_head_node_ip(cmds):
    head_ip = services.get_node_ip_address()
    out = []
    for cmd in cmds:
        out.append("export RAY_HEAD_IP={}; {}".format(head_ip, cmd))
    return out


def hash_launch_conf(node_conf):
    hasher = hashlib.sha1()
    hasher.update(json.dumps(node_conf).encode("utf-8"))
    return hasher.hexdigest()


def hash_runtime_conf(file_mounts, init_cmds):
    hasher = hashlib.sha1()
    def filehash(path):
        if os.path.isdir(path):
            return dirhash(path)
        else:
            fh = hashlib.sha1()
            with open(path, 'r') as f:
                fh.update(f.read().encode("utf-8"))
            return fh.hexdigest()
    hasher.update(json.dumps([
        file_mounts, init_cmds,
        [filehash(d) for d in sorted(file_mounts.values())]
    ]).encode("utf-8"))
    return hasher.hexdigest()
