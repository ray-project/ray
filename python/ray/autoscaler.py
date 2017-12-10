from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import base64
import json
import hashlib
import os
import random
import subprocess
import tempfile

from multiprocessing import Process
from collections import namedtuple

import boto3

from checksumdir import dirhash


def get_autoscaler(config):
    AUTOSCALERS = {
        "aws": AWSAutoscaler,
    }
    return AUTOSCALERS[config["provider"]](config)


DEFAULT_CLUSTER_CONFIG = {
    "provider": "aws",
    "worker_group": "default",
    "worker_group_version": 0,
    "num_nodes": 1,
    "node": {
        "InstanceType": "m4.xlarge",
        "ImageId": "ami-d04396aa",
        "KeyName": "ekl-laptop-thinkpad",
        "SubnetId": "subnet-20f16f0c",
        "SecurityGroupIds": ["sg-f3d40980"],
    },
    "file_mounts": {
        "/home/ubuntu/data": "/home/eric/Desktop/data",
    },
    "init_commands": [
        "/home/ubuntu/.local/bin/ray start --redis-address=OSTNAME:6379",
    ],
}

TAG_RAY_WORKER_GROUP = "ray:WorkerGroup"
TAG_RAY_WORKER_GROUP_VERSION = "ray:WorkerGroupVersion"
TAG_RAY_APPLIED_CONFIG = "ray:AppliedConfig"


def hash_files(config):
    hasher = hashlib.sha1()
    hasher.update(json.dumps([
        config["file_mounts"], config["init_commands"],
        [dirhash(d) for d in sorted(config["file_mounts"].values())]
    ]).encode("utf-8"))
    return base64.encodestring(hasher.digest()).decode("utf-8").strip()


def node_name(config, status):
    return "[{}] ray-worker-{}".format(status, config["worker_group"])


class NodeUpdater(Process):
    def __init__(self, node_id, config, config_hash):
        Process.__init__(self)
        self.ec2 = boto3.resource("ec2")
        matches = list(self.ec2.instances.filter(InstanceIds=[node_id]))
        assert len(matches) == 1, "Invalid instance id"
        self.node = matches[0]
        self.config = config
        self.config_hash = config_hash
        self.logfile = tempfile.NamedTemporaryFile(
            prefix='node-updater-', delete=False)

    def run(self):
        print("AWSAutoscaler: Updating {} to {}, remote logs at {}".format(
            self.node, self.config_hash, self.logfile.name))
        self.node.create_tags(Tags=[{
            "Key": "Name",
            "Value": node_name(self.config, "updating")
        }])
        try:
            self.do_update(self.node)
        except Exception as e:
            print(
                "AWSAutoscaler: Error updating {}, "
                "see {} for remote logs".format(e, self.logfile.name))
            self.node.create_tags(Tags=[{
                "Key": "Name",
                "Value": node_name(self.config, "error")
            }])
            print(
                "----- BEGIN REMOTE LOGS -----" +
                open(self.logfile.name).read() +
                "----- END REMOTE LOGS -----")
            return
        self.node.create_tags(Tags=[
            {
                "Key": "Name",
                "Value": node_name(self.config, "ok")
            },
            {
                "Key": TAG_RAY_APPLIED_CONFIG,
                "Value": self.config_hash,
            },
        ])
        print("AWSAutoscaler: Applied config {} to node {}".format(
            self.config_hash, self.node))

    def do_update(self, node):
        for remote_dir, local_dir in self.config["file_mounts"].items():
            assert os.path.isdir(local_dir)
            subprocess.check_call([
                "rsync", "-e", "ssh -i ~/.ssh/ekl-laptop-thinkpad.pem "
                "-o ConnectTimeout=1s -o StrictHostKeyChecking=no",
                "--delete", "-avz", "{}/".format(local_dir),
                "ubuntu@{}:{}/".format(node.public_ip_address, remote_dir)
            ], stdout=self.logfile, stderr=self.logfile)
        for cmd in self.config["init_commands"]:
            subprocess.check_call([
                "ssh", "-o", "ConnectTimeout=2s",
                "-o", "StrictHostKeyChecking=no",
                "-i", "~/.ssh/ekl-laptop-thinkpad.pem",
                "ubuntu@{}".format(node.public_ip_address),
                cmd,
            ], stdout=self.logfile, stderr=self.logfile)
    

class AWSAutoscaler(object):
    def __init__(self, config=DEFAULT_CLUSTER_CONFIG):
        self.config = config
        self.config_hash = hash_files(config)
        self.ec2 = boto3.resource("ec2")

        # Map from node.id to NodeUpdater processes
        self.updaters = {}

        for local_dir in config["file_mounts"].values():
            assert os.path.isdir(local_dir)

        print("AWSAutoscaler: {}".format(self.config))

    def nodes(self):
        return list(self.ec2.instances.filter(
            Filters=[
                {
                    "Name": "instance-state-name",
                    "Values": ["pending", "running"],
                },
                {
                    "Name": "tag:{}".format(TAG_RAY_WORKER_GROUP),
                    "Values": [self.config["worker_group"]],
                },
            ]))

    def update(self):
        nodes = self.nodes()
        target_num_nodes = self.config["num_nodes"]

        # Terminate nodes while there are too many
        while len(nodes) > target_num_nodes:
            print(
                "AWSAutoscaler: Terminating unneeded node: "
                "{}".format(nodes[-1]))
            nodes[-1].terminate()
            nodes = self.nodes()
            print(self.debug_string())

        if target_num_nodes == 0:
            return

        # Update nodes with out-of-date files
        for node in nodes:
            self.update_if_needed(node)

        # Launch a new node if needed
        if len(nodes) < target_num_nodes:
            print(self.debug_string(nodes))
            self.launch_new_node()
            print(self.debug_string())
            return
        else:
            # If enough nodes, terminate an out-of-date node.
            for node in nodes:
                if not self.version_ok(node):
                    print(
                        "AWSAutoscaler: Terminating outdated node: "
                        "{}".format(node))
                    node.terminate()
                    print(self.debug_string())
                    return

    def version_ok(self, node):
        version = -1
        for tag in node.tags:
            if tag["Key"] == TAG_RAY_WORKER_GROUP_VERSION:
                try:
                    version = int(tag["Value"])
                except ValueError:
                    pass
        if self.config["worker_group_version"] > version:
            print("AWSAutoscaler: Node {} has version {}, required {}".format(
                node, version, self.config["worker_group_version"]))
            return False
        return True

    def files_up_to_date(self, node):
        applied = None
        for tag in node.tags:
            if tag["Key"] == TAG_RAY_APPLIED_CONFIG:
                applied = tag["Value"]
        if applied != self.config_hash:
            print("AWSAutoscaler: {} has file state {}, required {}".format(
                node, applied, self.config_hash))
            return False
        return True

    def update_if_needed(self, node):
        if node.state["Name"] != "running":
            return
        if not self.version_ok(node):
            return
        if node.id in self.updaters:
            return
        if self.files_up_to_date(node):
            return
        updater = NodeUpdater(node.id, self.config, self.config_hash)
        updater.start()
        self.updaters[node.id] = updater

    def launch_new_node(self):
        print("AWSAutoscaler: Launching new node")
        conf = self.config["node"].copy()
        conf.update({
            "MinCount": 1,
            "MaxCount": 1,
            "TagSpecifications": conf.get("TagSpecifications", []) + [
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {
                            "Key": "Name",
                            "Value": node_name(self.config, "starting"),
                        },
                        {
                            "Key": TAG_RAY_WORKER_GROUP,
                            "Value": self.config["worker_group"],
                        },
                        {
                            "Key": TAG_RAY_WORKER_GROUP_VERSION,
                            "Value": str(int(
                                self.config["worker_group_version"])),
                        },
                    ],
                }
            ]
        })
        num_before = len(self.nodes())
        self.ec2.create_instances(**conf)
        # TODO(ekl) be less conservative in this check
        assert len(self.nodes()) > num_before, "Num nodes failed to increase"

    def debug_string(self, nodes=None):
        if nodes is None:
            nodes = self.nodes()
        target_num_nodes = self.config["num_nodes"]
        return "AWSAutoscaler: Have {} / {} target nodes".format(
                len(nodes), target_num_nodes)
