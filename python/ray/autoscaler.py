from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import base64
import json
import hashlib
import random
import subprocess

from collections import namedtuple

import boto3


def get_autoscaler(config):
    AUTOSCALERS = {
        "aws": AWSAutoscaler,
    }
    return AUTOSCALERS[config["provider"]](config)


DEFAULT_CLUSTER_CONFIG = {
    "provider": "aws",
    "worker_group": "default",
    "worker_group_version": 3,
    "num_nodes": 2,
    "node": {
        "InstanceType": "m4.2xlarge",
        "ImageId": "ami-d04396aa",
        "KeyName": "ekl-laptop-thinkpad",
        "SubnetId": "subnet-20f16f0c",
        "SecurityGroupIds": ["sg-f3d40980"],
    },
    "file_mounts": {
    },
    "init_commands": [
        "cd /home/ubuntu/ray-1 && git fetch upstream && "
        "git reset --hard upstream/master",
    ],
}

TAG_RAY_WORKER_GROUP = "ray:WorkerGroup"
TAG_RAY_WORKER_GROUP_VERSION = "ray:WorkerGroupVersion"
TAG_RAY_APPLIED_CONFIG = "ray:AppliedConfig"


class AWSAutoscaler(object):
    def __init__(self, config=DEFAULT_CLUSTER_CONFIG):
        self.config = config
        hasher = hashlib.sha1()
        hasher.update(json.dumps([
            config["file_mounts"], config["init_commands"],
        ]).encode("utf-8"))
        self.config_hash = base64.encodestring(
            hasher.digest()).decode("utf-8").strip()
        self.ec2 = boto3.resource("ec2")

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

        # Update nodes with out-of-date files
        for node in nodes:
            if node.state["Name"] == "running":
                if self.version_ok(node) and not self.files_up_to_date(node):
                    self.update_node(node)
        target_num_nodes = self.config["num_nodes"]

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
            # If too many nodes, terminate one at random.
            if len(nodes) > target_num_nodes:
                random.shuffle(nodes)
                print(
                    "AWSAutoscaler: Terminating unneeded node: "
                    "{}".format(nodes[0]))
                print(self.debug_string())
                nodes[0].terminate()

    def debug_string(self, nodes=None):
        if nodes is None:
            nodes = self.nodes()
        target_num_nodes = self.config["num_nodes"]
        return "AWSAutoscaler: have {} / {} target nodes".format(
                len(nodes), target_num_nodes)

    def version_ok(self, node):
        version = None
        for tag in node.tags:
            if tag["Key"] == TAG_RAY_WORKER_GROUP_VERSION:
                version = tag["Value"]
        if str(self.config["worker_group_version"]) != version:
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
            print("AWSAutoscaler: Node {} has config {}, required {}".format(
                node, applied, self.config_hash))
            return False
        return True

    def update_node(self, node):
        print("AWSAutoscaler: Updating node {} to {}".format(
            node, self.config_hash))
        try:
            self.do_update(node)
        except Exception as e:
            print("Error updating {}, retrying.".format(e))
            return
        node.create_tags(Tags=[{
            "Key": TAG_RAY_APPLIED_CONFIG,
            "Value": self.config_hash,
        }])
        print("AWSAutoscaler: Applied config {} to node {}".format(
            self.config_hash, node))

    def do_update(self, node):
        for cmd in self.config["init_commands"]:
            subprocess.check_call([
                "ssh", "-o", "ConnectTimeout=1s", "-o", "StrictHostKeyChecking=no",
                "-i", "~/.ssh/ekl-laptop-thinkpad.pem",
                "ubuntu@{}".format(node.public_ip_address),
                cmd,
            ])
    
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
                            "Value": "ray-worker-{}".format(
                                self.config["worker_group"]),
                        },
                        {
                            "Key": TAG_RAY_WORKER_GROUP,
                            "Value": self.config["worker_group"],
                        },
                        {
                            "Key": TAG_RAY_WORKER_GROUP_VERSION,
                            "Value": str(self.config["worker_group_version"]),
                        },
                    ],
                }
            ]
        })
        num_before = len(self.nodes())
        self.ec2.create_instances(**conf)
        # TODO(ekl) be less conservative in this check
        assert len(self.nodes()) > num_before, "Num nodes failed to increase"
