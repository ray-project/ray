from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import boto3
from botocore.config import Config

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME
from ray.ray_constants import BOTO_MAX_RETRIES


class AWSNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        config = Config(retries=dict(max_attempts=BOTO_MAX_RETRIES))
        self.ec2 = boto3.resource(
            "ec2", region_name=provider_config["region"], config=config)

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_nodes = {}

        # Cache of ip lookups. We assume IPs never change once assigned.
        self.internal_ip_cache = {}
        self.external_ip_cache = {}

    def nodes(self, tag_filters):
        filters = [
            {
                "Name": "instance-state-name",
                "Values": ["pending", "running"],
            },
            {
                "Name": "tag:{}".format(TAG_RAY_CLUSTER_NAME),
                "Values": [self.cluster_name],
            },
        ]
        for k, v in tag_filters.items():
            filters.append({
                "Name": "tag:{}".format(k),
                "Values": [v],
            })
        instances = list(self.ec2.instances.filter(Filters=filters))
        self.cached_nodes = {i.id: i for i in instances}
        return [i.id for i in instances]

    def is_running(self, node_id):
        node = self._node(node_id)
        return node.state["Name"] == "running"

    def is_terminated(self, node_id):
        node = self._node(node_id)
        state = node.state["Name"]
        return state not in ["running", "pending"]

    def node_tags(self, node_id):
        node = self._node(node_id)
        tags = {}
        for tag in node.tags:
            tags[tag["Key"]] = tag["Value"]
        return tags

    def external_ip(self, node_id):
        if node_id in self.external_ip_cache:
            return self.external_ip_cache[node_id]
        node = self._node(node_id)
        ip = node.public_ip_address
        if ip:
            self.external_ip_cache[node_id] = ip
        return ip

    def internal_ip(self, node_id):
        if node_id in self.internal_ip_cache:
            return self.internal_ip_cache[node_id]
        node = self._node(node_id)
        ip = node.private_ip_address
        if ip:
            self.internal_ip_cache[node_id] = ip
        return ip

    def set_node_tags(self, node_id, tags):
        node = self._node(node_id)
        tag_pairs = []
        for k, v in tags.items():
            tag_pairs.append({
                "Key": k, "Value": v,
            })
        node.create_tags(Tags=tag_pairs)

    def create_node(self, node_config, tags, count):
        conf = node_config.copy()
        tag_pairs = [{
            "Key": TAG_RAY_CLUSTER_NAME,
            "Value": self.cluster_name,
        }]
        for k, v in tags.items():
            tag_pairs.append(
                {
                    "Key": k,
                    "Value": v,
                })
        conf.update({
            "MinCount": 1,
            "MaxCount": count,
            "TagSpecifications": conf.get("TagSpecifications", []) + [
                {
                    "ResourceType": "instance",
                    "Tags": tag_pairs,
                }
            ]
        })
        self.ec2.create_instances(**conf)

    def terminate_node(self, node_id):
        node = self._node(node_id)
        node.terminate()

    def _node(self, node_id):
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]
        matches = list(self.ec2.instances.filter(InstanceIds=[node_id]))
        assert len(matches) == 1, "Invalid instance id {}".format(node_id)
        return matches[0]
