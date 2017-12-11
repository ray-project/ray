from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys

import boto3


def get_node_provider(provider_config, worker_group):
    NODE_PROVIDERS = {
        "aws": AWSNodeProvider,
        "gce": None,  # TODO: support more node providers
        "azure": None,
        "kubernetes": None,
        "docker": None,
        "local_cluster": None,
    }
    provider_cls = NODE_PROVIDERS.get(provider_config["type"])
    if provider_cls is None:
        raise NotImplementedError(
            "Unsupported node provider: {}".format(provider_config["type"]))
    return provider_cls(provider_config, worker_group)


class NodeProvider(object):
    def __init__(self, provider_config, worker_group):
        self.provider_config = provider_config
        self.worker_group = worker_group

    def nodes(self, tag_filters):
        raise NotImplementedError

    def is_running(self, node_id):
        raise NotImplementedError

    def is_terminated(self, node_id):
        raise NotImplementedError

    def node_tags(self, node_id):
        raise NotImplementedError

    def external_ip(self, node_id):
        raise NotImplementedError

    def create_node(self, node_config, tags, count):
        raise NotImplementedError

    def set_node_tags(self, node_id, tags):
        raise NotImplementedError

    def terminate_node(self, node_id):
        raise NotImplementedError


class AWSNodeProvider(NodeProvider):
    def __init__(self, provider_config, worker_group):
        NodeProvider.__init__(self, provider_config, worker_group)
        self.ec2 = boto3.resource("ec2", region_name=provider_config["region"])

    def nodes(self, tag_filters):
        filters = [
            {
                "Name": "instance-state-name",
                "Values": ["pending", "running"],
            },
        ]
        for k, v in tag_filters.items():
            filters.append({
                "Name": "tag:{}".format(k),
                "Values": [v],
            })
        instances = list(self.ec2.instances.filter(Filters=filters))
        return [i.id for i in instances]

    def is_running(self, node_id):
        node = self._node(node_id)
        return node.state["Name"] == "running"

    def is_terminated(self, node_id):
        node = self._node(node_id)
        state = node.state["Name"]
        return state in ["running", "pending"]

    def node_tags(self, node_id):
        node = self._node(node_id)
        tags = {}
        for tag in node.tags:
            tags[tag["Key"]] = tag["Value"]
        return tags

    def external_ip(self, node_id):
        node = self._node(node_id)
        return node.public_ip_address

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
        tag_pairs = []
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
        matches = list(self.ec2.instances.filter(InstanceIds=[node_id]))
        assert len(matches) == 1, "Invalid instance id {}".format(node_id)
        return matches[0]
