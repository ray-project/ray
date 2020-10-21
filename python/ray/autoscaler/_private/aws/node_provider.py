import random
import copy
import threading
from collections import defaultdict
import logging
import time
from typing import Any, Dict

import boto3
import botocore
from botocore.config import Config

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME, \
    TAG_RAY_LAUNCH_CONFIG, TAG_RAY_NODE_KIND, TAG_RAY_USER_NODE_TYPE
from ray.autoscaler._private.constants import BOTO_MAX_RETRIES, \
    BOTO_CREATE_MAX_RETRIES
from ray.autoscaler._private.aws.config import bootstrap_aws
from ray.autoscaler._private.log_timer import LogTimer

from ray.autoscaler._private.aws.utils import boto_exception_handler
from ray.autoscaler._private.cli_logger import cli_logger, cf

logger = logging.getLogger(__name__)

TAG_BATCH_DELAY = 1


def to_aws_format(tags):
    """Convert the Ray node name tag to the AWS-specific 'Name' tag."""

    if TAG_RAY_NODE_NAME in tags:
        tags["Name"] = tags[TAG_RAY_NODE_NAME]
        del tags[TAG_RAY_NODE_NAME]
    return tags


def from_aws_format(tags):
    """Convert the AWS-specific 'Name' tag to the Ray node name tag."""

    if "Name" in tags:
        tags[TAG_RAY_NODE_NAME] = tags["Name"]
        del tags["Name"]
    return tags


def make_ec2_client(region, max_retries, aws_credentials=None):
    """Make client, retrying requests up to `max_retries`."""
    config = Config(retries={"max_attempts": max_retries})
    aws_credentials = aws_credentials or {}
    return boto3.resource(
        "ec2", region_name=region, config=config, **aws_credentials)


class AWSNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.cache_stopped_nodes = provider_config.get("cache_stopped_nodes",
                                                       True)
        aws_credentials = provider_config.get("aws_credentials")

        self.ec2 = make_ec2_client(
            region=provider_config["region"],
            max_retries=BOTO_MAX_RETRIES,
            aws_credentials=aws_credentials)
        self.ec2_fail_fast = make_ec2_client(
            region=provider_config["region"],
            max_retries=0,
            aws_credentials=aws_credentials)

        # Try availability zones round-robin, starting from random offset
        self.subnet_idx = random.randint(0, 100)

        # Tags that we believe to actually be on EC2.
        self.tag_cache = {}
        # Tags that we will soon upload.
        self.tag_cache_pending = defaultdict(dict)
        # Number of threads waiting for a batched tag update.
        self.batch_thread_count = 0
        self.batch_update_done = threading.Event()
        self.batch_update_done.set()
        self.ready_for_new_batch = threading.Event()
        self.ready_for_new_batch.set()
        self.tag_cache_lock = threading.Lock()
        self.count_lock = threading.Lock()

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_nodes = {}

    def non_terminated_nodes(self, tag_filters):
        # Note that these filters are acceptable because they are set on
        #       node initialization, and so can never be sitting in the cache.
        tag_filters = to_aws_format(tag_filters)
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

        with boto_exception_handler(
                "Failed to fetch running instances from AWS."):
            nodes = list(self.ec2.instances.filter(Filters=filters))

        # Populate the tag cache with initial information if necessary
        for node in nodes:
            if node.id in self.tag_cache:
                continue

            self.tag_cache[node.id] = from_aws_format(
                {x["Key"]: x["Value"]
                 for x in node.tags})

        self.cached_nodes = {node.id: node for node in nodes}
        return [node.id for node in nodes]

    def is_running(self, node_id):
        node = self._get_cached_node(node_id)
        return node.state["Name"] == "running"

    def is_terminated(self, node_id):
        node = self._get_cached_node(node_id)
        state = node.state["Name"]
        return state not in ["running", "pending"]

    def node_tags(self, node_id):
        with self.tag_cache_lock:
            d1 = self.tag_cache[node_id]
            d2 = self.tag_cache_pending.get(node_id, {})
            return dict(d1, **d2)

    def external_ip(self, node_id):
        node = self._get_cached_node(node_id)

        if node.public_ip_address is None:
            node = self._get_node(node_id)

        return node.public_ip_address

    def internal_ip(self, node_id):
        node = self._get_cached_node(node_id)

        if node.private_ip_address is None:
            node = self._get_node(node_id)

        return node.private_ip_address

    def set_node_tags(self, node_id, tags):
        is_batching_thread = False
        with self.tag_cache_lock:
            if not self.tag_cache_pending:
                is_batching_thread = True
                # Wait for threads in the last batch to exit
                self.ready_for_new_batch.wait()
                self.ready_for_new_batch.clear()
                self.batch_update_done.clear()
            self.tag_cache_pending[node_id].update(tags)

        if is_batching_thread:
            time.sleep(TAG_BATCH_DELAY)
            with self.tag_cache_lock:
                self._update_node_tags()
                self.batch_update_done.set()

        with self.count_lock:
            self.batch_thread_count += 1
        self.batch_update_done.wait()

        with self.count_lock:
            self.batch_thread_count -= 1
            if self.batch_thread_count == 0:
                self.ready_for_new_batch.set()

    def _update_node_tags(self):
        batch_updates = defaultdict(list)

        for node_id, tags in self.tag_cache_pending.items():
            for x in tags.items():
                batch_updates[x].append(node_id)
            self.tag_cache[node_id].update(tags)

        self.tag_cache_pending = defaultdict(dict)

        self._create_tags(batch_updates)

    def _create_tags(self, batch_updates):
        for (k, v), node_ids in batch_updates.items():
            m = "Set tag {}={} on {}".format(k, v, node_ids)
            with LogTimer("AWSNodeProvider: {}".format(m)):
                if k == TAG_RAY_NODE_NAME:
                    k = "Name"
                self.ec2.meta.client.create_tags(
                    Resources=node_ids,
                    Tags=[{
                        "Key": k,
                        "Value": v
                    }],
                )

    def create_node(self, node_config, tags, count):
        tags = copy.deepcopy(tags)
        # Try to reuse previously stopped nodes with compatible configs
        if self.cache_stopped_nodes:
            # TODO(ekl) this is breaking the abstraction boundary a little by
            # peeking into the tag set.
            filters = [
                {
                    "Name": "instance-state-name",
                    "Values": ["stopped", "stopping"],
                },
                {
                    "Name": "tag:{}".format(TAG_RAY_CLUSTER_NAME),
                    "Values": [self.cluster_name],
                },
                {
                    "Name": "tag:{}".format(TAG_RAY_NODE_KIND),
                    "Values": [tags[TAG_RAY_NODE_KIND]],
                },
                {
                    "Name": "tag:{}".format(TAG_RAY_LAUNCH_CONFIG),
                    "Values": [tags[TAG_RAY_LAUNCH_CONFIG]],
                },
            ]
            # This tag may not always be present.
            if TAG_RAY_USER_NODE_TYPE in tags:
                filters.append({
                    "Name": "tag:{}".format(TAG_RAY_USER_NODE_TYPE),
                    "Values": [tags[TAG_RAY_USER_NODE_TYPE]],
                })

            reuse_nodes = list(
                self.ec2.instances.filter(Filters=filters))[:count]
            reuse_node_ids = [n.id for n in reuse_nodes]
            if reuse_nodes:
                cli_logger.print(
                    # todo: handle plural vs singular?
                    "Reusing nodes {}. "
                    "To disable reuse, set `cache_stopped_nodes: False` "
                    "under `provider` in the cluster configuration.",
                    cli_logger.render_list(reuse_node_ids))
                cli_logger.old_info(
                    logger, "AWSNodeProvider: reusing instances {}. "
                    "To disable reuse, set "
                    "'cache_stopped_nodes: False' in the provider "
                    "config.", reuse_node_ids)

                # todo: timed?
                with cli_logger.group("Stopping instances to reuse"):
                    for node in reuse_nodes:
                        self.tag_cache[node.id] = from_aws_format(
                            {x["Key"]: x["Value"]
                             for x in node.tags})
                        if node.state["Name"] == "stopping":
                            cli_logger.print("Waiting for instance {} to stop",
                                             node.id)
                            cli_logger.old_info(
                                logger,
                                "AWSNodeProvider: waiting for instance "
                                "{} to fully stop...", node.id)
                            node.wait_until_stopped()

                self.ec2.meta.client.start_instances(
                    InstanceIds=reuse_node_ids)
                for node_id in reuse_node_ids:
                    self.set_node_tags(node_id, tags)
                count -= len(reuse_node_ids)

        if count:
            self._create_node(node_config, tags, count)

    def _create_node(self, node_config, tags, count):
        tags = to_aws_format(tags)
        conf = node_config.copy()

        # Delete unsupported keys from the node config
        try:
            del conf["Resources"]
        except KeyError:
            pass

        tag_pairs = [{
            "Key": TAG_RAY_CLUSTER_NAME,
            "Value": self.cluster_name,
        }]
        for k, v in tags.items():
            tag_pairs.append({
                "Key": k,
                "Value": v,
            })
        tag_specs = [{
            "ResourceType": "instance",
            "Tags": tag_pairs,
        }]
        user_tag_specs = conf.get("TagSpecifications", [])
        # Allow users to add tags and override values of existing
        # tags with their own. This only applies to the resource type
        # "instance". All other resource types are appended to the list of
        # tag specs.
        for user_tag_spec in user_tag_specs:
            if user_tag_spec["ResourceType"] == "instance":
                for user_tag in user_tag_spec["Tags"]:
                    exists = False
                    for tag in tag_specs[0]["Tags"]:
                        if user_tag["Key"] == tag["Key"]:
                            exists = True
                            tag["Value"] = user_tag["Value"]
                            break
                    if not exists:
                        tag_specs[0]["Tags"] += [user_tag]
            else:
                tag_specs += [user_tag_spec]

        # SubnetIds is not a real config key: we must resolve to a
        # single SubnetId before invoking the AWS API.
        subnet_ids = conf.pop("SubnetIds")

        for attempt in range(1, BOTO_CREATE_MAX_RETRIES + 1):
            try:
                subnet_id = subnet_ids[self.subnet_idx % len(subnet_ids)]

                cli_logger.old_info(
                    logger, "NodeProvider: calling create_instances "
                    "with {} (count={}).", subnet_id, count)

                self.subnet_idx += 1
                conf.update({
                    "MinCount": 1,
                    "MaxCount": count,
                    "SubnetId": subnet_id,
                    "TagSpecifications": tag_specs
                })
                created = self.ec2_fail_fast.create_instances(**conf)

                # todo: timed?
                # todo: handle plurality?
                with cli_logger.group(
                        "Launched {} nodes",
                        count,
                        _tags=dict(subnet_id=subnet_id)):
                    for instance in created:
                        # NOTE(maximsmol): This is needed for mocking
                        # boto3 for tests. This is likely a bug in moto
                        # but AWS docs don't seem to say.
                        # You can patch moto/ec2/responses/instances.py
                        # to fix this (add <stateReason> to EC2_RUN_INSTANCES)

                        # The correct value is technically
                        # {"code": "0", "Message": "pending"}
                        state_reason = instance.state_reason or {
                            "Message": "pending"
                        }

                        cli_logger.print(
                            "Launched instance {}",
                            instance.instance_id,
                            _tags=dict(
                                state=instance.state["Name"],
                                info=state_reason["Message"]))
                        cli_logger.old_info(
                            logger, "NodeProvider: Created instance "
                            "[id={}, name={}, info={}]", instance.instance_id,
                            instance.state["Name"], state_reason["Message"])
                break
            except botocore.exceptions.ClientError as exc:
                if attempt == BOTO_CREATE_MAX_RETRIES:
                    # todo: err msg
                    cli_logger.abort(
                        "Failed to launch instances. Max attempts exceeded.")
                    cli_logger.old_error(
                        logger,
                        "create_instances: Max attempts ({}) exceeded.",
                        BOTO_CREATE_MAX_RETRIES)
                    raise exc
                else:
                    cli_logger.print(
                        "create_instances: Attempt failed with {}, retrying.",
                        exc)
                    cli_logger.old_error(logger, exc)

    def terminate_node(self, node_id):
        node = self._get_cached_node(node_id)
        if self.cache_stopped_nodes:
            if node.spot_instance_request_id:
                cli_logger.print(
                    "Terminating instance {} " +
                    cf.dimmed("(cannot stop spot instances, only terminate)"),
                    node_id)  # todo: show node name?

                cli_logger.old_info(
                    logger,
                    "AWSNodeProvider: terminating node {} (spot nodes cannot "
                    "be stopped, only terminated)", node_id)
                node.terminate()
            else:
                cli_logger.print("Stopping instance {} " + cf.dimmed(
                    "(to terminate instead, "
                    "set `cache_stopped_nodes: False` "
                    "under `provider` in the cluster configuration)"),
                                 node_id)  # todo: show node name?

                cli_logger.old_info(
                    logger,
                    "AWSNodeProvider: stopping node {}. To terminate nodes "
                    "on stop, set 'cache_stopped_nodes: False' in the "
                    "provider config.".format(node_id))
                node.stop()
        else:
            node.terminate()

        self.tag_cache.pop(node_id, None)
        self.tag_cache_pending.pop(node_id, None)

    def terminate_nodes(self, node_ids):
        if not node_ids:
            return
        if self.cache_stopped_nodes:
            spot_ids = []
            on_demand_ids = []

            for node_id in node_ids:
                if self._get_cached_node(node_id).spot_instance_request_id:
                    spot_ids += [node_id]
                else:
                    on_demand_ids += [node_id]

            if on_demand_ids:
                # todo: show node names?
                cli_logger.print(
                    "Stopping instances {} " + cf.dimmed(
                        "(to terminate instead, "
                        "set `cache_stopped_nodes: False` "
                        "under `provider` in the cluster configuration)"),
                    cli_logger.render_list(on_demand_ids))
                cli_logger.old_info(
                    logger,
                    "AWSNodeProvider: stopping nodes {}. To terminate nodes "
                    "on stop, set 'cache_stopped_nodes: False' in the "
                    "provider config.", on_demand_ids)

                self.ec2.meta.client.stop_instances(InstanceIds=on_demand_ids)
            if spot_ids:
                cli_logger.print(
                    "Terminating instances {} " +
                    cf.dimmed("(cannot stop spot instances, only terminate)"),
                    cli_logger.render_list(spot_ids))
                cli_logger.old_info(
                    logger,
                    "AWSNodeProvider: terminating nodes {} (spot nodes cannot "
                    "be stopped, only terminated)", spot_ids)

                self.ec2.meta.client.terminate_instances(InstanceIds=spot_ids)
        else:
            self.ec2.meta.client.terminate_instances(InstanceIds=node_ids)

        for node_id in node_ids:
            self.tag_cache.pop(node_id, None)
            self.tag_cache_pending.pop(node_id, None)

    def _get_node(self, node_id):
        """Refresh and get info for this node, updating the cache."""
        self.non_terminated_nodes({})  # Side effect: updates cache

        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        # Node not in {pending, running} -- retry with a point query. This
        # usually means the node was recently preempted or terminated.
        matches = list(self.ec2.instances.filter(InstanceIds=[node_id]))
        assert len(matches) == 1, "Invalid instance id {}".format(node_id)
        return matches[0]

    def _get_cached_node(self, node_id):
        """Return node info from cache if possible, otherwise fetches it."""
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        return self._get_node(node_id)

    @staticmethod
    def bootstrap_config(cluster_config):
        return bootstrap_aws(cluster_config)

    @staticmethod
    def fillout_available_node_types_resources(
            cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Fills out missing "resources" field for available_node_types."""
        if "available_node_types" not in cluster_config:
            return cluster_config
        cluster_config = copy.deepcopy(cluster_config)

        instances_list = boto3.client("ec2").describe_instance_types()[
            "InstanceTypes"]
        instances_dict = {
            instance["InstanceType"]: instance
            for instance in instances_list
        }
        available_node_types = cluster_config["available_node_types"]
        for node_type in available_node_types:
            instance_type = available_node_types[node_type]["node_config"][
                "InstanceType"]
            if instance_type in instances_dict:
                cpus = instances_dict[instance_type]["VCpuInfo"][
                    "DefaultVCpus"]
                autodetected_resources = {"CPU": cpus}
                gpus = instances_dict[instance_type].get("GpuInfo",
                                                         {}).get("Gpus")
                if gpus is not None:
                    # TODO(ameer): currently we support one gpu type per node.
                    assert len(gpus) == 1
                    gpu_name = gpus[0]["Name"]
                    autodetected_resources.update({
                        "GPU": gpus[0]["Count"],
                        f"accelerator_type:{gpu_name}": 1
                    })
                autodetected_resources.update(
                    available_node_types[node_type].get("resources", {}))
                if autodetected_resources != \
                        available_node_types[node_type].get("resources", {}):
                    available_node_types[node_type][
                        "resources"] = autodetected_resources
                    cli_logger.print("Updating the resources of {} to {}.",
                                     node_type, autodetected_resources)

        return cluster_config
