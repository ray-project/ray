import copy
import logging
import sys
import threading
import time
from collections import OrderedDict, defaultdict
from typing import Any, Dict, List

import botocore
from boto3.resources.base import ServiceResource

import ray._private.ray_constants as ray_constants
from ray._private.utils import get_neuron_core_constraint_name
from ray.autoscaler._private.aws.cloudwatch.cloudwatch_helper import (
    CLOUDWATCH_AGENT_INSTALLED_AMI_TAG,
    CLOUDWATCH_AGENT_INSTALLED_TAG,
    CloudwatchHelper,
)
from ray.autoscaler._private.aws.config import bootstrap_aws
from ray.autoscaler._private.aws.utils import (
    boto_exception_handler,
    client_cache,
    resource_cache,
)
from ray.autoscaler._private.cli_logger import cf, cli_logger
from ray.autoscaler._private.constants import BOTO_CREATE_MAX_RETRIES, BOTO_MAX_RETRIES
from ray.autoscaler._private.log_timer import LogTimer
from ray.autoscaler.node_launch_exception import NodeLaunchException
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    TAG_RAY_CLUSTER_NAME,
    TAG_RAY_LAUNCH_CONFIG,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_USER_NODE_TYPE,
)

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


def make_ec2_resource(region, max_retries, aws_credentials=None) -> ServiceResource:
    """Make client, retrying requests up to `max_retries`."""
    aws_credentials = aws_credentials or {}
    return resource_cache("ec2", region, max_retries, **aws_credentials)


def list_ec2_instances(
    region: str, aws_credentials: Dict[str, Any] = None
) -> List[Dict[str, Any]]:
    """Get all instance-types/resources available in the user's AWS region.
    Args:
        region: the region of the AWS provider. e.g., "us-west-2".
    Returns:
        final_instance_types: a list of instances. An example of one element in
        the list:
            {'InstanceType': 'm5a.xlarge', 'ProcessorInfo':
            {'SupportedArchitectures': ['x86_64'], 'SustainedClockSpeedInGhz':
            2.5},'VCpuInfo': {'DefaultVCpus': 4, 'DefaultCores': 2,
            'DefaultThreadsPerCore': 2, 'ValidCores': [2],
            'ValidThreadsPerCore': [1, 2]}, 'MemoryInfo': {'SizeInMiB': 16384},
            ...}

    """
    final_instance_types = []
    aws_credentials = aws_credentials or {}
    ec2 = client_cache("ec2", region, BOTO_MAX_RETRIES, **aws_credentials)
    instance_types = ec2.describe_instance_types()
    final_instance_types.extend(copy.deepcopy(instance_types["InstanceTypes"]))
    while "NextToken" in instance_types:
        instance_types = ec2.describe_instance_types(
            NextToken=instance_types["NextToken"]
        )
        final_instance_types.extend(copy.deepcopy(instance_types["InstanceTypes"]))

    return final_instance_types


class AWSNodeProvider(NodeProvider):
    max_terminate_nodes = 1000

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.cache_stopped_nodes = provider_config.get("cache_stopped_nodes", True)
        aws_credentials = provider_config.get("aws_credentials")

        self.ec2 = make_ec2_resource(
            region=provider_config["region"],
            max_retries=BOTO_MAX_RETRIES,
            aws_credentials=aws_credentials,
        )
        self.ec2_fail_fast = make_ec2_resource(
            region=provider_config["region"],
            max_retries=0,
            aws_credentials=aws_credentials,
        )

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
            filters.append(
                {
                    "Name": "tag:{}".format(k),
                    "Values": [v],
                }
            )

        with boto_exception_handler("Failed to fetch running instances from AWS."):
            nodes = list(self.ec2.instances.filter(Filters=filters))

        # Populate the tag cache with initial information if necessary
        for node in nodes:
            if node.id in self.tag_cache:
                continue

            self.tag_cache[node.id] = from_aws_format(
                {x["Key"]: x["Value"] for x in node.tags}
            )

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
                    Tags=[{"Key": k, "Value": v}],
                )

    def create_node(self, node_config, tags, count) -> Dict[str, Any]:
        """Creates instances.

        Returns dict mapping instance id to ec2.Instance object for the created
        instances.
        """
        # sort tags by key to support deterministic unit test stubbing
        tags = OrderedDict(sorted(copy.deepcopy(tags).items()))

        reused_nodes_dict = {}
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
                filters.append(
                    {
                        "Name": "tag:{}".format(TAG_RAY_USER_NODE_TYPE),
                        "Values": [tags[TAG_RAY_USER_NODE_TYPE]],
                    }
                )

            reuse_nodes = list(self.ec2.instances.filter(Filters=filters))[:count]
            reuse_node_ids = [n.id for n in reuse_nodes]
            reused_nodes_dict = {n.id: n for n in reuse_nodes}
            if reuse_nodes:
                cli_logger.print(
                    # todo: handle plural vs singular?
                    "Reusing nodes {}. "
                    "To disable reuse, set `cache_stopped_nodes: False` "
                    "under `provider` in the cluster configuration.",
                    cli_logger.render_list(reuse_node_ids),
                )

                # todo: timed?
                with cli_logger.group("Stopping instances to reuse"):
                    for node in reuse_nodes:
                        self.tag_cache[node.id] = from_aws_format(
                            {x["Key"]: x["Value"] for x in node.tags}
                        )
                        if node.state["Name"] == "stopping":
                            cli_logger.print("Waiting for instance {} to stop", node.id)
                            node.wait_until_stopped()

                self.ec2.meta.client.start_instances(InstanceIds=reuse_node_ids)
                for node_id in reuse_node_ids:
                    self.set_node_tags(node_id, tags)
                count -= len(reuse_node_ids)

        created_nodes_dict = {}
        if count:
            created_nodes_dict = self._create_node(node_config, tags, count)

        all_created_nodes = reused_nodes_dict
        all_created_nodes.update(created_nodes_dict)
        return all_created_nodes

    @staticmethod
    def _merge_tag_specs(
        tag_specs: List[Dict[str, Any]], user_tag_specs: List[Dict[str, Any]]
    ) -> None:
        """
        Merges user-provided node config tag specifications into a base
        list of node provider tag specifications. The base list of
        node provider tag specs is modified in-place.

        This allows users to add tags and override values of existing
        tags with their own, and only applies to the resource type
        "instance". All other resource types are appended to the list of
        tag specs.

        Args:
            tag_specs (List[Dict[str, Any]]): base node provider tag specs
            user_tag_specs (List[Dict[str, Any]]): user's node config tag specs
        """

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

    def _create_node(self, node_config, tags, count):
        created_nodes_dict = {}

        tags = to_aws_format(tags)
        conf = node_config.copy()

        tag_pairs = [
            {
                "Key": TAG_RAY_CLUSTER_NAME,
                "Value": self.cluster_name,
            }
        ]
        for k, v in tags.items():
            tag_pairs.append(
                {
                    "Key": k,
                    "Value": v,
                }
            )
        if CloudwatchHelper.cloudwatch_config_exists(self.provider_config, "agent"):
            cwa_installed = self._check_ami_cwa_installation(node_config)
            if cwa_installed:
                tag_pairs.extend(
                    [
                        {
                            "Key": CLOUDWATCH_AGENT_INSTALLED_TAG,
                            "Value": "True",
                        }
                    ]
                )
        tag_specs = [
            {
                "ResourceType": "instance",
                "Tags": tag_pairs,
            }
        ]
        user_tag_specs = conf.get("TagSpecifications", [])
        AWSNodeProvider._merge_tag_specs(tag_specs, user_tag_specs)

        # SubnetIds is not a real config key: we must resolve to a
        # single SubnetId before invoking the AWS API.
        subnet_ids = conf.pop("SubnetIds")

        # update config with min/max node counts and tag specs
        conf.update({"MinCount": 1, "MaxCount": count, "TagSpecifications": tag_specs})

        # Try to always launch in the first listed subnet.
        subnet_idx = 0
        cli_logger_tags = {}
        # NOTE: This ensures that we try ALL availability zones before
        # throwing an error.
        max_tries = max(BOTO_CREATE_MAX_RETRIES, len(subnet_ids))
        for attempt in range(1, max_tries + 1):
            try:
                if "NetworkInterfaces" in conf:
                    net_ifs = conf["NetworkInterfaces"]
                    # remove security group IDs previously copied from network
                    # interfaces (create_instances call fails otherwise)
                    conf.pop("SecurityGroupIds", None)
                    cli_logger_tags["network_interfaces"] = str(net_ifs)
                else:
                    subnet_id = subnet_ids[subnet_idx % len(subnet_ids)]
                    conf["SubnetId"] = subnet_id
                    cli_logger_tags["subnet_id"] = subnet_id

                created = self.ec2_fail_fast.create_instances(**conf)
                created_nodes_dict = {n.id: n for n in created}

                # todo: timed?
                # todo: handle plurality?
                with cli_logger.group(
                    "Launched {} nodes", count, _tags=cli_logger_tags
                ):
                    for instance in created:
                        # NOTE(maximsmol): This is needed for mocking
                        # boto3 for tests. This is likely a bug in moto
                        # but AWS docs don't seem to say.
                        # You can patch moto/ec2/responses/instances.py
                        # to fix this (add <stateReason> to EC2_RUN_INSTANCES)

                        # The correct value is technically
                        # {"code": "0", "Message": "pending"}
                        state_reason = instance.state_reason or {"Message": "pending"}

                        cli_logger.print(
                            "Launched instance {}",
                            instance.instance_id,
                            _tags=dict(
                                state=instance.state["Name"],
                                info=state_reason["Message"],
                            ),
                        )
                break
            except botocore.exceptions.ClientError as exc:
                # Launch failure may be due to instance type availability in
                # the given AZ
                subnet_idx += 1
                if attempt == max_tries:
                    try:
                        exc = NodeLaunchException(
                            category=exc.response["Error"]["Code"],
                            description=exc.response["Error"]["Message"],
                            src_exc_info=sys.exc_info(),
                        )
                    except Exception:
                        # In theory, all ClientError's we expect to get should
                        # have these fields, but just in case we can't parse
                        # it, it's fine, just throw the original error.
                        logger.warning("Couldn't parse exception.", exc)
                        pass
                    cli_logger.abort(
                        "Failed to launch instances. Max attempts exceeded.",
                        exc=exc,
                    )
                else:
                    cli_logger.warning(
                        "create_instances: Attempt failed with {}, retrying.", exc
                    )

        return created_nodes_dict

    def terminate_node(self, node_id):
        node = self._get_cached_node(node_id)
        if self.cache_stopped_nodes:
            if node.spot_instance_request_id:
                cli_logger.print(
                    "Terminating instance {} "
                    + cf.dimmed("(cannot stop spot instances, only terminate)"),
                    node_id,
                )  # todo: show node name?
                node.terminate()
            else:
                cli_logger.print(
                    "Stopping instance {} "
                    + cf.dimmed(
                        "(to terminate instead, "
                        "set `cache_stopped_nodes: False` "
                        "under `provider` in the cluster configuration)"
                    ),
                    node_id,
                )  # todo: show node name?
                node.stop()
        else:
            node.terminate()

        # TODO (Alex): We are leaking the tag cache here. Naively, we would
        # want to just remove the cache entry here, but terminating can be
        # asyncrhonous or error, which would result in a use after free error.
        # If this leak becomes bad, we can garbage collect the tag cache when
        # the node cache is updated.

    def _check_ami_cwa_installation(self, config):
        response = self.ec2.meta.client.describe_images(ImageIds=[config["ImageId"]])
        cwa_installed = False
        images = response.get("Images")
        if images:
            assert len(images) == 1, (
                f"Expected to find only 1 AMI with the given ID, "
                f"but found {len(images)}."
            )
            image_name = images[0].get("Name", "")
            if CLOUDWATCH_AGENT_INSTALLED_AMI_TAG in image_name:
                cwa_installed = True
        return cwa_installed

    def terminate_nodes(self, node_ids):
        if not node_ids:
            return

        terminate_instances_func = self.ec2.meta.client.terminate_instances
        stop_instances_func = self.ec2.meta.client.stop_instances

        # In some cases, this function stops some nodes, but terminates others.
        # Each of these requires a different EC2 API call. So, we use the
        # "nodes_to_terminate" dict below to keep track of exactly which API
        # call will be used to stop/terminate which set of nodes. The key is
        # the function to use, and the value is the list of nodes to terminate
        # with that function.
        nodes_to_terminate = {terminate_instances_func: [], stop_instances_func: []}

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
                    "Stopping instances {} "
                    + cf.dimmed(
                        "(to terminate instead, "
                        "set `cache_stopped_nodes: False` "
                        "under `provider` in the cluster configuration)"
                    ),
                    cli_logger.render_list(on_demand_ids),
                )

            if spot_ids:
                cli_logger.print(
                    "Terminating instances {} "
                    + cf.dimmed("(cannot stop spot instances, only terminate)"),
                    cli_logger.render_list(spot_ids),
                )

            nodes_to_terminate[stop_instances_func] = on_demand_ids
            nodes_to_terminate[terminate_instances_func] = spot_ids
        else:
            nodes_to_terminate[terminate_instances_func] = node_ids

        max_terminate_nodes = (
            self.max_terminate_nodes
            if self.max_terminate_nodes is not None
            else len(node_ids)
        )

        for terminate_func, nodes in nodes_to_terminate.items():
            for start in range(0, len(nodes), max_terminate_nodes):
                terminate_func(InstanceIds=nodes[start : start + max_terminate_nodes])

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
        cluster_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fills out missing "resources" field for available_node_types."""
        if "available_node_types" not in cluster_config:
            return cluster_config
        cluster_config = copy.deepcopy(cluster_config)

        instances_list = list_ec2_instances(
            cluster_config["provider"]["region"],
            cluster_config["provider"].get("aws_credentials"),
        )
        instances_dict = {
            instance["InstanceType"]: instance for instance in instances_list
        }
        available_node_types = cluster_config["available_node_types"]
        head_node_type = cluster_config["head_node_type"]
        for node_type in available_node_types:
            instance_type = available_node_types[node_type]["node_config"][
                "InstanceType"
            ]
            if instance_type in instances_dict:
                cpus = instances_dict[instance_type]["VCpuInfo"]["DefaultVCpus"]

                autodetected_resources = {"CPU": cpus}
                if node_type != head_node_type:
                    # we only autodetect worker node type memory resource
                    memory_total = instances_dict[instance_type]["MemoryInfo"][
                        "SizeInMiB"
                    ]
                    memory_total = int(memory_total) * 1024 * 1024
                    prop = 1 - ray_constants.DEFAULT_OBJECT_STORE_MEMORY_PROPORTION
                    memory_resources = int(memory_total * prop)
                    autodetected_resources["memory"] = memory_resources

                gpus = instances_dict[instance_type].get("GpuInfo", {}).get("Gpus")
                if gpus is not None:
                    # TODO(ameer): currently we support one gpu type per node.
                    assert len(gpus) == 1
                    gpu_name = gpus[0]["Name"]
                    autodetected_resources.update(
                        {"GPU": gpus[0]["Count"], f"accelerator_type:{gpu_name}": 1}
                    )
                # TODO: AWS SDK (public API) doesn't yet expose the NeuronCore
                #  information. It will be available (work-in-progress)
                #  as xxAcceleratorInfo in InstanceTypeInfo.
                #  https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_InstanceTypeInfo.html
                #  See https://github.com/ray-project/ray/issues/38473
                if (
                    instance_type.lower()
                    in ray_constants.AWS_NEURON_INSTANCE_MAP.keys()
                    and gpus is None
                ):
                    neuron_cores = ray_constants.AWS_NEURON_INSTANCE_MAP.get(
                        instance_type.lower()
                    )
                    autodetected_resources.update(
                        {
                            ray_constants.NEURON_CORES: neuron_cores,
                            get_neuron_core_constraint_name(): neuron_cores,
                        }
                    )

                autodetected_resources.update(
                    available_node_types[node_type].get("resources", {})
                )
                if autodetected_resources != available_node_types[node_type].get(
                    "resources", {}
                ):
                    available_node_types[node_type][
                        "resources"
                    ] = autodetected_resources
                    logger.debug(
                        "Updating the resources of {} to {}.".format(
                            node_type, autodetected_resources
                        )
                    )
            else:
                raise ValueError(
                    "Instance type "
                    + instance_type
                    + " is not available in AWS region: "
                    + cluster_config["provider"]["region"]
                    + "."
                )
        return cluster_config
