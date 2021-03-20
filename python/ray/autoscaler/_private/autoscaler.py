from collections import defaultdict, namedtuple, Counter
from typing import Any, Optional, Dict, List
from urllib3.exceptions import MaxRetryError
import copy
import logging
import math
import operator
import os
import subprocess
import threading
import time
import yaml
import collections

from ray.autoscaler.tags import (
    TAG_RAY_LAUNCH_CONFIG, TAG_RAY_RUNTIME_CONFIG,
    TAG_RAY_FILE_MOUNTS_CONTENTS, TAG_RAY_NODE_STATUS, TAG_RAY_NODE_KIND,
    TAG_RAY_USER_NODE_TYPE, STATUS_UNINITIALIZED, STATUS_WAITING_FOR_SSH,
    STATUS_SYNCING_FILES, STATUS_SETTING_UP, STATUS_UP_TO_DATE,
    NODE_KIND_WORKER, NODE_KIND_UNMANAGED, NODE_KIND_HEAD)
from ray.autoscaler._private.event_summarizer import EventSummarizer
from ray.autoscaler._private.legacy_info_string import legacy_log_info_string
from ray.autoscaler._private.providers import _get_node_provider
from ray.autoscaler._private.updater import NodeUpdaterThread
from ray.autoscaler._private.node_launcher import NodeLauncher
from ray.autoscaler._private.node_tracker import NodeTracker
from ray.autoscaler._private.resource_demand_scheduler import \
    get_bin_pack_residual, ResourceDemandScheduler, NodeType, NodeID, NodeIP, \
    ResourceDict
from ray.autoscaler._private.util import ConcurrentCounter, validate_config, \
    with_head_node_ip, hash_launch_conf, hash_runtime_conf, \
    format_info_string
from ray.autoscaler._private.constants import \
    AUTOSCALER_MAX_NUM_FAILURES, AUTOSCALER_MAX_LAUNCH_BATCH, \
    AUTOSCALER_MAX_CONCURRENT_LAUNCHES, AUTOSCALER_UPDATE_INTERVAL_S, \
    AUTOSCALER_HEARTBEAT_TIMEOUT_S
from six.moves import queue

logger = logging.getLogger(__name__)

# Tuple of modified fields for the given node_id returned by should_update
# that will be passed into a NodeUpdaterThread.
UpdateInstructions = namedtuple(
    "UpdateInstructions",
    ["node_id", "setup_commands", "ray_start_commands", "docker_config"])

AutoscalerSummary = namedtuple(
    "AutoscalerSummary",
    ["active_nodes", "pending_nodes", "pending_launches", "failed_nodes"])


class StandardAutoscaler:
    """The autoscaling control loop for a Ray cluster.

    There are two ways to start an autoscaling cluster: manually by running
    `ray start --head --autoscaling-config=/path/to/config.yaml` on a instance
    that has permission to launch other instances, or you can also use `ray up
    /path/to/config.yaml` from your laptop, which will configure the right
    AWS/Cloud roles automatically. See the documentation for a full definition
    of autoscaling behavior:
    https://docs.ray.io/en/master/cluster/autoscaling.html
    StandardAutoscaler's `update` method is periodically called in
    `monitor.py`'s monitoring loop.

    StandardAutoscaler is also used to bootstrap clusters (by adding workers
    until the cluster size that can handle the resource demand is met).
    """

    def __init__(self,
                 config_path,
                 load_metrics,
                 max_launch_batch=AUTOSCALER_MAX_LAUNCH_BATCH,
                 max_concurrent_launches=AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
                 max_failures=AUTOSCALER_MAX_NUM_FAILURES,
                 process_runner=subprocess,
                 update_interval_s=AUTOSCALER_UPDATE_INTERVAL_S,
                 prefix_cluster_info=False,
                 event_summarizer=None):
        self.config_path = config_path
        # Prefix each line of info string with cluster name if True
        self.prefix_cluster_info = prefix_cluster_info
        # Keep this before self.reset (self.provider needs to be created
        # exactly once).
        self.provider = None
        self.resource_demand_scheduler = None
        self.reset(errors_fatal=True)
        self.head_node_ip = load_metrics.local_ip
        self.load_metrics = load_metrics

        self.max_failures = max_failures
        self.max_launch_batch = max_launch_batch
        self.max_concurrent_launches = max_concurrent_launches
        self.process_runner = process_runner
        self.event_summarizer = event_summarizer or EventSummarizer()

        # Map from node_id to NodeUpdater processes
        self.updaters = {}
        self.num_failed_updates = defaultdict(int)
        self.num_successful_updates = defaultdict(int)
        self.num_failures = 0
        self.last_update_time = 0.0
        self.update_interval_s = update_interval_s

        # Node launchers
        self.launch_queue = queue.Queue()
        self.pending_launches = ConcurrentCounter()
        max_batches = math.ceil(
            max_concurrent_launches / float(max_launch_batch))
        for i in range(int(max_batches)):
            node_launcher = NodeLauncher(
                provider=self.provider,
                queue=self.launch_queue,
                index=i,
                pending=self.pending_launches,
                node_types=self.available_node_types,
            )
            node_launcher.daemon = True
            node_launcher.start()

        # NodeTracker maintains soft state to track the number of recently
        # failed nodes. It is best effort only.
        self.node_tracker = NodeTracker()

        # Expand local file_mounts to allow ~ in the paths. This can't be done
        # earlier when the config is written since we might be on different
        # platform and the expansion would result in wrong path.
        self.config["file_mounts"] = {
            remote: os.path.expanduser(local)
            for remote, local in self.config["file_mounts"].items()
        }

        for local_path in self.config["file_mounts"].values():
            assert os.path.exists(local_path)

        logger.info("StandardAutoscaler: {}".format(self.config))

    def update(self):
        try:
            self.reset(errors_fatal=False)
            self._update()
        except Exception as e:
            logger.exception("StandardAutoscaler: "
                             "Error during autoscaling.")
            # Don't abort the autoscaler if the K8s API server is down.
            # https://github.com/ray-project/ray/issues/12255
            is_k8s_connection_error = (
                self.config["provider"]["type"] == "kubernetes"
                and isinstance(e, MaxRetryError))
            if not is_k8s_connection_error:
                self.num_failures += 1
            if self.num_failures > self.max_failures:
                logger.critical("StandardAutoscaler: "
                                "Too many errors, abort.")
                raise e

    def _update(self):
        now = time.time()
        # Throttle autoscaling updates to this interval to avoid exceeding
        # rate limits on API calls.
        if now - self.last_update_time < self.update_interval_s:
            return

        self.last_update_time = now
        nodes = self.workers()

        self.load_metrics.prune_active_ips([
            self.provider.internal_ip(node_id)
            for node_id in self.all_workers()
        ])

        # Terminate any idle or out of date nodes
        last_used = self.load_metrics.last_used_time_by_ip
        horizon = now - (60 * self.config["idle_timeout_minutes"])

        nodes_to_terminate: Dict[NodeID, bool] = []
        node_type_counts = collections.defaultdict(int)
        # Sort based on last used to make sure to keep min_workers that
        # were most recently used. Otherwise, _keep_min_workers_of_node_type
        # might keep a node that should be terminated.
        sorted_node_ids = self._sort_based_on_last_used(nodes, last_used)
        # Don't terminate nodes needed by request_resources()
        nodes_allowed_to_terminate: Dict[NodeID, bool] = {}
        if self.load_metrics.get_resource_requests():
            nodes_allowed_to_terminate = self._get_nodes_allowed_to_terminate(
                sorted_node_ids)

        for node_id in sorted_node_ids:
            # Make sure to not kill idle node types if the number of workers
            # of that type is lower/equal to the min_workers of that type
            # or it is needed for request_resources().
            if (self._keep_min_worker_of_node_type(node_id, node_type_counts)
                    or not nodes_allowed_to_terminate.get(
                        node_id, True)) and self.launch_config_ok(node_id):
                continue

            node_ip = self.provider.internal_ip(node_id)
            if node_ip in last_used and last_used[node_ip] < horizon:
                logger.info("StandardAutoscaler: "
                            "{}: Terminating idle node.".format(node_id))
                self.event_summarizer.add(
                    "Removing {} nodes of type " + self._get_node_type(node_id)
                    + " (idle).",
                    quantity=1,
                    aggregate=operator.add)
                nodes_to_terminate.append(node_id)
            elif not self.launch_config_ok(node_id):
                logger.info("StandardAutoscaler: "
                            "{}: Terminating outdated node.".format(node_id))
                self.event_summarizer.add(
                    "Removing {} nodes of type " + self._get_node_type(node_id)
                    + " (outdated).",
                    quantity=1,
                    aggregate=operator.add)
                nodes_to_terminate.append(node_id)

        if nodes_to_terminate:
            self.provider.terminate_nodes(nodes_to_terminate)
            for node in nodes_to_terminate:
                self.node_tracker.untrack(node)
            nodes = self.workers()

        # Terminate nodes if there are too many
        nodes_to_terminate = []
        while (len(nodes) -
               len(nodes_to_terminate)) > self.config["max_workers"] and nodes:
            to_terminate = nodes.pop()
            logger.info("StandardAutoscaler: "
                        "{}: Terminating unneeded node.".format(to_terminate))
            self.event_summarizer.add(
                "Removing {} nodes of type " +
                self._get_node_type(to_terminate) + " (max workers).",
                quantity=1,
                aggregate=operator.add)
            nodes_to_terminate.append(to_terminate)

        if nodes_to_terminate:
            self.provider.terminate_nodes(nodes_to_terminate)
            for node in nodes_to_terminate:
                self.node_tracker.untrack(node)
            nodes = self.workers()

        to_launch = self.resource_demand_scheduler.get_nodes_to_launch(
            self.provider.non_terminated_nodes(tag_filters={}),
            self.pending_launches.breakdown(),
            self.load_metrics.get_resource_demand_vector(),
            self.load_metrics.get_resource_utilization(),
            self.load_metrics.get_pending_placement_groups(),
            self.load_metrics.get_static_node_resources_by_ip(),
            ensure_min_cluster_size=self.load_metrics.get_resource_requests())
        for node_type, count in to_launch.items():
            self.launch_new_node(count, node_type=node_type)

        nodes = self.workers()

        # Process any completed updates
        completed = []
        for node_id, updater in self.updaters.items():
            if not updater.is_alive():
                completed.append(node_id)
        if completed:
            nodes_to_terminate: List[NodeID] = []
            for node_id in completed:
                if self.updaters[node_id].exitcode == 0:
                    self.num_successful_updates[node_id] += 1
                    # Mark the node as active to prevent the node recovery
                    # logic immediately trying to restart Ray on the new node.
                    self.load_metrics.mark_active(
                        self.provider.internal_ip(node_id))
                else:
                    logger.error(f"StandardAutoscaler: {node_id}: Terminating "
                                 "failed to setup/initialize node.")
                    self.event_summarizer.add(
                        "Removing {} nodes of type " +
                        self._get_node_type(node_id) + " (launch failed).",
                        quantity=1,
                        aggregate=operator.add)
                    nodes_to_terminate.append(node_id)
                    self.num_failed_updates[node_id] += 1
                del self.updaters[node_id]
            if nodes_to_terminate:
                self.provider.terminate_nodes(nodes_to_terminate)

            nodes = self.workers()

        # Update nodes with out-of-date files.
        # TODO(edoakes): Spawning these threads directly seems to cause
        # problems. They should at a minimum be spawned as daemon threads.
        # See https://github.com/ray-project/ray/pull/5903 for more info.
        T = []
        for node_id, setup_commands, ray_start_commands, docker_config in (
                self.should_update(node_id) for node_id in nodes):
            if node_id is not None:
                resources = self._node_resources(node_id)
                logger.debug(f"{node_id}: Starting new thread runner.")
                T.append(
                    threading.Thread(
                        target=self.spawn_updater,
                        args=(node_id, setup_commands, ray_start_commands,
                              resources, docker_config)))
        for t in T:
            t.start()
        for t in T:
            t.join()

        # Attempt to recover unhealthy nodes
        for node_id in nodes:
            self.recover_if_needed(node_id, now)

        logger.info(self.info_string())
        legacy_log_info_string(self, nodes)

    def _sort_based_on_last_used(self, nodes: List[NodeID],
                                 last_used: Dict[str, float]) -> List[NodeID]:
        """Sort the nodes based on the last time they were used.

        The first item in the return list is the most recently used.
        """
        last_used_copy = copy.deepcopy(last_used)
        # Add the unconnected nodes as the least recently used (the end of
        # list). This prioritizes connected nodes.
        least_recently_used = -1

        def last_time_used(node_id: NodeID):
            node_ip = self.provider.internal_ip(node_id)
            if node_ip not in last_used_copy:
                return least_recently_used
            else:
                return last_used_copy[node_ip]

        return sorted(nodes, key=last_time_used, reverse=True)

    def _get_nodes_allowed_to_terminate(
            self, sorted_node_ids: List[NodeID]) -> Dict[NodeID, bool]:
        # TODO(ameer): try merging this with resource_demand_scheduler
        # code responsible for adding nodes for request_resources().
        """Returns the nodes allowed to terminate for request_resources().

        Args:
            sorted_node_ids: the node ids sorted based on last used (LRU last).

        Returns:
            nodes_allowed_to_terminate: whether the node id is allowed to
                terminate or not.
        """
        nodes_allowed_to_terminate: Dict[NodeID, bool] = {}
        head_node_resources: ResourceDict = copy.deepcopy(
            self.available_node_types[self.config["head_node_type"]][
                "resources"])
        if not head_node_resources:
            # Legacy yaml might include {} in the resources field.
            # TODO(ameer): this is somewhat duplicated in
            # resource_demand_scheduler.py.
            head_id: List[NodeID] = self.provider.non_terminated_nodes({
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD
            })
            if head_id:
                head_ip = self.provider.internal_ip(head_id[0])
                static_nodes: Dict[
                    NodeIP,
                    ResourceDict] = \
                    self.load_metrics.get_static_node_resources_by_ip()
                head_node_resources = static_nodes.get(head_ip, {})
            else:
                head_node_resources = {}

        max_node_resources: List[ResourceDict] = [head_node_resources]
        resource_demand_vector_worker_node_ids = []
        # Get max resources on all the non terminated nodes.
        for node_id in sorted_node_ids:
            tags = self.provider.node_tags(node_id)
            if TAG_RAY_USER_NODE_TYPE in tags:
                node_type = tags[TAG_RAY_USER_NODE_TYPE]
                node_resources: ResourceDict = copy.deepcopy(
                    self.available_node_types[node_type]["resources"])
                if not node_resources:
                    # Legacy yaml might include {} in the resources field.
                    static_nodes: Dict[
                        NodeIP,
                        ResourceDict] = \
                            self.load_metrics.get_static_node_resources_by_ip()
                    node_ip = self.provider.internal_ip(node_id)
                    node_resources = static_nodes.get(node_ip, {})
                max_node_resources.append(node_resources)
                resource_demand_vector_worker_node_ids.append(node_id)
        # Since it is sorted based on last used, we "keep" nodes that are
        # most recently used when we binpack. We assume get_bin_pack_residual
        # is following the given order here.
        used_resource_requests: List[ResourceDict]
        _, used_resource_requests = \
            get_bin_pack_residual(max_node_resources,
                                  self.load_metrics.get_resource_requests())
        # Remove the first entry (the head node).
        max_node_resources.pop(0)
        # Remove the first entry (the head node).
        used_resource_requests.pop(0)
        for i, node_id in enumerate(resource_demand_vector_worker_node_ids):
            if used_resource_requests[i] == max_node_resources[i] \
                    and max_node_resources[i]:
                # No resources of the node were needed for request_resources().
                # max_node_resources[i] is an empty dict for legacy yamls
                # before the node is connected.
                nodes_allowed_to_terminate[node_id] = True
            else:
                nodes_allowed_to_terminate[node_id] = False
        return nodes_allowed_to_terminate

    def _keep_min_worker_of_node_type(
            self, node_id: NodeID,
            node_type_counts: Dict[NodeType, int]) -> bool:
        """Returns if workers of node_type can be terminated.
        The worker cannot be terminated to respect min_workers constraint.

        Receives the counters of running nodes so far and determines if idle
        node_id should be terminated or not. It also updates the counters
        (node_type_counts), which is returned by reference.

        Args:
            node_type_counts(Dict[NodeType, int]): The non_terminated node
                types counted so far.
        Returns:
            bool: if workers of node_types can be terminated or not.
        """
        tags = self.provider.node_tags(node_id)
        if TAG_RAY_USER_NODE_TYPE in tags:
            node_type = tags[TAG_RAY_USER_NODE_TYPE]
            node_type_counts[node_type] += 1
            min_workers = self.available_node_types[node_type].get(
                "min_workers", 0)
            max_workers = self.available_node_types[node_type].get(
                "max_workers", 0)
            if node_type_counts[node_type] <= min(min_workers, max_workers):
                return True

        return False

    def _node_resources(self, node_id):
        node_type = self.provider.node_tags(node_id).get(
            TAG_RAY_USER_NODE_TYPE)
        if self.available_node_types:
            return self.available_node_types.get(node_type, {}).get(
                "resources", {})
        else:
            return {}

    def reset(self, errors_fatal=False):
        sync_continuously = False
        if hasattr(self, "config"):
            sync_continuously = self.config.get(
                "file_mounts_sync_continuously", False)
        try:
            with open(self.config_path) as f:
                new_config = yaml.safe_load(f.read())
            if new_config != getattr(self, "config", None):
                try:
                    validate_config(new_config)
                except Exception as e:
                    logger.debug(
                        "Cluster config validation failed. The version of "
                        "the ray CLI you launched this cluster with may "
                        "be higher than the version of ray being run on "
                        "the cluster. Some new features may not be "
                        "available until you upgrade ray on your cluster.",
                        exc_info=e)
            (new_runtime_hash,
             new_file_mounts_contents_hash) = hash_runtime_conf(
                 new_config["file_mounts"],
                 new_config["cluster_synced_files"],
                 [
                     new_config["worker_setup_commands"],
                     new_config["worker_start_ray_commands"],
                 ],
                 generate_file_mounts_contents_hash=sync_continuously,
             )
            self.config = new_config
            self.runtime_hash = new_runtime_hash
            self.file_mounts_contents_hash = new_file_mounts_contents_hash
            if not self.provider:
                self.provider = _get_node_provider(self.config["provider"],
                                                   self.config["cluster_name"])

            self.available_node_types = self.config["available_node_types"]
            upscaling_speed = self.config.get("upscaling_speed")
            aggressive = self.config.get("autoscaling_mode") == "aggressive"
            target_utilization_fraction = self.config.get(
                "target_utilization_fraction")
            if upscaling_speed:
                upscaling_speed = float(upscaling_speed)
            # TODO(ameer): consider adding (if users ask) an option of
            # initial_upscaling_num_workers.
            elif aggressive:
                upscaling_speed = 99999
                logger.warning(
                    "Legacy aggressive autoscaling mode "
                    "detected. Replacing it by setting upscaling_speed to "
                    "99999.")
            elif target_utilization_fraction:
                upscaling_speed = (
                    1 / max(target_utilization_fraction, 0.001) - 1)
                logger.warning(
                    "Legacy target_utilization_fraction config "
                    "detected. Replacing it by setting upscaling_speed to " +
                    "1 / target_utilization_fraction - 1.")
            else:
                upscaling_speed = 1.0
            if self.resource_demand_scheduler:
                # The node types are autofilled internally for legacy yamls,
                # overwriting the class will remove the inferred node resources
                # for legacy yamls.
                self.resource_demand_scheduler.reset_config(
                    self.provider, self.available_node_types,
                    self.config["max_workers"], self.config["head_node_type"],
                    upscaling_speed)
            else:
                self.resource_demand_scheduler = ResourceDemandScheduler(
                    self.provider, self.available_node_types,
                    self.config["max_workers"], self.config["head_node_type"],
                    upscaling_speed)

        except Exception as e:
            if errors_fatal:
                raise e
            else:
                logger.exception("StandardAutoscaler: "
                                 "Error parsing config.")

    def launch_config_ok(self, node_id):
        node_tags = self.provider.node_tags(node_id)
        tag_launch_conf = node_tags.get(TAG_RAY_LAUNCH_CONFIG)
        node_type = node_tags.get(TAG_RAY_USER_NODE_TYPE)

        launch_config = copy.deepcopy(self.config["worker_nodes"])
        if node_type:
            launch_config.update(
                self.config["available_node_types"][node_type]["node_config"])
        calculated_launch_hash = hash_launch_conf(launch_config,
                                                  self.config["auth"])

        if calculated_launch_hash != tag_launch_conf:
            return False
        return True

    def files_up_to_date(self, node_id):
        node_tags = self.provider.node_tags(node_id)
        applied_config_hash = node_tags.get(TAG_RAY_RUNTIME_CONFIG)
        applied_file_mounts_contents_hash = node_tags.get(
            TAG_RAY_FILE_MOUNTS_CONTENTS)
        if (applied_config_hash != self.runtime_hash
                or (self.file_mounts_contents_hash is not None
                    and self.file_mounts_contents_hash !=
                    applied_file_mounts_contents_hash)):
            logger.info("StandardAutoscaler: "
                        "{}: Runtime state is ({},{}), want ({},{})".format(
                            node_id, applied_config_hash,
                            applied_file_mounts_contents_hash,
                            self.runtime_hash, self.file_mounts_contents_hash))
            return False
        return True

    def recover_if_needed(self, node_id, now):
        if not self.can_update(node_id):
            return
        key = self.provider.internal_ip(node_id)

        if key in self.load_metrics.last_heartbeat_time_by_ip:
            last_heartbeat_time = self.load_metrics.last_heartbeat_time_by_ip[
                key]
            delta = now - last_heartbeat_time
            if delta < AUTOSCALER_HEARTBEAT_TIMEOUT_S:
                return

        logger.warning("StandardAutoscaler: "
                       "{}: No recent heartbeat, "
                       "restarting Ray to recover...".format(node_id))
        self.event_summarizer.add(
            "Restarting {} nodes of type " + self._get_node_type(node_id) +
            " (lost contact with raylet).",
            quantity=1,
            aggregate=operator.add)
        updater = NodeUpdaterThread(
            node_id=node_id,
            provider_config=self.config["provider"],
            provider=self.provider,
            auth_config=self.config["auth"],
            cluster_name=self.config["cluster_name"],
            file_mounts={},
            initialization_commands=[],
            setup_commands=[],
            ray_start_commands=with_head_node_ip(
                self.config["worker_start_ray_commands"], self.head_node_ip),
            runtime_hash=self.runtime_hash,
            file_mounts_contents_hash=self.file_mounts_contents_hash,
            process_runner=self.process_runner,
            use_internal_ip=True,
            is_head_node=False,
            docker_config=self.config.get("docker"),
            node_resources=self._node_resources(node_id))
        updater.start()
        self.updaters[node_id] = updater

    def _get_node_type(self, node_id: str) -> str:
        node_tags = self.provider.node_tags(node_id)
        if TAG_RAY_USER_NODE_TYPE in node_tags:
            return node_tags[TAG_RAY_USER_NODE_TYPE]
        else:
            return "unknown"

    def _get_node_type_specific_fields(self, node_id: str,
                                       fields_key: str) -> Any:
        fields = self.config[fields_key]
        node_tags = self.provider.node_tags(node_id)
        if TAG_RAY_USER_NODE_TYPE in node_tags:
            node_type = node_tags[TAG_RAY_USER_NODE_TYPE]
            if node_type not in self.available_node_types:
                raise ValueError(f"Unknown node type tag: {node_type}.")
            node_specific_config = self.available_node_types[node_type]
            if fields_key in node_specific_config:
                fields = node_specific_config[fields_key]
        return fields

    def _get_node_specific_docker_config(self, node_id):
        if "docker" not in self.config:
            return {}
        docker_config = copy.deepcopy(self.config.get("docker", {}))
        node_specific_docker = self._get_node_type_specific_fields(
            node_id, "docker")
        docker_config.update(node_specific_docker)
        return docker_config

    def should_update(self, node_id):
        if not self.can_update(node_id):
            return UpdateInstructions(None, None, None, None)  # no update

        status = self.provider.node_tags(node_id).get(TAG_RAY_NODE_STATUS)
        if status == STATUS_UP_TO_DATE and self.files_up_to_date(node_id):
            return UpdateInstructions(None, None, None, None)  # no update

        successful_updated = self.num_successful_updates.get(node_id, 0) > 0
        if successful_updated and self.config.get("restart_only", False):
            setup_commands = []
            ray_start_commands = self.config["worker_start_ray_commands"]
        elif successful_updated and self.config.get("no_restart", False):
            setup_commands = self._get_node_type_specific_fields(
                node_id, "worker_setup_commands")
            ray_start_commands = []
        else:
            setup_commands = self._get_node_type_specific_fields(
                node_id, "worker_setup_commands")
            ray_start_commands = self.config["worker_start_ray_commands"]

        docker_config = self._get_node_specific_docker_config(node_id)
        return UpdateInstructions(
            node_id=node_id,
            setup_commands=setup_commands,
            ray_start_commands=ray_start_commands,
            docker_config=docker_config)

    def spawn_updater(self, node_id, setup_commands, ray_start_commands,
                      node_resources, docker_config):
        logger.info(f"Creating new (spawn_updater) updater thread for node"
                    f" {node_id}.")
        ip = self.provider.internal_ip(node_id)
        node_type = self._get_node_type(node_id)
        self.node_tracker.track(node_id, ip, node_type)
        updater = NodeUpdaterThread(
            node_id=node_id,
            provider_config=self.config["provider"],
            provider=self.provider,
            auth_config=self.config["auth"],
            cluster_name=self.config["cluster_name"],
            file_mounts=self.config["file_mounts"],
            initialization_commands=with_head_node_ip(
                self._get_node_type_specific_fields(
                    node_id, "initialization_commands"), self.head_node_ip),
            setup_commands=with_head_node_ip(setup_commands,
                                             self.head_node_ip),
            ray_start_commands=with_head_node_ip(ray_start_commands,
                                                 self.head_node_ip),
            runtime_hash=self.runtime_hash,
            file_mounts_contents_hash=self.file_mounts_contents_hash,
            is_head_node=False,
            cluster_synced_files=self.config["cluster_synced_files"],
            rsync_options={
                "rsync_exclude": self.config.get("rsync_exclude"),
                "rsync_filter": self.config.get("rsync_filter")
            },
            process_runner=self.process_runner,
            use_internal_ip=True,
            docker_config=docker_config,
            node_resources=node_resources)
        updater.start()
        self.updaters[node_id] = updater

    def can_update(self, node_id):
        if node_id in self.updaters:
            return False
        if not self.launch_config_ok(node_id):
            return False
        if self.num_failed_updates.get(node_id, 0) > 0:  # TODO(ekl) retry?
            return False
        logger.debug(f"{node_id} is not being updated and "
                     "passes config check (can_update=True).")
        return True

    def launch_new_node(self, count: int, node_type: Optional[str]) -> None:
        logger.info(
            "StandardAutoscaler: Queue {} new nodes for launch".format(count))
        self.event_summarizer.add(
            "Adding {} nodes of type " + str(node_type) + ".",
            quantity=count,
            aggregate=operator.add)
        self.pending_launches.inc(node_type, count)
        config = copy.deepcopy(self.config)
        # Split into individual launch requests of the max batch size.
        while count > 0:
            self.launch_queue.put((config, min(count, self.max_launch_batch),
                                   node_type))
            count -= self.max_launch_batch

    def all_workers(self):
        return self.workers() + self.unmanaged_workers()

    def workers(self):
        return self.provider.non_terminated_nodes(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})

    def unmanaged_workers(self):
        return self.provider.non_terminated_nodes(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_UNMANAGED})

    def kill_workers(self):
        logger.error("StandardAutoscaler: kill_workers triggered")
        nodes = self.workers()
        if nodes:
            self.provider.terminate_nodes(nodes)
            for node in nodes:
                self.node_tracker.untrack(node)
        logger.error("StandardAutoscaler: terminated {} node(s)".format(
            len(nodes)))

    def summary(self):
        """Summarizes the active, pending, and failed node launches.

        An active node is a node whose raylet is actively reporting heartbeats.
        A pending node is non-active node whose node tag is uninitialized,
        waiting for ssh, syncing files, or setting up.
        If a node is not pending or active, it is failed.

        Returns:
            AutoscalerSummary: The summary.
        """
        all_node_ids = self.provider.non_terminated_nodes(tag_filters={})

        active_nodes = Counter()
        pending_nodes = []
        failed_nodes = []
        non_failed = set()

        for node_id in all_node_ids:
            ip = self.provider.internal_ip(node_id)
            node_tags = self.provider.node_tags(node_id)

            if not all(
                    tag in node_tags
                    for tag in (TAG_RAY_NODE_KIND, TAG_RAY_USER_NODE_TYPE,
                                TAG_RAY_NODE_STATUS)):
                # In some node providers, creation of a node and tags is not
                # atomic, so just skip it.
                continue

            if node_tags[TAG_RAY_NODE_KIND] == NODE_KIND_UNMANAGED:
                continue
            node_type = node_tags[TAG_RAY_USER_NODE_TYPE]

            # TODO (Alex): If a node's raylet has died, it shouldn't be marked
            # as active.
            is_active = self.load_metrics.is_active(ip)
            if is_active:
                active_nodes[node_type] += 1
                non_failed.add(node_id)
            else:
                status = node_tags[TAG_RAY_NODE_STATUS]
                pending_states = [
                    STATUS_UNINITIALIZED, STATUS_WAITING_FOR_SSH,
                    STATUS_SYNCING_FILES, STATUS_SETTING_UP
                ]
                is_pending = status in pending_states
                if is_pending:
                    pending_nodes.append((ip, node_type, status))
                    non_failed.add(node_id)

        failed_nodes = self.node_tracker.get_all_failed_node_info(non_failed)

        # The concurrent counter leaves some 0 counts in, so we need to
        # manually filter those out.
        pending_launches = {}
        for node_type, count in self.pending_launches.breakdown().items():
            if count:
                pending_launches[node_type] = count

        return AutoscalerSummary(
            active_nodes=active_nodes,
            pending_nodes=pending_nodes,
            pending_launches=pending_launches,
            failed_nodes=failed_nodes)

    def info_string(self):
        lm_summary = self.load_metrics.summary()
        autoscaler_summary = self.summary()
        return "\n" + format_info_string(lm_summary, autoscaler_summary)
