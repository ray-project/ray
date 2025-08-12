import copy
import logging
import math
import operator
import os
import queue
import subprocess
import threading
import time
from collections import Counter, defaultdict, namedtuple
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple, Union

import yaml

import ray
from ray._common.utils import PLACEMENT_GROUP_BUNDLE_RESOURCE_NAME
from ray.autoscaler._private.constants import (
    AUTOSCALER_HEARTBEAT_TIMEOUT_S,
    AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
    AUTOSCALER_MAX_LAUNCH_BATCH,
    AUTOSCALER_MAX_NUM_FAILURES,
    AUTOSCALER_STATUS_LOG,
    AUTOSCALER_UPDATE_INTERVAL_S,
    DISABLE_LAUNCH_CONFIG_CHECK_KEY,
    DISABLE_NODE_UPDATERS_KEY,
    FOREGROUND_NODE_LAUNCH_KEY,
    WORKER_LIVENESS_CHECK_KEY,
)
from ray.autoscaler._private.event_summarizer import EventSummarizer
from ray.autoscaler._private.legacy_info_string import legacy_log_info_string
from ray.autoscaler._private.load_metrics import LoadMetrics
from ray.autoscaler._private.local.node_provider import (
    LocalNodeProvider,
    record_local_head_state_if_needed,
)
from ray.autoscaler._private.node_launcher import BaseNodeLauncher, NodeLauncher
from ray.autoscaler._private.node_provider_availability_tracker import (
    NodeAvailabilitySummary,
    NodeProviderAvailabilityTracker,
)
from ray.autoscaler._private.node_tracker import NodeTracker
from ray.autoscaler._private.prom_metrics import AutoscalerPrometheusMetrics
from ray.autoscaler._private.providers import _get_node_provider
from ray.autoscaler._private.resource_demand_scheduler import (
    ResourceDemandScheduler,
    ResourceDict,
    get_bin_pack_residual,
    placement_groups_to_resource_demands,
)
from ray.autoscaler._private.updater import NodeUpdaterThread
from ray.autoscaler._private.util import (
    ConcurrentCounter,
    NodeCount,
    NodeID,
    NodeIP,
    NodeType,
    NodeTypeConfigDict,
    format_info_string,
    hash_launch_conf,
    hash_runtime_conf,
    validate_config,
    with_head_node_ip,
)
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_UNMANAGED,
    NODE_KIND_WORKER,
    STATUS_UP_TO_DATE,
    STATUS_UPDATE_FAILED,
    TAG_RAY_FILE_MOUNTS_CONTENTS,
    TAG_RAY_LAUNCH_CONFIG,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_RUNTIME_CONFIG,
    TAG_RAY_USER_NODE_TYPE,
)
from ray.exceptions import RpcError

logger = logging.getLogger(__name__)

# Status of a node e.g. "up-to-date", see ray/autoscaler/tags.py
NodeStatus = str

# Tuple of modified fields for the given node_id returned by should_update
# that will be passed into a NodeUpdaterThread.
UpdateInstructions = namedtuple(
    "UpdateInstructions",
    ["node_id", "setup_commands", "ray_start_commands", "docker_config"],
)

NodeLaunchData = Tuple[NodeTypeConfigDict, NodeCount, Optional[NodeType]]


@dataclass
class AutoscalerSummary:
    active_nodes: Dict[NodeType, int]
    idle_nodes: Optional[Dict[NodeType, int]]
    pending_nodes: List[Tuple[NodeIP, NodeType, NodeStatus]]
    pending_launches: Dict[NodeType, int]
    failed_nodes: List[Tuple[NodeIP, NodeType]]
    node_availability_summary: NodeAvailabilitySummary = field(
        default_factory=lambda: NodeAvailabilitySummary({})
    )
    # A dictionary of node IP to a list of reasons the node is not idle.
    node_activities: Optional[Dict[str, Tuple[NodeIP, List[str]]]] = None
    pending_resources: Dict[str, int] = field(default_factory=lambda: {})
    # A mapping from node name (the same key as `usage_by_node`) to node type.
    # Optional for deployment modes which have the concept of node types and
    # backwards compatibility.
    node_type_mapping: Optional[Dict[str, str]] = None
    # Whether the autoscaler summary is v1 or v2.
    legacy: bool = False


class NonTerminatedNodes:
    """Class to extract and organize information on non-terminated nodes."""

    def __init__(self, provider: NodeProvider):
        start_time = time.time()
        # All non-terminated nodes
        self.all_node_ids = provider.non_terminated_nodes({})

        # Managed worker nodes (node kind "worker"):
        self.worker_ids: List[NodeID] = []
        # The head node (node kind "head")
        self.head_id: Optional[NodeID] = None

        for node in self.all_node_ids:
            node_kind = provider.node_tags(node)[TAG_RAY_NODE_KIND]
            if node_kind == NODE_KIND_WORKER:
                self.worker_ids.append(node)
            elif node_kind == NODE_KIND_HEAD:
                self.head_id = node

        # Note: For typical use-cases, self.all_node_ids == self.worker_ids +
        # [self.head_id]. The difference being in the case of unmanaged nodes.

        # Record the time of the non_terminated nodes call. This typically
        # translates to a "describe" or "list" call on most cluster managers
        # which can be quite expensive. Note that we include the processing
        # time because on some clients, there may be pagination and the
        # underlying api calls may be done lazily.
        self.non_terminated_nodes_time = time.time() - start_time
        logger.info(
            f"The autoscaler took {round(self.non_terminated_nodes_time, 3)}"
            " seconds to fetch the list of non-terminated nodes."
        )

    def remove_terminating_nodes(self, terminating_nodes: List[NodeID]) -> None:
        """Remove nodes we're in the process of terminating from internal
        state."""

        def not_terminating(node):
            return node not in terminating_nodes

        self.worker_ids = list(filter(not_terminating, self.worker_ids))
        self.all_node_ids = list(filter(not_terminating, self.all_node_ids))


# Whether a worker should be kept based on the min_workers and
# max_workers constraints.
#
# keep: should keep the worker
# terminate: should terminate the worker
# decide_later: the worker can be terminated if needed
KeepOrTerminate = Enum("KeepOrTerminate", "keep terminate decide_later")


class StandardAutoscaler:
    """The autoscaling control loop for a Ray cluster.

    There are two ways to start an autoscaling cluster: manually by running
    `ray start --head --autoscaling-config=/path/to/config.yaml` on a instance
    that has permission to launch other instances, or you can also use `ray up
    /path/to/config.yaml` from your laptop, which will configure the right
    AWS/Cloud roles automatically. See the Ray documentation
    (https://docs.ray.io/en/latest/) for a full definition of autoscaling behavior.
    StandardAutoscaler's `update` method is periodically called in
    `monitor.py`'s monitoring loop.

    StandardAutoscaler is also used to bootstrap clusters (by adding workers
    until the cluster size that can handle the resource demand is met).
    """

    def __init__(
        self,
        # TODO(ekl): require config reader to be a callable always.
        config_reader: Union[str, Callable[[], dict]],
        load_metrics: LoadMetrics,
        gcs_client: "ray._raylet.GcsClient",
        session_name: Optional[str] = None,
        max_launch_batch: int = AUTOSCALER_MAX_LAUNCH_BATCH,
        max_concurrent_launches: int = AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
        max_failures: int = AUTOSCALER_MAX_NUM_FAILURES,
        process_runner: Any = subprocess,
        update_interval_s: int = AUTOSCALER_UPDATE_INTERVAL_S,
        prefix_cluster_info: bool = False,
        event_summarizer: Optional[EventSummarizer] = None,
        prom_metrics: Optional[AutoscalerPrometheusMetrics] = None,
    ):
        """Create a StandardAutoscaler.

        Args:
            config_reader: Path to a Ray Autoscaler YAML, or a function to read
                and return the latest config.
            load_metrics: Provides metrics for the Ray cluster.
            session_name: The current Ray session name when this autoscaler
                is deployed.
            max_launch_batch: Max number of nodes to launch in one request.
            max_concurrent_launches: Max number of nodes that can be
                concurrently launched. This value and `max_launch_batch`
                determine the number of batches that are used to launch nodes.
            max_failures: Number of failures that the autoscaler will tolerate
                before exiting.
            process_runner: Subproc-like interface used by the CommandRunner.
            update_interval_s: Seconds between running the autoscaling loop.
            prefix_cluster_info: Whether to add the cluster name to info strs.
            event_summarizer: Utility to consolidate duplicated messages.
            prom_metrics: Prometheus metrics for autoscaler-related operations.
            gcs_client: client for interactions with the GCS. Used to drain nodes
                before termination.
        """

        if isinstance(config_reader, str):
            # Auto wrap with file reader.
            def read_fn():
                with open(config_reader) as f:
                    new_config = yaml.safe_load(f.read())
                return new_config

            self.config_reader = read_fn
        else:
            self.config_reader = config_reader

        self.node_provider_availability_tracker = NodeProviderAvailabilityTracker()
        # Prefix each line of info string with cluster name if True
        self.prefix_cluster_info = prefix_cluster_info
        # Keep this before self.reset (self.provider needs to be created
        # exactly once).
        self.provider = None
        # Keep this before self.reset (if an exception occurs in reset
        # then prom_metrics must be instantitiated to increment the
        # exception counter)
        self.prom_metrics = prom_metrics or AutoscalerPrometheusMetrics(
            session_name=session_name
        )  # noqa
        self.resource_demand_scheduler = None
        self.reset(errors_fatal=True)
        self.load_metrics = load_metrics

        self.max_failures = max_failures
        self.max_launch_batch = max_launch_batch
        self.max_concurrent_launches = max_concurrent_launches
        self.process_runner = process_runner
        self.event_summarizer = event_summarizer or EventSummarizer()

        # Map from node_id to NodeUpdater threads
        self.updaters: Dict[NodeID, NodeUpdaterThread] = {}
        self.num_failed_updates: Dict[NodeID, int] = defaultdict(int)
        self.num_successful_updates: Dict[NodeID, int] = defaultdict(int)
        self.num_failures = 0
        self.last_update_time = 0.0
        self.update_interval_s = update_interval_s

        # Keeps track of pending and running nodes
        self.non_terminated_nodes: Optional[NonTerminatedNodes] = None

        # Tracks nodes scheduled for termination
        self.nodes_to_terminate: List[NodeID] = []

        # Disable NodeUpdater threads if true.
        # Should be set to true in situations where another component, such as
        # a Kubernetes operator, is responsible for Ray setup on nodes.
        self.disable_node_updaters = self.config["provider"].get(
            DISABLE_NODE_UPDATERS_KEY, False
        )
        logger.info(f"{DISABLE_NODE_UPDATERS_KEY}:{self.disable_node_updaters}")

        # Disable launch configuration checking if set to true.
        # This setting is used in scenarios where there is no meaningful node type
        # to enforce, such as in fake multinode situations. When this option is enabled,
        # outdated nodes will not be terminated.
        self.disable_launch_config_check = self.config["provider"].get(
            DISABLE_LAUNCH_CONFIG_CHECK_KEY, False
        )
        logger.info(
            f"{DISABLE_LAUNCH_CONFIG_CHECK_KEY}:{self.disable_launch_config_check}"
        )

        # By default, the autoscaler launches nodes in batches asynchronously in
        # background threads.
        # When the following flag is set, that behavior is disabled, so that nodes
        # are launched in the main thread, all in one batch, blocking until all
        # NodeProvider.create_node calls have returned.
        self.foreground_node_launch = self.config["provider"].get(
            FOREGROUND_NODE_LAUNCH_KEY, False
        )
        logger.info(f"{FOREGROUND_NODE_LAUNCH_KEY}:{self.foreground_node_launch}")

        # By default, the autoscaler kills and/or tries to recover
        # a worker node if it hasn't produced a resource heartbeat in the last 30
        # seconds. The worker_liveness_check flag allows disabling this behavior in
        # settings where another component, such as a Kubernetes operator, is
        # responsible for healthchecks.
        self.worker_liveness_check = self.config["provider"].get(
            WORKER_LIVENESS_CHECK_KEY, True
        )
        logger.info(f"{WORKER_LIVENESS_CHECK_KEY}:{self.worker_liveness_check}")

        # Node launchers
        self.foreground_node_launcher: Optional[BaseNodeLauncher] = None
        self.launch_queue: Optional[queue.Queue[NodeLaunchData]] = None
        self.pending_launches = ConcurrentCounter()
        if self.foreground_node_launch:
            self.foreground_node_launcher = BaseNodeLauncher(
                provider=self.provider,
                pending=self.pending_launches,
                event_summarizer=self.event_summarizer,
                node_provider_availability_tracker=self.node_provider_availability_tracker,  # noqa: E501 Flake and black disagree how to format this.
                session_name=session_name,
                node_types=self.available_node_types,
                prom_metrics=self.prom_metrics,
            )
        else:
            self.launch_queue = queue.Queue()
            max_batches = math.ceil(max_concurrent_launches / float(max_launch_batch))
            for i in range(int(max_batches)):
                node_launcher = NodeLauncher(
                    provider=self.provider,
                    queue=self.launch_queue,
                    index=i,
                    pending=self.pending_launches,
                    event_summarizer=self.event_summarizer,
                    node_provider_availability_tracker=self.node_provider_availability_tracker,  # noqa: E501 Flake and black disagreee how to format this.
                    session_name=session_name,
                    node_types=self.available_node_types,
                    prom_metrics=self.prom_metrics,
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

        self.gcs_client = gcs_client

        for local_path in self.config["file_mounts"].values():
            assert os.path.exists(local_path)
        logger.info("StandardAutoscaler: {}".format(self.config))

    @property
    def all_node_types(self) -> Set[str]:
        return self.config["available_node_types"].keys()

    def update(self):
        try:
            self.reset(errors_fatal=False)
            self._update()
        except Exception as e:
            self.prom_metrics.update_loop_exceptions.inc()
            logger.exception("StandardAutoscaler: Error during autoscaling.")
            self.num_failures += 1
            if self.num_failures > self.max_failures:
                logger.critical("StandardAutoscaler: Too many errors, abort.")
                raise e

    def _update(self):
        # For type checking, assert that these objects have been instantitiated.
        assert self.provider
        assert self.resource_demand_scheduler

        now = time.time()
        # Throttle autoscaling updates to this interval to avoid exceeding
        # rate limits on API calls.
        if now - self.last_update_time < self.update_interval_s:
            return

        self.last_update_time = now

        # Query the provider to update the list of non-terminated nodes
        self.non_terminated_nodes = NonTerminatedNodes(self.provider)

        # Back off the update if the provider says it's not safe to proceed.
        if not self.provider.safe_to_scale():
            logger.info(
                "Backing off of autoscaler update."
                f" Will try again in {self.update_interval_s} seconds."
            )
            return

        # This will accumulate the nodes we need to terminate.
        self.nodes_to_terminate = []

        # Update status strings
        if AUTOSCALER_STATUS_LOG:
            logger.info(self.info_string())
        legacy_log_info_string(self, self.non_terminated_nodes.worker_ids)

        if not self.provider.is_readonly():
            self.terminate_nodes_to_enforce_config_constraints(now)

            if self.disable_node_updaters:
                # Don't handle unhealthy nodes if the liveness check is disabled.
                # self.worker_liveness_check is True by default.
                if self.worker_liveness_check:
                    self.terminate_unhealthy_nodes(now)
            else:
                self.process_completed_updates()
                self.update_nodes()
                # Don't handle unhealthy nodes if the liveness check is disabled.
                # self.worker_liveness_check is True by default.
                if self.worker_liveness_check:
                    self.attempt_to_recover_unhealthy_nodes(now)
                self.set_prometheus_updater_data()

        # Update running nodes gauge
        num_workers = len(self.non_terminated_nodes.worker_ids)
        self.prom_metrics.running_workers.set(num_workers)

        # Remove IPs from LoadMetrics that are not known to the NodeProvider.
        self.load_metrics.prune_active_ips(
            active_ips=[
                self.provider.internal_ip(node_id)
                for node_id in self.non_terminated_nodes.all_node_ids
            ]
        )

        # Dict[NodeType, int], List[ResourceDict]
        to_launch, unfulfilled = self.resource_demand_scheduler.get_nodes_to_launch(
            self.non_terminated_nodes.all_node_ids,
            self.pending_launches.breakdown(),
            self.load_metrics.get_resource_demand_vector(),
            self.load_metrics.get_resource_utilization(),
            self.load_metrics.get_pending_placement_groups(),
            self.load_metrics.get_static_node_resources_by_ip(),
            ensure_min_cluster_size=self.load_metrics.get_resource_requests(),
            node_availability_summary=self.node_provider_availability_tracker.summary(),
        )
        self._report_pending_infeasible(unfulfilled)

        if not self.provider.is_readonly():
            self.launch_required_nodes(to_launch)

        # Execute optional end-of-update logic.
        # Keep this method call at the end of autoscaler._update().
        self.provider.post_process()

        # Record the amount of time the autoscaler took for
        # this _update() iteration.
        update_time = time.time() - self.last_update_time
        logger.info(
            f"The autoscaler took {round(update_time, 3)}"
            " seconds to complete the update iteration."
        )
        self.prom_metrics.update_time.observe(update_time)

    def terminate_nodes_to_enforce_config_constraints(self, now: float):
        """Terminates nodes to enforce constraints defined by the autoscaling
        config.

        (1) Terminates nodes in excess of `max_workers`.
        (2) Terminates nodes idle for longer than `idle_timeout_minutes`.
        (3) Terminates outdated nodes,
                namely nodes whose configs don't match `node_config` for the
                relevant node type.

        Avoids terminating non-outdated nodes required by
        autoscaler.sdk.request_resources().
        """
        # For type checking, assert that these objects have been instantitiated.
        assert self.non_terminated_nodes
        assert self.provider

        last_used = self.load_metrics.ray_nodes_last_used_time_by_ip

        idle_timeout_s = 60 * self.config["idle_timeout_minutes"]

        last_used_cutoff = now - idle_timeout_s

        # Sort based on last used to make sure to keep min_workers that
        # were most recently used. Otherwise, _keep_min_workers_of_node_type
        # might keep a node that should be terminated.
        sorted_node_ids = self._sort_based_on_last_used(
            self.non_terminated_nodes.worker_ids, last_used
        )

        # Don't terminate nodes needed by request_resources()
        nodes_not_allowed_to_terminate = self._get_nodes_needed_for_request_resources(
            sorted_node_ids
        )

        # Tracks counts of nodes we intend to keep for each node type.
        node_type_counts = defaultdict(int)

        def keep_node(node_id: NodeID) -> None:
            assert self.provider
            # Update per-type counts.
            tags = self.provider.node_tags(node_id)
            if TAG_RAY_USER_NODE_TYPE in tags:
                node_type = tags[TAG_RAY_USER_NODE_TYPE]
                node_type_counts[node_type] += 1

        # Nodes that we could terminate, if needed.
        nodes_we_could_terminate: List[NodeID] = []

        for node_id in sorted_node_ids:
            # Make sure to not kill idle node types if the number of workers
            # of that type is lower/equal to the min_workers of that type
            # or it is needed for request_resources().
            should_keep_or_terminate, reason = self._keep_worker_of_node_type(
                node_id, node_type_counts
            )
            if should_keep_or_terminate == KeepOrTerminate.terminate:
                self.schedule_node_termination(node_id, reason, logger.info)
                continue
            if (
                should_keep_or_terminate == KeepOrTerminate.keep
                or node_id in nodes_not_allowed_to_terminate
            ) and self.launch_config_ok(node_id):
                keep_node(node_id)
                continue

            node_ip = self.provider.internal_ip(node_id)

            if node_ip in last_used and last_used[node_ip] < last_used_cutoff:
                self.schedule_node_termination(node_id, "idle", logger.info)
                # Get the local time of the node's last use as a string.
                formatted_last_used_time = time.asctime(
                    time.localtime(last_used[node_ip])
                )
                logger.info(f"Node last used: {formatted_last_used_time}.")
                # Note that the current time will appear in the log prefix.
            elif not self.launch_config_ok(node_id):
                self.schedule_node_termination(node_id, "outdated", logger.info)
            else:
                keep_node(node_id)
                nodes_we_could_terminate.append(node_id)

        # Terminate nodes if there are too many
        num_workers = len(self.non_terminated_nodes.worker_ids)
        num_extra_nodes_to_terminate = (
            num_workers - len(self.nodes_to_terminate) - self.config["max_workers"]
        )

        if num_extra_nodes_to_terminate > len(nodes_we_could_terminate):
            logger.warning(
                "StandardAutoscaler: trying to terminate "
                f"{num_extra_nodes_to_terminate} nodes, while only "
                f"{len(nodes_we_could_terminate)} are safe to terminate."
                " Inconsistent config is likely."
            )
            num_extra_nodes_to_terminate = len(nodes_we_could_terminate)

        # If num_extra_nodes_to_terminate is negative or zero,
        # we would have less than max_workers nodes after terminating
        # nodes_to_terminate and we do not need to terminate anything else.
        if num_extra_nodes_to_terminate > 0:
            extra_nodes_to_terminate = nodes_we_could_terminate[
                -num_extra_nodes_to_terminate:
            ]
            for node_id in extra_nodes_to_terminate:
                self.schedule_node_termination(node_id, "max workers", logger.info)

        self.terminate_scheduled_nodes()

    def schedule_node_termination(
        self, node_id: NodeID, reason_opt: Optional[str], logger_method: Callable
    ) -> None:
        # For type checking, assert that this object has been instantitiated.
        assert self.provider

        if reason_opt is None:
            raise Exception("reason should be not None.")
        reason: str = reason_opt
        node_ip = self.provider.internal_ip(node_id)
        # Log, record an event, and add node_id to nodes_to_terminate.
        logger_method(
            "StandardAutoscaler: "
            f"Terminating the node with id {node_id}"
            f" and ip {node_ip}."
            f" ({reason})"
        )
        self.event_summarizer.add(
            "Removing {} nodes of type "
            + self._get_node_type(node_id)
            + " ({}).".format(reason),
            quantity=1,
            aggregate=operator.add,
        )
        self.nodes_to_terminate.append(node_id)

    def terminate_scheduled_nodes(self):
        """Terminate scheduled nodes and clean associated autoscaler state."""
        # For type checking, assert that these objects have been instantitiated.
        assert self.provider
        assert self.non_terminated_nodes

        if not self.nodes_to_terminate:
            return

        # Drain the nodes
        self.drain_nodes_via_gcs(self.nodes_to_terminate)
        # Terminate the nodes
        self.provider.terminate_nodes(self.nodes_to_terminate)
        for node in self.nodes_to_terminate:
            self.node_tracker.untrack(node)
            self.prom_metrics.stopped_nodes.inc()

        # Update internal node lists
        self.non_terminated_nodes.remove_terminating_nodes(self.nodes_to_terminate)

        self.nodes_to_terminate = []

    def drain_nodes_via_gcs(self, provider_node_ids_to_drain: List[NodeID]):
        """Send an RPC request to the GCS to drain (prepare for termination)
        the nodes with the given node provider ids.

        note: The current implementation of DrainNode on the GCS side is to
        de-register and gracefully shut down the Raylets. In the future,
        the behavior may change to better reflect the name "Drain."
        See https://github.com/ray-project/ray/pull/19350.
        """
        # For type checking, assert that this object has been instantitiated.
        assert self.provider

        # The GCS expects Node ids in the request, rather than NodeProvider
        # ids. To get the Node ids of the nodes to we're draining, we make
        # the following translations of identifiers:
        # node provider node id -> ip -> node id

        # Convert node provider node ids to ips.
        node_ips = set()
        failed_ip_fetch = False
        for provider_node_id in provider_node_ids_to_drain:
            # If the provider's call to fetch ip fails, the exception is not
            # fatal. Log the exception and proceed.
            try:
                ip = self.provider.internal_ip(provider_node_id)
                node_ips.add(ip)
            except Exception:
                logger.exception(
                    "Failed to get ip of node with id"
                    f" {provider_node_id} during scale-down."
                )
                failed_ip_fetch = True
        if failed_ip_fetch:
            self.prom_metrics.drain_node_exceptions.inc()

        # Only attempt to drain connected nodes, i.e. nodes with ips in
        # LoadMetrics.
        connected_node_ips = node_ips & self.load_metrics.node_id_by_ip.keys()

        # Convert ips to Node ids.
        # (The assignment ip->node_id is well-defined under current
        # assumptions. See "use_node_id_as_ip" in monitor.py)
        node_ids_to_drain = {
            self.load_metrics.node_id_by_ip[ip] for ip in connected_node_ips
        }

        if not node_ids_to_drain:
            return

        logger.info(f"Draining {len(node_ids_to_drain)} raylet(s).")
        try:
            # A successful response indicates that the GCS has marked the
            # desired nodes as "drained." The cloud provider can then terminate
            # the nodes without the GCS printing an error.
            # Check if we succeeded in draining all of the intended nodes by
            # looking at the RPC response.
            drained_node_ids = set(
                self.gcs_client.drain_nodes(node_ids_to_drain, timeout=5)
            )
            failed_to_drain = node_ids_to_drain - drained_node_ids
            if failed_to_drain:
                self.prom_metrics.drain_node_exceptions.inc()
                logger.error(f"Failed to drain {len(failed_to_drain)} raylet(s).")
        # If we get a gRPC error with an UNIMPLEMENTED code, fail silently.
        # This error indicates that the GCS is using Ray version < 1.8.0,
        # for which DrainNode is not implemented.
        except RpcError as e:
            # If the code is UNIMPLEMENTED, pass.
            if e.rpc_code == ray._raylet.GRPC_STATUS_CODE_UNIMPLEMENTED:
                pass
            # Otherwise, it's a plain old gRPC error and we should log it.
            else:
                self.prom_metrics.drain_node_exceptions.inc()
                logger.exception("Failed to drain Ray nodes. Traceback follows.")
        except Exception:
            # We don't need to interrupt the autoscaler update with an
            # exception, but we should log what went wrong and record the
            # failure in Prometheus.
            self.prom_metrics.drain_node_exceptions.inc()
            logger.exception("Failed to drain Ray nodes. Traceback follows.")

    def launch_required_nodes(self, to_launch: Dict[NodeType, int]) -> None:
        if to_launch:
            for node_type, count in to_launch.items():
                self.launch_new_node(count, node_type=node_type)

    def update_nodes(self):
        """Run NodeUpdaterThreads to run setup commands, sync files,
        and/or start Ray.
        """
        # Update nodes with out-of-date files.
        # TODO(edoakes): Spawning these threads directly seems to cause
        # problems. They should at a minimum be spawned as daemon threads.
        # See https://github.com/ray-project/ray/pull/5903 for more info.
        T = []
        for node_id, setup_commands, ray_start_commands, docker_config in (
            self.should_update(node_id)
            for node_id in self.non_terminated_nodes.worker_ids
        ):
            if node_id is not None:
                resources = self._node_resources(node_id)
                labels = self._node_labels(node_id)
                logger.debug(f"{node_id}: Starting new thread runner.")
                T.append(
                    threading.Thread(
                        target=self.spawn_updater,
                        args=(
                            node_id,
                            setup_commands,
                            ray_start_commands,
                            resources,
                            labels,
                            docker_config,
                        ),
                    )
                )
        for t in T:
            t.start()
        for t in T:
            t.join()

    def process_completed_updates(self):
        """Clean up completed NodeUpdaterThreads."""
        completed_nodes = []
        for node_id, updater in self.updaters.items():
            if not updater.is_alive():
                completed_nodes.append(node_id)
        if completed_nodes:
            failed_nodes = []
            for node_id in completed_nodes:
                updater = self.updaters[node_id]
                if updater.exitcode == 0:
                    self.num_successful_updates[node_id] += 1
                    self.prom_metrics.successful_updates.inc()
                    if updater.for_recovery:
                        self.prom_metrics.successful_recoveries.inc()
                    if updater.update_time:
                        self.prom_metrics.worker_update_time.observe(
                            updater.update_time
                        )
                    # Mark the node as active to prevent the node recovery
                    # logic immediately trying to restart Ray on the new node.
                    self.load_metrics.mark_active(self.provider.internal_ip(node_id))
                else:
                    failed_nodes.append(node_id)
                    self.num_failed_updates[node_id] += 1
                    self.prom_metrics.failed_updates.inc()
                    if updater.for_recovery:
                        self.prom_metrics.failed_recoveries.inc()
                    self.node_tracker.untrack(node_id)
                del self.updaters[node_id]

            if failed_nodes:
                # Some nodes in failed_nodes may already have been terminated
                # during an update (for being idle after missing a heartbeat).

                # Update the list of non-terminated workers.
                for node_id in failed_nodes:
                    # Check if the node has already been terminated.
                    if node_id in self.non_terminated_nodes.worker_ids:
                        self.schedule_node_termination(
                            node_id, "launch failed", logger.error
                        )
                    else:
                        logger.warning(
                            f"StandardAutoscaler: {node_id}:"
                            " Failed to update node."
                            " Node has already been terminated."
                        )
                self.terminate_scheduled_nodes()

    def set_prometheus_updater_data(self):
        """Record total number of active NodeUpdaterThreads and how many of
        these are being run to recover nodes.
        """
        self.prom_metrics.updating_nodes.set(len(self.updaters))
        num_recovering = 0
        for updater in self.updaters.values():
            if updater.for_recovery:
                num_recovering += 1
        self.prom_metrics.recovering_nodes.set(num_recovering)

    def _report_pending_infeasible(self, unfulfilled: List[ResourceDict]):
        """Emit event messages for infeasible or unschedulable tasks.

        This adds messages to the event summarizer for warning on infeasible
        or "cluster full" resource requests.

        Args:
            unfulfilled: List of resource demands that would be unfulfilled
                even after full scale-up.
        """
        # For type checking, assert that this object has been instantitiated.
        assert self.resource_demand_scheduler
        pending = []
        infeasible = []
        for bundle in unfulfilled:
            placement_group = any(
                "_group_" in k or k == PLACEMENT_GROUP_BUNDLE_RESOURCE_NAME
                for k in bundle
            )
            if placement_group:
                continue
            if self.resource_demand_scheduler.is_feasible(bundle):
                pending.append(bundle)
            else:
                infeasible.append(bundle)
        if pending:
            if self.load_metrics.cluster_full_of_actors_detected:
                for request in pending:
                    self.event_summarizer.add_once_per_interval(
                        "Warning: The following resource request cannot be "
                        "scheduled right now: {}. This is likely due to all "
                        "cluster resources being claimed by actors. Consider "
                        "creating fewer actors or adding more nodes "
                        "to this Ray cluster.".format(request),
                        key="pending_{}".format(sorted(request.items())),
                        interval_s=30,
                    )
        if infeasible:
            for request in infeasible:
                self.event_summarizer.add_once_per_interval(
                    "Error: No available node types can fulfill resource "
                    "request {}. Add suitable node types to this cluster to "
                    "resolve this issue.".format(request),
                    key="infeasible_{}".format(sorted(request.items())),
                    interval_s=30,
                )

    def _sort_based_on_last_used(
        self, nodes: List[NodeID], last_used: Dict[str, float]
    ) -> List[NodeID]:
        """Sort the nodes based on the last time they were used.

        The first item in the return list is the most recently used.
        """
        last_used_copy = copy.deepcopy(last_used)
        # Add the unconnected nodes as the least recently used (the end of
        # list). This prioritizes connected nodes.
        least_recently_used = -1

        def last_time_used(node_id: NodeID):
            assert self.provider
            node_ip = self.provider.internal_ip(node_id)
            if node_ip not in last_used_copy:
                return least_recently_used
            else:
                return last_used_copy[node_ip]

        return sorted(nodes, key=last_time_used, reverse=True)

    def _get_nodes_needed_for_request_resources(
        self, sorted_node_ids: List[NodeID]
    ) -> FrozenSet[NodeID]:
        # TODO(ameer): try merging this with resource_demand_scheduler
        # code responsible for adding nodes for get_resource_requests() and get_pending_placement_groups().
        """Returns the nodes NOT allowed to terminate due to get_resource_requests() and get_pending_placement_groups().

        Args:
            sorted_node_ids: the node ids sorted based on last used (LRU last).

        Returns:
            FrozenSet[NodeID]: a set of nodes (node ids) that
            we should NOT terminate.
        """
        # For type checking, assert that this object has been instantitiated.
        assert self.provider

        nodes_not_allowed_to_terminate: Set[NodeID] = set()

        resource_demands, strict_spreads = placement_groups_to_resource_demands(
            self.load_metrics.get_pending_placement_groups()
        )
        resource_demands.extend(self.load_metrics.get_resource_requests())
        if not resource_demands and not strict_spreads:
            return frozenset(nodes_not_allowed_to_terminate)

        static_node_resources: Dict[
            NodeIP, ResourceDict
        ] = self.load_metrics.get_static_node_resources_by_ip()

        head_node_resources: ResourceDict = copy.deepcopy(
            self.available_node_types[self.config["head_node_type"]]["resources"]
        )
        # TODO(ameer): this is somewhat duplicated in
        # resource_demand_scheduler.py.
        if not head_node_resources:
            # Legacy yaml might include {} in the resources field.
            head_node_ip = self.provider.internal_ip(self.non_terminated_nodes.head_id)
            head_node_resources = static_node_resources.get(head_node_ip, {})

        node_total_resources: List[ResourceDict] = [head_node_resources]
        resource_demand_vector_worker_node_ids = []
        # Get max resources on all the non terminated nodes.
        for node_id in sorted_node_ids:
            tags = self.provider.node_tags(node_id)
            if TAG_RAY_USER_NODE_TYPE in tags:
                node_type = tags[TAG_RAY_USER_NODE_TYPE]
                node_resources: ResourceDict = copy.deepcopy(
                    self.available_node_types[node_type]["resources"]
                )
                if not node_resources:
                    # Legacy yaml might include {} in the resources field.
                    node_ip = self.provider.internal_ip(node_id)
                    node_resources = static_node_resources.get(node_ip, {})
                node_total_resources.append(node_resources)
                resource_demand_vector_worker_node_ids.append(node_id)
        # Since it is sorted based on last used, we "keep" nodes that are
        # most recently used when we binpack. We assume get_bin_pack_residual
        # is following the given order here.
        node_remaining_resources = copy.deepcopy(node_total_resources)

        for strict_spread in strict_spreads:
            unfulfilled, updated_node_remaining_resources = get_bin_pack_residual(
                node_remaining_resources, strict_spread, strict_spread=True
            )
            if unfulfilled:
                continue
            node_remaining_resources = updated_node_remaining_resources

        _, node_remaining_resources = get_bin_pack_residual(
            node_remaining_resources, resource_demands
        )
        # Remove the first entry (the head node).
        node_total_resources.pop(0)
        # Remove the first entry (the head node).
        node_remaining_resources.pop(0)
        for i, node_id in enumerate(resource_demand_vector_worker_node_ids):
            if (
                node_remaining_resources[i] == node_total_resources[i]
                and node_total_resources[i]
            ):
                # No resources of the node were needed for request_resources().
                # node_total_resources[i] is an empty dict for legacy yamls
                # before the node is connected.
                pass
            else:
                nodes_not_allowed_to_terminate.add(node_id)
        return frozenset(nodes_not_allowed_to_terminate)

    def _keep_worker_of_node_type(
        self, node_id: NodeID, node_type_counts: Dict[NodeType, int]
    ) -> Tuple[KeepOrTerminate, Optional[str]]:
        """Determines if a worker should be kept based on the min_workers
        and max_workers constraint of the worker's node_type.

        Returns KeepOrTerminate.keep when both of the following hold:
        (a) The worker's node_type is present among the keys of the current
            config's available_node_types dict.
        (b) Deleting the node would violate the min_workers constraint for that
            worker's node_type.

        Returns KeepOrTerminate.terminate when both the following hold:
        (a) The worker's node_type is not present among the keys of the current
            config's available_node_types dict.
        (b) Keeping the node would violate the max_workers constraint for that
            worker's node_type.

        Return KeepOrTerminate.decide_later otherwise.

        Args:
            node_type_counts(Dict[NodeType, int]): The non_terminated node
                types counted so far.
        Returns:
            KeepOrTerminate: keep if the node should be kept, terminate if the
            node should be terminated, decide_later if we are allowed
            to terminate it, but do not have to.
            Optional[str]: reason for termination. Not None on
            KeepOrTerminate.terminate, None otherwise.
        """
        # For type checking, assert that this object has been instantitiated.
        assert self.provider

        tags = self.provider.node_tags(node_id)
        if TAG_RAY_USER_NODE_TYPE in tags:
            node_type = tags[TAG_RAY_USER_NODE_TYPE]

            min_workers = self.available_node_types.get(node_type, {}).get(
                "min_workers", 0
            )
            max_workers = self.available_node_types.get(node_type, {}).get(
                "max_workers", 0
            )
            if node_type not in self.available_node_types:
                # The node type has been deleted from the cluster config.
                # Allow terminating it if needed.
                available_node_types = list(self.available_node_types.keys())
                return (
                    KeepOrTerminate.terminate,
                    f"not in available_node_types: {available_node_types}",
                )
            new_count = node_type_counts[node_type] + 1
            if new_count <= min(min_workers, max_workers):
                return KeepOrTerminate.keep, None
            if new_count > max_workers:
                return KeepOrTerminate.terminate, "max_workers_per_type"

        return KeepOrTerminate.decide_later, None

    def _node_resources(self, node_id):
        node_type = self.provider.node_tags(node_id).get(TAG_RAY_USER_NODE_TYPE)
        if self.available_node_types:
            return self.available_node_types.get(node_type, {}).get("resources", {})
        else:
            return {}

    def _node_labels(self, node_id):
        node_type = self.provider.node_tags(node_id).get(TAG_RAY_USER_NODE_TYPE)
        if self.available_node_types:
            return self.available_node_types.get(node_type, {}).get("labels", {})
        else:
            return {}

    def reset(self, errors_fatal=False):
        sync_continuously = False
        if hasattr(self, "config"):
            sync_continuously = self.config.get("file_mounts_sync_continuously", False)
        try:
            new_config = self.config_reader()
            if new_config != getattr(self, "config", None):
                try:
                    validate_config(new_config)
                except Exception as e:
                    self.prom_metrics.config_validation_exceptions.inc()
                    logger.debug(
                        "Cluster config validation failed. The version of "
                        "the ray CLI you launched this cluster with may "
                        "be higher than the version of ray being run on "
                        "the cluster. Some new features may not be "
                        "available until you upgrade ray on your cluster.",
                        exc_info=e,
                    )
            logger.debug(
                f"New config after validation: {new_config},"
                f" of type: {type(new_config)}"
            )
            (new_runtime_hash, new_file_mounts_contents_hash) = hash_runtime_conf(
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
                self.provider = _get_node_provider(
                    self.config["provider"], self.config["cluster_name"]
                )

            # If using the LocalNodeProvider, make sure the head node is marked
            # non-terminated.
            if isinstance(self.provider, LocalNodeProvider):
                record_local_head_state_if_needed(self.provider)

            self.available_node_types = self.config["available_node_types"]
            upscaling_speed = self.config.get("upscaling_speed")
            aggressive = self.config.get("autoscaling_mode") == "aggressive"
            target_utilization_fraction = self.config.get("target_utilization_fraction")
            if upscaling_speed:
                upscaling_speed = float(upscaling_speed)
            # TODO(ameer): consider adding (if users ask) an option of
            # initial_upscaling_num_workers.
            elif aggressive:
                upscaling_speed = 99999
                logger.warning(
                    "Legacy aggressive autoscaling mode "
                    "detected. Replacing it by setting upscaling_speed to "
                    "99999."
                )
            elif target_utilization_fraction:
                upscaling_speed = 1 / max(target_utilization_fraction, 0.001) - 1
                logger.warning(
                    "Legacy target_utilization_fraction config "
                    "detected. Replacing it by setting upscaling_speed to "
                    + "1 / target_utilization_fraction - 1."
                )
            else:
                upscaling_speed = 1.0
            if self.resource_demand_scheduler:
                # The node types are autofilled internally for legacy yamls,
                # overwriting the class will remove the inferred node resources
                # for legacy yamls.
                self.resource_demand_scheduler.reset_config(
                    self.provider,
                    self.available_node_types,
                    self.config["max_workers"],
                    self.config["head_node_type"],
                    upscaling_speed,
                )
            else:
                self.resource_demand_scheduler = ResourceDemandScheduler(
                    self.provider,
                    self.available_node_types,
                    self.config["max_workers"],
                    self.config["head_node_type"],
                    upscaling_speed,
                )

        except Exception as e:
            self.prom_metrics.reset_exceptions.inc()
            if errors_fatal:
                raise e
            else:
                logger.exception("StandardAutoscaler: Error parsing config.")

    def launch_config_ok(self, node_id):
        if self.disable_launch_config_check:
            return True
        node_tags = self.provider.node_tags(node_id)
        tag_launch_conf = node_tags.get(TAG_RAY_LAUNCH_CONFIG)
        node_type = node_tags.get(TAG_RAY_USER_NODE_TYPE)
        if node_type not in self.available_node_types:
            # The node type has been deleted from the cluster config.
            # Don't keep the node.
            return False

        # The `worker_nodes` field is deprecated in favor of per-node-type
        # node_configs. We allow it for backwards-compatibility.
        launch_config = copy.deepcopy(self.config.get("worker_nodes", {}))
        if node_type:
            launch_config.update(
                self.config["available_node_types"][node_type]["node_config"]
            )
        calculated_launch_hash = hash_launch_conf(launch_config, self.config["auth"])

        if calculated_launch_hash != tag_launch_conf:
            return False
        return True

    def files_up_to_date(self, node_id):
        node_tags = self.provider.node_tags(node_id)
        applied_config_hash = node_tags.get(TAG_RAY_RUNTIME_CONFIG)
        applied_file_mounts_contents_hash = node_tags.get(TAG_RAY_FILE_MOUNTS_CONTENTS)
        if applied_config_hash != self.runtime_hash or (
            self.file_mounts_contents_hash is not None
            and self.file_mounts_contents_hash != applied_file_mounts_contents_hash
        ):
            logger.info(
                "StandardAutoscaler: "
                "{}: Runtime state is ({},{}), want ({},{})".format(
                    node_id,
                    applied_config_hash,
                    applied_file_mounts_contents_hash,
                    self.runtime_hash,
                    self.file_mounts_contents_hash,
                )
            )
            return False
        return True

    def heartbeat_on_time(self, node_id: NodeID, now: float) -> bool:
        """Determine whether we've received a heartbeat from a node within the
        last AUTOSCALER_HEARTBEAT_TIMEOUT_S seconds.
        """
        # For type checking, assert that this object has been instantitiated.
        assert self.provider

        key = self.provider.internal_ip(node_id)

        if key in self.load_metrics.last_heartbeat_time_by_ip:
            last_heartbeat_time = self.load_metrics.last_heartbeat_time_by_ip[key]
            delta = now - last_heartbeat_time
            if delta < AUTOSCALER_HEARTBEAT_TIMEOUT_S:
                return True
        return False

    def terminate_unhealthy_nodes(self, now: float):
        """Terminated nodes for which we haven't received a heartbeat on time.
        These nodes are subsequently terminated.
        """
        # For type checking, assert that these objects have been instantitiated.
        assert self.provider
        assert self.non_terminated_nodes

        for node_id in self.non_terminated_nodes.worker_ids:
            node_status = self.provider.node_tags(node_id)[TAG_RAY_NODE_STATUS]
            # We're not responsible for taking down
            # nodes with pending or failed status:
            if not node_status == STATUS_UP_TO_DATE:
                continue
            # This node is up-to-date. If it hasn't had the chance to produce
            # a heartbeat, fake the heartbeat now (see logic for completed node
            # updaters).
            ip = self.provider.internal_ip(node_id)
            if ip not in self.load_metrics.last_heartbeat_time_by_ip:
                self.load_metrics.mark_active(ip)
            # Heartbeat indicates node is healthy:
            if self.heartbeat_on_time(node_id, now):
                continue
            self.schedule_node_termination(
                node_id, "lost contact with raylet", logger.warning
            )
        self.terminate_scheduled_nodes()

    def attempt_to_recover_unhealthy_nodes(self, now):
        for node_id in self.non_terminated_nodes.worker_ids:
            self.recover_if_needed(node_id, now)

    def recover_if_needed(self, node_id, now):
        if not self.can_update(node_id):
            return
        if self.heartbeat_on_time(node_id, now):
            return

        logger.warning(
            "StandardAutoscaler: "
            "{}: No recent heartbeat, "
            "restarting Ray to recover...".format(node_id)
        )
        self.event_summarizer.add(
            "Restarting {} nodes of type "
            + self._get_node_type(node_id)
            + " (lost contact with raylet).",
            quantity=1,
            aggregate=operator.add,
        )
        head_node_ip = self.provider.internal_ip(self.non_terminated_nodes.head_id)
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
                self.config["worker_start_ray_commands"], head_node_ip
            ),
            runtime_hash=self.runtime_hash,
            file_mounts_contents_hash=self.file_mounts_contents_hash,
            process_runner=self.process_runner,
            use_internal_ip=True,
            is_head_node=False,
            docker_config=self._get_node_specific_docker_config(node_id),
            node_resources=self._node_resources(node_id),
            node_labels=self._node_labels(node_id),
            for_recovery=True,
        )
        updater.start()
        self.updaters[node_id] = updater

    def _get_node_type(self, node_id: str) -> str:
        # For type checking, assert that this object has been instantitiated.
        assert self.provider

        node_tags = self.provider.node_tags(node_id)
        if TAG_RAY_USER_NODE_TYPE in node_tags:
            return node_tags[TAG_RAY_USER_NODE_TYPE]
        else:
            return "unknown_node_type"

    def _get_node_type_specific_fields(self, node_id: str, fields_key: str) -> Any:
        # For type checking, assert that this object has been instantitiated.
        assert self.provider

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
        node_specific_docker = self._get_node_type_specific_fields(node_id, "docker")
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
                node_id, "worker_setup_commands"
            )
            ray_start_commands = []
        else:
            setup_commands = self._get_node_type_specific_fields(
                node_id, "worker_setup_commands"
            )
            ray_start_commands = self.config["worker_start_ray_commands"]

        docker_config = self._get_node_specific_docker_config(node_id)
        return UpdateInstructions(
            node_id=node_id,
            setup_commands=setup_commands,
            ray_start_commands=ray_start_commands,
            docker_config=docker_config,
        )

    def spawn_updater(
        self,
        node_id,
        setup_commands,
        ray_start_commands,
        node_resources,
        node_labels,
        docker_config,
    ):
        logger.info(
            f"Creating new (spawn_updater) updater thread for node" f" {node_id}."
        )
        ip = self.provider.internal_ip(node_id)
        node_type = self._get_node_type(node_id)
        self.node_tracker.track(node_id, ip, node_type)
        head_node_ip = self.provider.internal_ip(self.non_terminated_nodes.head_id)
        updater = NodeUpdaterThread(
            node_id=node_id,
            provider_config=self.config["provider"],
            provider=self.provider,
            auth_config=self.config["auth"],
            cluster_name=self.config["cluster_name"],
            file_mounts=self.config["file_mounts"],
            initialization_commands=with_head_node_ip(
                self._get_node_type_specific_fields(node_id, "initialization_commands"),
                head_node_ip,
            ),
            setup_commands=with_head_node_ip(setup_commands, head_node_ip),
            ray_start_commands=with_head_node_ip(ray_start_commands, head_node_ip),
            runtime_hash=self.runtime_hash,
            file_mounts_contents_hash=self.file_mounts_contents_hash,
            is_head_node=False,
            cluster_synced_files=self.config["cluster_synced_files"],
            rsync_options={
                "rsync_exclude": self.config.get("rsync_exclude"),
                "rsync_filter": self.config.get("rsync_filter"),
            },
            process_runner=self.process_runner,
            use_internal_ip=True,
            docker_config=docker_config,
            node_resources=node_resources,
            node_labels=node_labels,
        )
        updater.start()
        self.updaters[node_id] = updater

    def can_update(self, node_id):
        if self.disable_node_updaters:
            return False
        if node_id in self.updaters:
            return False
        if not self.launch_config_ok(node_id):
            return False
        if self.num_failed_updates.get(node_id, 0) > 0:  # TODO(ekl) retry?
            return False
        logger.debug(
            f"{node_id} is not being updated and "
            "passes config check (can_update=True)."
        )
        return True

    def launch_new_node(self, count: int, node_type: str) -> None:
        logger.info("StandardAutoscaler: Queue {} new nodes for launch".format(count))
        self.pending_launches.inc(node_type, count)
        config = copy.deepcopy(self.config)
        if self.foreground_node_launch:
            assert self.foreground_node_launcher is not None
            # Launch in the main thread and block.
            self.foreground_node_launcher.launch_node(config, count, node_type)
        else:
            assert self.launch_queue is not None
            # Split into individual launch requests of the max batch size.
            while count > 0:
                # Enqueue launch data for the background NodeUpdater threads.
                self.launch_queue.put(
                    (config, min(count, self.max_launch_batch), node_type)
                )
                count -= self.max_launch_batch

    def kill_workers(self):
        logger.error("StandardAutoscaler: kill_workers triggered")
        nodes = self.workers()
        if nodes:
            self.provider.terminate_nodes(nodes)
            for node in nodes:
                self.node_tracker.untrack(node)
                self.prom_metrics.stopped_nodes.inc()
        logger.error("StandardAutoscaler: terminated {} node(s)".format(len(nodes)))

    def summary(self) -> Optional[AutoscalerSummary]:
        """Summarizes the active, pending, and failed node launches.

        An active node is a node whose raylet is actively reporting heartbeats.
        A pending node is non-active node whose node tag is uninitialized,
        waiting for ssh, syncing files, or setting up.
        If a node is not pending or active, it is failed.

        Returns:
            AutoscalerSummary: The summary.
        """
        # For type checking, assert that this object has been instantitiated.
        assert self.provider

        if not self.non_terminated_nodes:
            return None
        active_nodes: Dict[NodeType, int] = Counter()
        pending_nodes = []
        failed_nodes = []
        non_failed = set()

        node_type_mapping = {}
        now = time.time()
        for node_id in self.non_terminated_nodes.all_node_ids:
            ip = self.provider.internal_ip(node_id)
            node_tags = self.provider.node_tags(node_id)

            if not all(
                tag in node_tags
                for tag in (
                    TAG_RAY_NODE_KIND,
                    TAG_RAY_USER_NODE_TYPE,
                    TAG_RAY_NODE_STATUS,
                )
            ):
                # In some node providers, creation of a node and tags is not
                # atomic, so just skip it.
                continue

            if node_tags[TAG_RAY_NODE_KIND] == NODE_KIND_UNMANAGED:
                continue
            node_type = node_tags[TAG_RAY_USER_NODE_TYPE]

            node_type_mapping[ip] = node_type

            is_active = self.heartbeat_on_time(node_id, now)
            if is_active:
                active_nodes[node_type] += 1
                non_failed.add(node_id)
            else:
                status = node_tags[TAG_RAY_NODE_STATUS]
                completed_states = [STATUS_UP_TO_DATE, STATUS_UPDATE_FAILED]
                is_pending = status not in completed_states
                if is_pending:
                    pending_nodes.append((node_id, ip, node_type, status))
                    non_failed.add(node_id)

        failed_nodes = self.node_tracker.get_all_failed_node_info(non_failed)

        # The concurrent counter leaves some 0 counts in, so we need to
        # manually filter those out.
        pending_launches = {}
        for node_type, count in self.pending_launches.breakdown().items():
            if count:
                pending_launches[node_type] = count

        pending_resources = {}
        for node_resources in self.resource_demand_scheduler.calculate_node_resources(
            nodes=[node_id for node_id, _, _, _ in pending_nodes],
            pending_nodes=pending_launches,
            # We don't fill this field out because we're intentionally only
            # passing pending nodes (which aren't tracked by load metrics
            # anyways).
            unused_resources_by_ip={},
        )[0]:
            for key, value in node_resources.items():
                pending_resources[key] = value + pending_resources.get(key, 0)

        return AutoscalerSummary(
            # Convert active_nodes from counter to dict for later serialization
            active_nodes=dict(active_nodes),
            idle_nodes=None,
            pending_nodes=[
                (ip, node_type, status) for _, ip, node_type, status in pending_nodes
            ],
            pending_launches=pending_launches,
            failed_nodes=failed_nodes,
            node_availability_summary=self.node_provider_availability_tracker.summary(),
            pending_resources=pending_resources,
            node_type_mapping=node_type_mapping,
            legacy=True,
        )

    def info_string(self):
        lm_summary = self.load_metrics.summary()
        autoscaler_summary = self.summary()
        assert autoscaler_summary
        return "\n" + format_info_string(lm_summary, autoscaler_summary)
