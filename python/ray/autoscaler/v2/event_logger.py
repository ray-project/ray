import hashlib
import json
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, List, Optional

from ray._private.event.event_logger import EventLoggerAdapter
from ray.autoscaler.v2.utils import ResourceRequestUtil
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceConstraint,
    GangResourceRequest,
    ResourceRequest,
)
from ray.core.generated.common_pb2 import LabelSelectorOperator
from ray.core.generated.instance_manager_pb2 import LaunchRequest, TerminationRequest

if TYPE_CHECKING:
    from ray._common.observability.dashboard_head_event_publisher import (
        DashboardHeadRayEventPublisher,
    )
    from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
    from ray.core.generated.autoscaler_pb2 import AutoscalingState

logger = logging.getLogger(__name__)


class AutoscalerEventLogger:
    """
    Logs events related to the autoscaler.

    When ONE-event is enabled (``RAY_enable_python_ray_event=true``), structured
    events are published through the dashboard head and the legacy export-event
    logger is skipped. Otherwise only the legacy export-event logger is used.

    # TODO:
    - Add more logging for other events.
    - Rate limit the events if too spammy.
    """

    def __init__(
        self,
        export_event_logger: Optional[EventLoggerAdapter] = None,
        ray_event_publisher: Optional["DashboardHeadRayEventPublisher"] = None,
        session_name: str = "",
    ):
        self._export_event_logger = export_event_logger
        self._ray_event_publisher = ray_event_publisher
        self._session_name = session_name
        self._last_scaling_decision_hash: str = ""

    def log_node_provisioning(
        self,
        autoscaling_state: "AutoscalingState",
    ) -> None:
        """Log node provisioning state.

        Emitted when the provisioning state (pending/allocated/failed
        instances) changes.  Only emitted when ONE-event is enabled.
        """
        if self._ray_event_publisher is None:
            return

        self._emit_node_provisioning_event(autoscaling_state)

    def log_config_definition(
        self,
        config: "AutoscalingConfig",
    ) -> None:
        """Log the autoscaler configuration.

        Emitted once at autoscaler startup and whenever the config changes.
        Only emitted when ONE-event is enabled (a Ray event publisher is set).
        """
        if self._ray_event_publisher is None:
            return

        self._emit_config_definition_event(config)

    def log_cluster_scheduling_update(
        self,
        cluster_resources: Dict[str, float],
        launch_requests: Optional[List[LaunchRequest]] = None,
        terminate_requests: Optional[List[TerminationRequest]] = None,
        infeasible_requests: Optional[List[ResourceRequest]] = None,
        infeasible_gang_requests: Optional[List[GangResourceRequest]] = None,
        infeasible_cluster_resource_constraints: Optional[
            List[ClusterResourceConstraint]
        ] = None,
    ) -> None:
        """
        Log updates to the autoscaler scheduling state.

        Emits:
        - info logs for node launches and terminations (counts grouped by node type).
        - an info log summarizing the cluster size after a resize (CPUs/GPUs/TPUs).
        - warnings describing infeasible single resource requests, infeasible gang
          (placement group) requests, and infeasible cluster resource constraints.

        Args:
            cluster_resources: Mapping of resource name to total resources for the
                current cluster state.
            launch_requests: Node launch requests issued in this scheduling step.
            terminate_requests: Node termination requests issued in this scheduling
                step.
            infeasible_requests: Resource requests that could not be satisfied by
                any available node type.
            infeasible_gang_requests: Gang/placement group requests that could not
                be scheduled.
            infeasible_cluster_resource_constraints: Cluster-level resource
                constraints that could not be satisfied.

        Returns:
            None
        """

        if self._ray_event_publisher is not None:
            # ONE-event path: publish a structured RayEvent through dashboard head.
            self._emit_scaling_decision_event(
                cluster_resources=cluster_resources,
                launch_requests=launch_requests,
                terminate_requests=terminate_requests,
                infeasible_requests=infeasible_requests,
                infeasible_gang_requests=infeasible_gang_requests,
                infeasible_cluster_resource_constraints=(
                    infeasible_cluster_resource_constraints
                ),
            )
        elif self._export_event_logger is not None:
            # Legacy export-event path.
            self._log_export_events(
                cluster_resources=cluster_resources,
                launch_requests=launch_requests,
                terminate_requests=terminate_requests,
                infeasible_requests=infeasible_requests,
                infeasible_gang_requests=infeasible_gang_requests,
                infeasible_cluster_resource_constraints=(
                    infeasible_cluster_resource_constraints
                ),
            )

    # ------------------------------------------------------------------
    # Legacy export-event logger
    # ------------------------------------------------------------------

    def _log_export_events(
        self,
        cluster_resources: Dict[str, float],
        launch_requests: Optional[List[LaunchRequest]] = None,
        terminate_requests: Optional[List[TerminationRequest]] = None,
        infeasible_requests: Optional[List[ResourceRequest]] = None,
        infeasible_gang_requests: Optional[List[GangResourceRequest]] = None,
        infeasible_cluster_resource_constraints: Optional[
            List[ClusterResourceConstraint]
        ] = None,
    ) -> None:
        # Log any launch events.
        if launch_requests:
            launch_type_count = defaultdict(int)
            for req in launch_requests:
                launch_type_count[req.instance_type] += req.count

            for idx, (instance_type, count) in enumerate(launch_type_count.items()):
                log_str = f"Adding {count} node(s) of type {instance_type}."
                self._export_event_logger.info(f"{log_str}")
                logger.info(f"{log_str}")

        # Log any terminate events.
        if terminate_requests:
            termination_by_causes_and_type = defaultdict(int)
            for req in terminate_requests:
                termination_by_causes_and_type[(req.cause, req.instance_type)] += 1

            cause_reason_map = {
                TerminationRequest.Cause.OUTDATED: "outdated",
                TerminationRequest.Cause.MAX_NUM_NODES: "max number of worker nodes reached",  # noqa
                TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE: "max number of worker nodes per type reached",  # noqa
                TerminationRequest.Cause.IDLE: "idle",
            }

            for idx, ((cause, instance_type), count) in enumerate(
                termination_by_causes_and_type.items()
            ):
                log_str = f"Removing {count} nodes of type {instance_type} ({cause_reason_map[cause]})."  # noqa
                self._export_event_logger.info(f"{log_str}")
                logger.info(f"{log_str}")

        # Cluster shape changes.
        if launch_requests or terminate_requests:
            num_cpus = cluster_resources.get("CPU", 0)
            log_str = f"Resized to {int(num_cpus)} CPUs"

            if "GPU" in cluster_resources:
                log_str += f", {int(cluster_resources['GPU'])} GPUs"
            if "TPU" in cluster_resources:
                log_str += f", {int(cluster_resources['TPU'])} TPUs"

            self._export_event_logger.info(f"{log_str}.")
            self._export_event_logger.debug(
                f"Current cluster resources: {dict(cluster_resources)}."
            )

        # Log any infeasible requests.
        if infeasible_requests:
            requests_by_count = ResourceRequestUtil.group_by_count(infeasible_requests)
            log_str = "No available node types can fulfill resource requests "
            for idx, req_count in enumerate(requests_by_count):
                resource_map = ResourceRequestUtil.to_resource_map(req_count.request)
                log_str += f"{resource_map}*{req_count.count}"
                if idx < len(requests_by_count) - 1:
                    log_str += ", "

                # Parse and log label selectors if present
                if req_count.request.label_selectors:
                    selector_strs = []
                    for selector in req_count.request.label_selectors:
                        for constraint in selector.label_constraints:
                            op = LabelSelectorOperator.Name(constraint.operator)
                            values = ",".join(constraint.label_values)
                            selector_strs.append(
                                f"{constraint.label_key} {op} [{values}]"
                            )
                    if selector_strs:
                        log_str += (
                            " with label selectors: [" + "; ".join(selector_strs) + "]"
                        )

            log_str += (
                ". Add suitable node types to this cluster to resolve this issue."
            )
            self._export_event_logger.warning(log_str)

        if infeasible_gang_requests:
            # Log for each placement group requests.
            for gang_request in infeasible_gang_requests:
                log_str = (
                    "No available node types can fulfill "
                    "placement group requests (detail={details}): ".format(
                        details=gang_request.details
                    )
                )
                requests_by_count = ResourceRequestUtil.group_by_count(
                    gang_request.requests
                )
                for idx, req_count in enumerate(requests_by_count):
                    resource_map = ResourceRequestUtil.to_resource_map(
                        req_count.request
                    )
                    log_str += f"{resource_map}*{req_count.count}"
                    if idx < len(requests_by_count) - 1:
                        log_str += ", "

                log_str += (
                    ". Add suitable node types to this cluster to resolve this issue."
                )
                self._export_event_logger.warning(log_str)

        if infeasible_cluster_resource_constraints:
            # We will only have max 1 cluster resource constraint for now since it's
            # from `request_resources()` sdk, where the most recent call would override
            # the previous one.
            for infeasible_constraint in infeasible_cluster_resource_constraints:
                log_str = "No available node types can fulfill cluster constraint: "
                for i, requests_by_count in enumerate(
                    infeasible_constraint.resource_requests
                ):
                    resource_map = ResourceRequestUtil.to_resource_map(
                        requests_by_count.request
                    )
                    log_str += f"{resource_map}*{requests_by_count.count}"
                    if i < len(infeasible_constraint.resource_requests) - 1:
                        log_str += ", "

                log_str += (
                    ". Add suitable node types to this cluster to resolve this issue."
                )
                self._export_event_logger.warning(log_str)

    # ------------------------------------------------------------------
    # ONE-event structured emission
    # ------------------------------------------------------------------

    def _emit_scaling_decision_event(
        self,
        cluster_resources: Dict[str, float],
        launch_requests: Optional[List[LaunchRequest]] = None,
        terminate_requests: Optional[List[TerminationRequest]] = None,
        infeasible_requests: Optional[List[ResourceRequest]] = None,
        infeasible_gang_requests: Optional[List[GangResourceRequest]] = None,
        infeasible_cluster_resource_constraints: Optional[
            List[ClusterResourceConstraint]
        ] = None,
    ) -> None:
        """Publish a structured AutoscalerScalingDecisionEvent."""
        from ray._common.observability.autoscaler_events import (
            AutoscalerScalingDecisionEventBuilder,
        )

        # Convert LaunchRequest protos to dicts for the builder.
        launch_actions = []
        if launch_requests:
            for req in launch_requests:
                launch_actions.append(
                    {"instance_type": req.instance_type, "count": req.count}
                )

        # Convert TerminationRequest protos to dicts for the builder.
        # TerminationRequest.Cause enum values match the
        # AutoscalerScalingDecisionEvent.TerminateAction.TerminationCause values.
        terminate_actions = []
        if terminate_requests:
            termination_by_causes_and_type = defaultdict(int)
            for req in terminate_requests:
                termination_by_causes_and_type[(req.cause, req.instance_type)] += 1
            for (cause, instance_type), count in termination_by_causes_and_type.items():
                terminate_actions.append(
                    {
                        "cause": cause,
                        "instance_type": instance_type,
                        "count": count,
                    }
                )

        # Convert infeasible ResourceRequest protos to resource dicts
        # with label constraints preserved.
        infeasible_resource_dicts = []
        if infeasible_requests:
            for req in infeasible_requests:
                entry = {"resources": ResourceRequestUtil.to_resource_map(req)}
                label_constraints = []
                for selector in req.label_selectors:
                    for constraint in selector.label_constraints:
                        label_constraints.append(
                            {
                                "label_key": constraint.label_key,
                                "operator": LabelSelectorOperator.Name(
                                    constraint.operator
                                ),
                                "values": list(constraint.label_values),
                            }
                        )
                if label_constraints:
                    entry["label_constraints"] = label_constraints
                infeasible_resource_dicts.append(entry)

        # Convert infeasible gang requests.
        infeasible_gang_dicts = []
        if infeasible_gang_requests:
            for gang in infeasible_gang_requests:
                bundles = [
                    ResourceRequestUtil.to_resource_map(r) for r in gang.requests
                ]
                infeasible_gang_dicts.append(
                    {"bundles": bundles, "details": gang.details}
                )

        # Convert infeasible cluster resource constraints.
        infeasible_constraint_dicts = []
        if infeasible_cluster_resource_constraints:
            for constraint in infeasible_cluster_resource_constraints:
                resource_requests = []
                for rr in constraint.resource_requests:
                    resource_requests.append(
                        {
                            "request": ResourceRequestUtil.to_resource_map(rr.request),
                            "count": rr.count,
                        }
                    )
                infeasible_constraint_dicts.append(
                    {"resource_requests": resource_requests}
                )

        payload_hash = hashlib.sha256(
            json.dumps(
                {
                    "launch_actions": launch_actions,
                    "terminate_actions": terminate_actions,
                    "cluster_resources_after": dict(cluster_resources),
                    "infeasible_resource_requests": infeasible_resource_dicts,
                    "infeasible_gang_resource_requests": infeasible_gang_dicts,
                    "infeasible_cluster_resource_constraints": (
                        infeasible_constraint_dicts
                    ),
                },
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        if payload_hash == self._last_scaling_decision_hash:
            return

        try:
            builder = AutoscalerScalingDecisionEventBuilder(
                launch_actions=launch_actions,
                terminate_actions=terminate_actions,
                cluster_resources_after=dict(cluster_resources),
                infeasible_resource_requests=infeasible_resource_dicts,
                infeasible_gang_resource_requests=infeasible_gang_dicts,
                infeasible_cluster_resource_constraints=infeasible_constraint_dicts,
                session_name=self._session_name,
            )
            event = builder.build()
            self._ray_event_publisher.publish(event)
            self._last_scaling_decision_hash = payload_hash
        except Exception:
            logger.exception("Failed to emit AutoscalerScalingDecisionEvent.")

    def _emit_config_definition_event(
        self,
        config: "AutoscalingConfig",
    ) -> None:
        """Publish a structured AutoscalerConfigDefinitionEvent."""
        from ray._common.observability.autoscaler_events import (
            AutoscalerConfigDefinitionEventBuilder,
        )

        node_type_configs = config.get_node_type_configs() or {}
        available_node_types = []
        for node_type_name, nt_cfg in node_type_configs.items():
            available_node_types.append(
                {
                    "node_type_name": node_type_name,
                    "min_worker_nodes": nt_cfg.min_worker_nodes,
                    "max_worker_nodes": nt_cfg.max_worker_nodes,
                    "idle_timeout_s": nt_cfg.idle_timeout_s
                    if nt_cfg.idle_timeout_s is not None
                    else -1,
                    "resources": dict(nt_cfg.resources),
                    "labels": dict(nt_cfg.labels),
                }
            )

        try:
            from ray.core.generated.events_autoscaler_config_definition_event_pb2 import (  # noqa
                AutoscalerConfigDefinitionEvent,
            )

            builder = AutoscalerConfigDefinitionEventBuilder(
                autoscaler_version=AutoscalerConfigDefinitionEvent.V2,
                cloud_provider_type=config.provider.name.lower(),
                max_workers=config.get_max_num_worker_nodes() or 0,
                available_node_types=available_node_types,
                upscaling_speed=config.get_upscaling_speed(),
                session_name=self._session_name,
            )
            event = builder.build()
            self._ray_event_publisher.publish(event)
        except Exception:
            logger.exception("Failed to emit AutoscalerConfigDefinitionEvent.")

    def _emit_node_provisioning_event(
        self,
        autoscaling_state: "AutoscalingState",
    ) -> None:
        """Publish a structured AutoscalerNodeProvisioningEvent."""
        from ray._common.observability.autoscaler_events import (
            AutoscalerNodeProvisioningEventBuilder,
        )

        requested_instances = []
        for req in autoscaling_state.pending_instance_requests:
            requested_instances.append(
                {
                    "instance_type_name": req.instance_type_name,
                    "ray_node_type_name": req.ray_node_type_name,
                    "count": req.count,
                    "request_ts": req.request_ts,
                }
            )

        allocated_instances = []
        for inst in autoscaling_state.pending_instances:
            allocated_instances.append(
                {
                    "instance_type_name": inst.instance_type_name,
                    "ray_node_type_name": inst.ray_node_type_name,
                    "instance_id": inst.instance_id,
                    "ip_address": inst.ip_address,
                }
            )

        failed_instances = []
        for req in autoscaling_state.failed_instance_requests:
            failed_instances.append(
                {
                    "instance_type_name": req.instance_type_name,
                    "ray_node_type_name": req.ray_node_type_name,
                    "count": req.count,
                    "reason": req.reason,
                    "start_ts": req.start_ts,
                    "failed_ts": req.failed_ts,
                }
            )

        try:
            builder = AutoscalerNodeProvisioningEventBuilder(
                requested_instances=requested_instances,
                allocated_instances=allocated_instances,
                failed_instances=failed_instances,
                session_name=self._session_name,
            )
            event = builder.build()
            self._ray_event_publisher.publish(event)
        except Exception:
            logger.exception("Failed to emit AutoscalerNodeProvisioningEvent.")
