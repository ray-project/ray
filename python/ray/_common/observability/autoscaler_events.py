from typing import Dict, List, Optional

import ray._private.ray_constants as ray_constants
from ray._common.observability.internal_event import InternalEventBuilder
from ray._private.grpc_utils import epoch_to_protobuf_timestamp
from ray.core.generated.events_autoscaler_config_definition_event_pb2 import (
    AutoscalerConfigDefinitionEvent,
)
from ray.core.generated.events_autoscaler_node_provisioning_event_pb2 import (
    AutoscalerNodeProvisioningEvent,
)
from ray.core.generated.events_autoscaler_scaling_decision_event_pb2 import (
    AutoscalerScalingDecisionEvent,
)
from ray.core.generated.events_base_event_pb2 import RayEvent as RayEventProto


def is_ray_event_enabled() -> bool:
    """Check whether Python-side ONE-event publishing is active."""
    return ray_constants.RAY_ENABLE_PYTHON_RAY_EVENT


# ---------------------------------------------------------------------------
# AutoscalerConfigDefinitionEvent
# ---------------------------------------------------------------------------


class AutoscalerConfigDefinitionEventBuilder(InternalEventBuilder):
    """Builds an AutoscalerConfigDefinitionEvent.

    Emitted once when the autoscaler starts (or whenever config changes).
    """

    def __init__(
        self,
        autoscaler_version: int,
        cloud_provider_type: str,
        max_workers: int,
        available_node_types: List[Dict],
        upscaling_speed: float = 0.0,
        session_name: str = "",
    ):
        """Initialize the config definition event builder.

        Args:
            autoscaler_version: The autoscaler version enum value
                (AutoscalerConfigDefinitionEvent.AutoscalerVersion).
            cloud_provider_type: The type of cloud provider
                ("aws", "gcp", etc).
            max_workers: The maximum number of workers.
            available_node_types: List of dicts, each with keys
                ``node_type_name``, ``min_worker_nodes``,
                ``max_worker_nodes``, ``idle_timeout_s``,
                ``resources`` (dict), and ``labels`` (dict).
            upscaling_speed: The upscaling speed.
            session_name: The Ray session name.
        """
        super().__init__(
            source_type=RayEventProto.SourceType.AUTOSCALER,
            event_type=RayEventProto.EventType.AUTOSCALER_CONFIG_DEFINITION_EVENT,
            nested_event_field_number=RayEventProto.AUTOSCALER_CONFIG_DEFINITION_EVENT_FIELD_NUMBER,
            session_name=session_name,
        )
        self._autoscaler_version = autoscaler_version
        self._cloud_provider_type = cloud_provider_type
        self._max_workers = max_workers
        self._available_node_types = available_node_types
        self._upscaling_speed = upscaling_speed

    def get_entity_id(self) -> str:
        return "autoscaler_config"

    def serialize_event_data(self) -> bytes:
        event = AutoscalerConfigDefinitionEvent()
        event.autoscaler_version = self._autoscaler_version
        event.cloud_provider_type = self._cloud_provider_type
        event.max_workers = self._max_workers
        event.upscaling_speed = self._upscaling_speed

        for nt in self._available_node_types:
            cfg = event.available_node_types.add()
            cfg.node_type_name = nt.get("node_type_name", "")
            cfg.min_worker_nodes = nt.get("min_worker_nodes", 0)
            cfg.max_worker_nodes = nt.get("max_worker_nodes", 0)
            cfg.idle_timeout_s = nt.get("idle_timeout_s", -1)
            for k, v in nt.get("resources", {}).items():
                cfg.resources[k] = v
            for k, v in nt.get("labels", {}).items():
                cfg.labels[k] = v

        return event.SerializeToString()


# ---------------------------------------------------------------------------
# AutoscalerScalingDecisionEvent
# ---------------------------------------------------------------------------


class AutoscalerScalingDecisionEventBuilder(InternalEventBuilder):
    """Builds an AutoscalerScalingDecisionEvent.

    Emitted for every autoscaler scaling decision.
    """

    def __init__(
        self,
        launch_actions: Optional[List[Dict]] = None,
        terminate_actions: Optional[List[Dict]] = None,
        cluster_resources_after: Optional[Dict[str, float]] = None,
        infeasible_resource_requests: Optional[List[Dict]] = None,
        infeasible_gang_resource_requests: Optional[List[Dict]] = None,
        infeasible_cluster_resource_constraints: Optional[List[Dict]] = None,
        session_name: str = "",
    ):
        """Initialize the scaling decision event builder.

        Args:
            launch_actions: List of dicts with ``instance_type``
                and ``count`` keys.
            terminate_actions: List of dicts with ``cause``
                (TerminationCause enum int), ``instance_type``,
                and ``count`` keys.
            cluster_resources_after: Resource name to amount mapping.
            infeasible_resource_requests: List of dicts with
                ``resources`` (dict) and optional
                ``label_constraints`` (list of dicts with
                ``label_key``, ``operator``, ``values``).
            infeasible_gang_resource_requests: List of dicts with
                ``bundles`` (list) and ``details`` (str) keys.
            infeasible_cluster_resource_constraints: List of dicts
                with ``resource_requests`` containing request/count
                pairs.
            session_name: The Ray session name.
        """
        super().__init__(
            source_type=RayEventProto.SourceType.AUTOSCALER,
            event_type=RayEventProto.EventType.AUTOSCALER_SCALING_DECISION_EVENT,
            nested_event_field_number=RayEventProto.AUTOSCALER_SCALING_DECISION_EVENT_FIELD_NUMBER,
            session_name=session_name,
        )
        self._launch_actions = launch_actions or []
        self._terminate_actions = terminate_actions or []
        self._cluster_resources_after = cluster_resources_after or {}
        self._infeasible_resource_requests = infeasible_resource_requests or []
        self._infeasible_gang_resource_requests = (
            infeasible_gang_resource_requests or []
        )
        self._infeasible_cluster_resource_constraints = (
            infeasible_cluster_resource_constraints or []
        )

    def get_entity_id(self) -> str:
        return "autoscaler_scaling_decision"

    def serialize_event_data(self) -> bytes:
        event = AutoscalerScalingDecisionEvent()

        for launch_action in self._launch_actions:
            action = event.launch_actions.add()
            action.instance_type = launch_action.get("instance_type", "")
            action.count = launch_action.get("count", 0)

        for terminate_action in self._terminate_actions:
            action = event.terminate_actions.add()
            action.cause = terminate_action.get("cause", 0)
            action.instance_type = terminate_action.get("instance_type", "")
            action.count = terminate_action.get("count", 0)

        for k, v in self._cluster_resources_after.items():
            event.cluster_resources_after[k] = v

        for req in self._infeasible_resource_requests:
            bundle = event.infeasible_resource_requests.add()
            for k, v in req.get("resources", {}).items():
                bundle.resources[k] = v
            for label_constraint in req.get("label_constraints", []):
                constraint = bundle.label_constraints.add()
                constraint.label_key = label_constraint.get("label_key", "")
                constraint.operator = label_constraint.get("operator", "")
                for val in label_constraint.get("values", []):
                    constraint.values.append(val)

        for gang in self._infeasible_gang_resource_requests:
            gang_msg = event.infeasible_gang_resource_requests.add()
            for b in gang.get("bundles", []):
                bundle = gang_msg.bundles.add()
                for k, v in b.items():
                    bundle.resources[k] = v
            gang_msg.details = gang.get("details", "")

        for constraint in self._infeasible_cluster_resource_constraints:
            constraint_msg = event.infeasible_cluster_resource_constraints.add()
            for rr in constraint.get("resource_requests", []):
                rr_msg = constraint_msg.resource_requests.add()
                for k, v in rr.get("request", {}).items():
                    rr_msg.request.resources[k] = v
                rr_msg.count = rr.get("count", 0)

        return event.SerializeToString()


# ---------------------------------------------------------------------------
# AutoscalerNodeProvisioningEvent
# ---------------------------------------------------------------------------


class AutoscalerNodeProvisioningEventBuilder(InternalEventBuilder):
    """Builds an AutoscalerNodeProvisioningEvent.

    Emitted when pending/failed instance state changes.
    """

    def __init__(
        self,
        requested_instances: Optional[List[Dict]] = None,
        allocated_instances: Optional[List[Dict]] = None,
        failed_instances: Optional[List[Dict]] = None,
        session_name: str = "",
    ):
        """Initialize the node provisioning event builder.

        Args:
            requested_instances: List of dicts with
                ``instance_type_name``, ``ray_node_type_name``,
                ``count``, and optional ``request_ts`` (float epoch
                seconds) keys.
            allocated_instances: List of dicts with
                ``instance_type_name``, ``ray_node_type_name``,
                ``instance_id``, and ``ip_address`` keys.
            failed_instances: List of dicts with
                ``instance_type_name``, ``ray_node_type_name``,
                ``count``, ``reason``, ``start_ts`` (float), and
                ``failed_ts`` (float) keys.
            session_name: The Ray session name.
        """
        super().__init__(
            source_type=RayEventProto.SourceType.AUTOSCALER,
            event_type=RayEventProto.EventType.AUTOSCALER_NODE_PROVISIONING_EVENT,
            nested_event_field_number=RayEventProto.AUTOSCALER_NODE_PROVISIONING_EVENT_FIELD_NUMBER,
            session_name=session_name,
        )
        self._requested_instances = requested_instances or []
        self._allocated_instances = allocated_instances or []
        self._failed_instances = failed_instances or []

    def get_entity_id(self) -> str:
        return "autoscaler_provisioning"

    def serialize_event_data(self) -> bytes:
        event = AutoscalerNodeProvisioningEvent()

        for requested_instance in self._requested_instances:
            msg = event.requested_instances.add()
            msg.instance_type_name = requested_instance.get("instance_type_name", "")
            msg.ray_node_type_name = requested_instance.get("ray_node_type_name", "")
            msg.count = requested_instance.get("count", 0)
            if requested_instance.get("request_ts", 0) > 0:
                msg.request_ts.CopyFrom(
                    epoch_to_protobuf_timestamp(requested_instance["request_ts"])
                )

        for allocated_instance in self._allocated_instances:
            msg = event.allocated_instances.add()
            msg.instance_type_name = allocated_instance.get("instance_type_name", "")
            msg.ray_node_type_name = allocated_instance.get("ray_node_type_name", "")
            msg.instance_id = allocated_instance.get("instance_id", "")
            msg.ip_address = allocated_instance.get("ip_address", "")

        for failed_instance in self._failed_instances:
            msg = event.failed_instances.add()
            msg.instance_type_name = failed_instance.get("instance_type_name", "")
            msg.ray_node_type_name = failed_instance.get("ray_node_type_name", "")
            msg.count = failed_instance.get("count", 0)
            msg.reason = failed_instance.get("reason", "")
            if failed_instance.get("start_ts", 0) > 0:
                msg.start_ts.CopyFrom(
                    epoch_to_protobuf_timestamp(failed_instance["start_ts"])
                )
            if failed_instance.get("failed_ts", 0) > 0:
                msg.failed_ts.CopyFrom(
                    epoch_to_protobuf_timestamp(failed_instance["failed_ts"])
                )

        return event.SerializeToString()
