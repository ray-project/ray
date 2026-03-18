from collections import OrderedDict
from typing import Dict, List, Optional

from ray.core.generated.instance_manager_pb2 import TerminationRequest

_TERMINATION_CAUSE_REASON_MAP = {
    TerminationRequest.Cause.OUTDATED: "outdated",
    TerminationRequest.Cause.MAX_NUM_NODES: "max number of worker nodes reached",
    TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE: (
        "max number of worker nodes per type reached"
    ),
    TerminationRequest.Cause.IDLE: "idle",
}


def build_autoscaler_scheduling_update_rows(
    *,
    cluster_resources: Dict[str, float],
    launch_actions: Optional[List[Dict]] = None,
    terminate_actions: Optional[List[Dict]] = None,
    infeasible_resource_requests: Optional[List[Dict]] = None,
    infeasible_gang_resource_requests: Optional[List[Dict]] = None,
    infeasible_cluster_resource_constraints: Optional[List[Dict]] = None,
) -> List[Dict[str, str]]:
    """Build human-readable dashboard rows from a scaling decision payload.

    Used by both the autoscaler event logger (legacy export-event path) and the
    dashboard event storage (ONE-event rendering path).
    """
    rows = []

    if launch_actions:
        for launch_action in launch_actions:
            rows.append(
                {
                    "severity": "INFO",
                    "log_to_logger": True,
                    "message": (
                        "Adding "
                        f"{launch_action['count']} node(s) of type "
                        f"{launch_action['instance_type']}."
                    ),
                }
            )

    if terminate_actions:
        for terminate_action in terminate_actions:
            cause = terminate_action["cause"]
            cause_reason = _TERMINATION_CAUSE_REASON_MAP.get(cause, "unknown")
            rows.append(
                {
                    "severity": "INFO",
                    "log_to_logger": True,
                    "message": (
                        "Removing "
                        f"{terminate_action['count']} nodes of type "
                        f"{terminate_action['instance_type']} ({cause_reason})."
                    ),
                }
            )

    if launch_actions or terminate_actions:
        num_cpus = cluster_resources.get("CPU", 0)
        resize_message = f"Resized to {int(num_cpus)} CPUs"
        if "GPU" in cluster_resources:
            resize_message += f", {int(cluster_resources['GPU'])} GPUs"
        if "TPU" in cluster_resources:
            resize_message += f", {int(cluster_resources['TPU'])} TPUs"
        rows.append({"severity": "INFO", "message": f"{resize_message}."})

    if infeasible_resource_requests:
        log_str = "No available node types can fulfill resource requests "
        for idx, request in enumerate(infeasible_resource_requests):
            log_str += (
                f"{request['resources']}*{request['count']}"
                if "count" in request
                else str(request["resources"])
            )
            if idx < len(infeasible_resource_requests) - 1:
                log_str += ", "

            if request.get("label_constraints"):
                selector_strs = []
                for constraint in request["label_constraints"]:
                    values = ",".join(constraint["values"])
                    selector_strs.append(
                        f"{constraint['label_key']} "
                        f"{constraint['operator']} [{values}]"
                    )
                if selector_strs:
                    log_str += (
                        " with label selectors: [" + "; ".join(selector_strs) + "]"
                    )

        log_str += ". Add suitable node types to this cluster to resolve this issue."
        rows.append({"severity": "WARNING", "message": log_str})

    if infeasible_gang_resource_requests:
        for gang_request in infeasible_gang_resource_requests:
            log_str = (
                "No available node types can fulfill "
                f"placement group requests (detail={gang_request['details']}): "
            )
            requests_by_count = gang_request["bundles"]
            for idx, request in enumerate(requests_by_count):
                log_str += (
                    f"{request['resources']}*{request['count']}"
                    if "count" in request
                    else str(request["resources"])
                )
                if idx < len(requests_by_count) - 1:
                    log_str += ", "

            log_str += (
                ". Add suitable node types to this cluster to resolve this issue."
            )
            rows.append({"severity": "WARNING", "message": log_str})

    if infeasible_cluster_resource_constraints:
        for infeasible_constraint in infeasible_cluster_resource_constraints:
            log_str = "No available node types can fulfill cluster constraint: "
            resource_requests = infeasible_constraint["resource_requests"]
            for idx, request in enumerate(resource_requests):
                log_str += f"{request['request']}*{request['count']}"
                if idx < len(resource_requests) - 1:
                    log_str += ", "

            log_str += (
                ". Add suitable node types to this cluster to resolve this issue."
            )
            rows.append({"severity": "WARNING", "message": log_str})

    return rows


def group_resource_bundle_dicts(bundles: list[dict]) -> list[dict]:
    """Group resource bundle dicts by (resources, label_constraints), summing counts.

    Each bundle dict is expected to have:
      - "resources": dict[str, float]
      - "label_constraints": list[dict] (optional)
      - "count": int (optional, defaults to 1; 0 is treated as 1 for backward compat)

    Returns a list of grouped dicts with "resources", "count", and optionally
    "label_constraints".
    """
    grouped: "OrderedDict[tuple, dict]" = OrderedDict()
    for bundle in bundles:
        resources = bundle.get("resources", {})
        label_constraints = bundle.get("label_constraints", [])
        count = bundle.get("count", 1) or 1  # treat 0 as 1

        bundle_key = (
            tuple(sorted(resources.items())),
            tuple(
                (
                    constraint["label_key"],
                    constraint["operator"],
                    tuple(constraint["values"]),
                )
                for constraint in label_constraints
            ),
        )
        if bundle_key not in grouped:
            grouped[bundle_key] = {
                "resources": resources,
                "count": 0,
            }
            if label_constraints:
                grouped[bundle_key]["label_constraints"] = label_constraints
        grouped[bundle_key]["count"] += count
    return list(grouped.values())
