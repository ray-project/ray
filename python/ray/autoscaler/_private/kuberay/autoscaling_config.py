import decimal
import json
import logging
import re
import time
from itertools import chain
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

from ray._private.accelerators import get_all_accelerator_managers
from ray._private.label_utils import (
    validate_node_label_syntax,
)
from ray.autoscaler._private.constants import (
    DISABLE_LAUNCH_CONFIG_CHECK_KEY,
    DISABLE_NODE_UPDATERS_KEY,
    FOREGROUND_NODE_LAUNCH_KEY,
    WORKER_LIVENESS_CHECK_KEY,
)
from ray.autoscaler._private.kuberay import node_provider, utils
from ray.autoscaler._private.util import validate_config
from ray.util.debug import log_once

logger = logging.getLogger(__name__)

AUTOSCALER_OPTIONS_KEY = "autoscalerOptions"
IDLE_SECONDS_KEY = "idleTimeoutSeconds"
PRIORITY_KEY = "priority"
UPSCALING_KEY = "upscalingMode"
UPSCALING_VALUE_AGGRESSIVE = "Aggressive"
UPSCALING_VALUE_DEFAULT = "Default"
UPSCALING_VALUE_CONSERVATIVE = "Conservative"

MAX_RAYCLUSTER_FETCH_TRIES = 5
RAYCLUSTER_FETCH_RETRY_S = 5

# Logical group name for the KubeRay head group.
# Used as the name of the "head node type" by the autoscaler.
_HEAD_GROUP_NAME = "headgroup"

RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION = "ray.io/multihost-topology-key"
RAY_MULTIHOST_TOPOLOGY_EXCLUSIVE_ANNOTATION = "ray.io/multihost-topology-exclusive"

GKE_SLICE_SCHEDULER_NAME = "slice-scheduler"
KUEUE_TOPOLOGY_SCHEDULING_GATE = "kueue.x-k8s.io/topology"
GKE_TPU_SLICE_PREFIX = "slice.tpu.google.com/"
GKE_NODEPOOL_LABEL_KEY = "cloud.google.com/gke-nodepool"
GKE_TPU_SLICE_LABEL_KEY = "cloud.google.com/gke-tpu-slice"
GKE_TPU_PARTITION_PREFIX = "cloud.google.com/gke-tpu-partition-"
GKE_TPU_SLICE_ID_PREFIX = "cloud.google.com/gke-tpu-slice-"
GKE_TPU_ID_SUFFIX = "-id"

EXTERNAL_PLACEMENT_NONEMPTY_KEYS = (
    "volcano.sh/network-topology-mode",
    "kueue.x-k8s.io/podset-required-topology",
    "kueue.x-k8s.io/podset-preferred-topology",
    "kueue.x-k8s.io/podset-slice-required-topology",
    "kueue.x-k8s.io/podset-slice-required-topology-constraints",
)
KUEUE_PODSET_UNCONSTRAINED_TOPOLOGY_ANNOTATION = (
    "kueue.x-k8s.io/podset-unconstrained-topology"
)

_NODE_LABEL_FIELD_PATH_RE = re.compile(r"\Ametadata\.labels\['([^']+)'\]\Z")
_K8S_DNS1123_SUBDOMAIN_MAX_LEN = 253
_K8S_QUALIFIED_NAME_MAX_LEN = 63
_K8S_DNS1123_LABEL_FMT = r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?"
_K8S_DNS1123_SUBDOMAIN_RE = re.compile(
    rf"\A{_K8S_DNS1123_LABEL_FMT}(?:\.{_K8S_DNS1123_LABEL_FMT})*\Z"
)
_K8S_QUALIFIED_NAME_RE = re.compile(r"\A(?:[A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]\Z")


class AutoscalingConfigProducer:
    """Produces an autoscaling config by reading data from the RayCluster CR.

    Used to fetch the autoscaling config at the beginning of each autoscaler iteration.

    In the context of Ray deployment on Kubernetes, the autoscaling config is an
    internal interface.

    The autoscaling config carries the strict subset of RayCluster CR data required by
    the autoscaler to make scaling decisions; in particular, the autoscaling config does
    not carry pod configuration data.

    This class is the only public object in this file.
    """

    def __init__(self, ray_cluster_name, ray_cluster_namespace):
        self.kubernetes_api_client = node_provider.KubernetesHttpApiClient(
            namespace=ray_cluster_namespace
        )
        self._ray_cr_path = f"rayclusters/{ray_cluster_name}"

    def __call__(self):
        ray_cr = self._fetch_ray_cr_from_k8s_with_retries()
        autoscaling_config = _derive_autoscaling_config_from_ray_cr(ray_cr)
        return autoscaling_config

    def _fetch_ray_cr_from_k8s_with_retries(self) -> Dict[str, Any]:
        """Fetch the RayCluster CR by querying the K8s API server.

        Retry on HTTPError for robustness, in particular to protect autoscaler
        initialization.
        """
        for i in range(1, MAX_RAYCLUSTER_FETCH_TRIES + 1):
            try:
                return self.kubernetes_api_client.get(self._ray_cr_path)
            except requests.HTTPError as e:
                if i < MAX_RAYCLUSTER_FETCH_TRIES:
                    logger.exception(
                        "Failed to fetch RayCluster CR from K8s. Retrying."
                    )
                    time.sleep(RAYCLUSTER_FETCH_RETRY_S)
                else:
                    raise e from None

        # This branch is inaccessible. Raise to satisfy mypy.
        raise AssertionError


def _derive_autoscaling_config_from_ray_cr(ray_cr: Dict[str, Any]) -> Dict[str, Any]:
    provider_config = _generate_provider_config(ray_cr["metadata"]["namespace"])

    available_node_types = _generate_available_node_types_from_ray_cr_spec(
        ray_cr["spec"], ray_cr_metadata=ray_cr.get("metadata")
    )

    # The autoscaler expects a global max workers field. We set it to the sum of
    # node type max workers.
    global_max_workers = sum(
        node_type["max_workers"] for node_type in available_node_types.values()
    )

    # Legacy autoscaling fields carry no information but are required for compatibility.
    legacy_autoscaling_fields = _generate_legacy_autoscaling_config_fields()

    # Process autoscaler options.
    autoscaler_options = ray_cr["spec"].get(AUTOSCALER_OPTIONS_KEY, {})
    if IDLE_SECONDS_KEY in autoscaler_options:
        idle_timeout_minutes = autoscaler_options[IDLE_SECONDS_KEY] / 60.0
    else:
        idle_timeout_minutes = 1.0

    if autoscaler_options.get(UPSCALING_KEY) == UPSCALING_VALUE_CONSERVATIVE:
        upscaling_speed = 1  # Rate-limit upscaling if "Conservative" is set by user.
    # This elif is redudant but included for clarity.
    elif autoscaler_options.get(UPSCALING_KEY) == UPSCALING_VALUE_DEFAULT:
        upscaling_speed = 1000  # i.e. big, no rate-limiting by default
    # This elif is redudant but included for clarity.
    elif autoscaler_options.get(UPSCALING_KEY) == UPSCALING_VALUE_AGGRESSIVE:
        upscaling_speed = 1000
    else:
        upscaling_speed = 1000

    autoscaling_config = {
        "provider": provider_config,
        "cluster_name": ray_cr["metadata"]["name"],
        "head_node_type": _HEAD_GROUP_NAME,
        "available_node_types": available_node_types,
        "max_workers": global_max_workers,
        # Should consider exposing `idleTimeoutMinutes` in the RayCluster CRD,
        # under an `autoscaling` field.
        "idle_timeout_minutes": idle_timeout_minutes,
        # Should consider exposing `upscalingSpeed` in the RayCluster CRD,
        # under an `autoscaling` field.
        "upscaling_speed": upscaling_speed,
        **legacy_autoscaling_fields,
    }

    # Make sure the config is readable by the autoscaler.
    validate_config(autoscaling_config)

    return autoscaling_config


def _generate_provider_config(ray_cluster_namespace: str) -> Dict[str, Any]:
    """Generates the `provider` field of the autoscaling config, which carries data
    required to instantiate the KubeRay node provider.
    """
    return {
        "type": "kuberay",
        "namespace": ray_cluster_namespace,
        DISABLE_NODE_UPDATERS_KEY: True,
        DISABLE_LAUNCH_CONFIG_CHECK_KEY: True,
        FOREGROUND_NODE_LAUNCH_KEY: True,
        WORKER_LIVENESS_CHECK_KEY: False,
    }


def _generate_legacy_autoscaling_config_fields() -> Dict[str, Any]:
    """Generates legacy autoscaling config fields required for compatibiliy."""
    return {
        "file_mounts": {},
        "cluster_synced_files": [],
        "file_mounts_sync_continuously": False,
        "initialization_commands": [],
        "setup_commands": [],
        "head_setup_commands": [],
        "worker_setup_commands": [],
        "head_start_ray_commands": [],
        "worker_start_ray_commands": [],
        "auth": {},
    }


def is_exclusive_tpu_slice_topology_key(key: str) -> bool:
    """Return True iff `key` identifies an exclusive GKE TPU slice topology key.

    Supports node-pool, TPU slice, and Dynamic Slicing partition/slice ID keys
    for planned KubeRay multi-host topology coordination.
    """
    if not isinstance(key, str):
        return False
    if key in (GKE_NODEPOOL_LABEL_KEY, GKE_TPU_SLICE_LABEL_KEY):
        return True
    if not key.endswith(GKE_TPU_ID_SUFFIX):
        return False
    trimmed = key[: -len(GKE_TPU_ID_SUFFIX)]
    for prefix in (GKE_TPU_PARTITION_PREFIX, GKE_TPU_SLICE_ID_PREFIX):
        if trimmed.startswith(prefix) and len(trimmed) > len(prefix):
            return True
    return False


def _is_k8s_qualified_name(value: str) -> bool:
    """Return True iff `value` satisfies Kubernetes validation.IsQualifiedName."""
    if not isinstance(value, str):
        return False
    parts = value.split("/")
    if len(parts) == 1:
        name = parts[0]
    elif len(parts) == 2:
        prefix, name = parts
        if (
            not prefix
            or len(prefix) > _K8S_DNS1123_SUBDOMAIN_MAX_LEN
            or not _K8S_DNS1123_SUBDOMAIN_RE.match(prefix)
        ):
            return False
    else:
        return False

    return (
        1 <= len(name) <= _K8S_QUALIFIED_NAME_MAX_LEN
        and _K8S_QUALIFIED_NAME_RE.match(name) is not None
    )


def _parse_worker_group_label_refs(
    group_spec: Dict[str, Any],
) -> Tuple[List[str], Dict[str, str]]:
    """Parse workerGroupSpecs[].labelRefs into delivered Ray keys and a lookup map.

    Validates Kubernetes qualified names (`validation.IsQualifiedName`) on both
    the Kubernetes node label key and delivered Ray label key for KubeRay
    `labelRefs` (`kuberay#5329`, `kuberay#5501`), skipping entries with
    malformed syntax or duplicate delivered Ray label keys.

    Args:
        group_spec: Worker group specification dictionary from the RayCluster CR.

    Returns:
        A tuple of `(delivered_keys, node_label_to_ray_key)`, where
        `delivered_keys` is the deduplicated list of delivered Ray label keys
        preserving first-seen order and `node_label_to_ray_key` maps each
        Kubernetes node label key to its first delivered Ray label key.
    """
    raw_refs = group_spec.get("labelRefs")
    if not isinstance(raw_refs, list):
        return [], {}

    delivered_keys: List[str] = []
    seen_delivered: Set[str] = set()
    node_label_to_ray_key: Dict[str, str] = {}

    for ref in raw_refs:
        if not isinstance(ref, dict):
            continue
        value_from = ref.get("valueFrom")
        if not isinstance(value_from, dict):
            continue
        node_ref = value_from.get("nodeRef")
        if not isinstance(node_ref, dict):
            continue
        field_path = node_ref.get("fieldPath")
        if not isinstance(field_path, str):
            continue
        match = _NODE_LABEL_FIELD_PATH_RE.match(field_path)
        if not match:
            continue
        node_label_key = match.group(1)
        if not _is_k8s_qualified_name(node_label_key):
            continue

        raw_name = ref.get("name")
        if raw_name is None or raw_name == "":
            delivered_key = node_label_key
        elif isinstance(raw_name, str) and _is_k8s_qualified_name(raw_name):
            delivered_key = raw_name
        else:
            continue

        if delivered_key in seen_delivered:
            continue
        seen_delivered.add(delivered_key)
        delivered_keys.append(delivered_key)
        node_label_to_ray_key.setdefault(node_label_key, delivered_key)

    return delivered_keys, node_label_to_ray_key


def _lookup_multihost_annotation(
    ray_cr_metadata: Optional[Dict[str, Any]],
    pod_annotations: Dict[str, Any],
    key: str,
) -> Tuple[Optional[str], bool]:
    """Resolve a planned KubeRay multi-host topology annotation (pod template over RayCluster by key presence)."""
    if isinstance(pod_annotations, dict) and key in pod_annotations:
        val = pod_annotations[key]
        return (val if isinstance(val, str) else ""), True
    cluster_annotations = (
        ray_cr_metadata.get("annotations")
        if isinstance(ray_cr_metadata, dict)
        else None
    )
    if isinstance(cluster_annotations, dict) and key in cluster_annotations:
        val = cluster_annotations[key]
        return (val if isinstance(val, str) else ""), True
    return None, False


def _has_external_multihost_placement(
    ray_cr_metadata: Optional[Dict[str, Any]],
    group_spec: Dict[str, Any],
) -> bool:
    """Return True if an external scheduler/controller manages multi-host placement.

    Suppresses planned KubeRay multi-host topology domain inference when GKE
    slice-scheduler, Kueue Topology Aware Scheduling, or Volcano network
    topology controls pod placement.
    """
    template = (
        group_spec.get("template")
        if isinstance(group_spec.get("template"), dict)
        else {}
    )
    pod_spec = template.get("spec") if isinstance(template.get("spec"), dict) else {}
    pod_meta = (
        template.get("metadata") if isinstance(template.get("metadata"), dict) else {}
    )

    if pod_spec.get("schedulerName") == GKE_SLICE_SCHEDULER_NAME:
        return True

    scheduling_gates = pod_spec.get("schedulingGates")
    if isinstance(scheduling_gates, list):
        for gate in scheduling_gates:
            if (
                isinstance(gate, dict)
                and gate.get("name") == KUEUE_TOPOLOGY_SCHEDULING_GATE
            ):
                return True

    pod_node_selector = pod_spec.get("nodeSelector")
    pod_labels = pod_meta.get("labels")
    pod_annotations = pod_meta.get("annotations")
    group_labels = group_spec.get("labels")
    for mapping in (pod_node_selector, pod_labels, pod_annotations, group_labels):
        if not isinstance(mapping, dict):
            continue
        for k, v in mapping.items():
            if (
                isinstance(k, str)
                and k.startswith(GKE_TPU_SLICE_PREFIX)
                and isinstance(v, str)
                and v != ""
            ):
                return True

    cluster_labels = (
        ray_cr_metadata.get("labels") if isinstance(ray_cr_metadata, dict) else None
    )
    cluster_annotations = (
        ray_cr_metadata.get("annotations")
        if isinstance(ray_cr_metadata, dict)
        else None
    )
    for mapping in (
        cluster_labels,
        cluster_annotations,
        pod_labels,
        pod_annotations,
        group_labels,
    ):
        if not isinstance(mapping, dict):
            continue
        for key in EXTERNAL_PLACEMENT_NONEMPTY_KEYS:
            val = mapping.get(key)
            if isinstance(val, str) and val != "":
                return True
        unconstrained = mapping.get(KUEUE_PODSET_UNCONSTRAINED_TOPOLOGY_ANNOTATION)
        if isinstance(unconstrained, str) and unconstrained.lower() == "true":
            return True

    return False


def _generate_available_node_types_from_ray_cr_spec(
    ray_cr_spec: Dict[str, Any],
    ray_cr_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Formats autoscaler "available_node_types" field based on the Ray CR's group
    specs.
    """
    headGroupSpec = ray_cr_spec["headGroupSpec"]
    return {
        _HEAD_GROUP_NAME: _node_type_from_group_spec(
            headGroupSpec, is_head=True, ray_cr_metadata=ray_cr_metadata
        ),
        **{
            worker_group_spec["groupName"]: _node_type_from_group_spec(
                worker_group_spec, is_head=False, ray_cr_metadata=ray_cr_metadata
            )
            for worker_group_spec in ray_cr_spec["workerGroupSpecs"]
        },
    }


def _node_type_from_group_spec(
    group_spec: Dict[str, Any],
    is_head: bool,
    ray_cr_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Converts CR group spec to autoscaler node type."""
    group_name = _HEAD_GROUP_NAME if is_head else group_spec["groupName"]

    if is_head:
        # The head node type has no workers because the head is not a worker.
        min_workers = max_workers = 0
        num_of_hosts = 1
    else:
        # `minReplicas` and `maxReplicas` are required fields for each workerGroupSpec.
        # numOfHosts specifies the number of workers per replica in KubeRay v1.1+.
        raw_num_hosts = group_spec.get("numOfHosts")
        num_of_hosts = 1 if raw_num_hosts is None else max(0, int(raw_num_hosts))
        min_workers = group_spec["minReplicas"] * num_of_hosts
        max_workers = group_spec["maxReplicas"] * num_of_hosts

    resources = _get_ray_resources_from_group_spec(group_spec, is_head)
    labels = _get_labels_from_group_spec(group_spec, group_name, resources)

    node_type = {
        "min_workers": min_workers,
        "max_workers": max_workers,
        # `node_config` is a legacy field required for compatibility.
        # Pod config data is required by the operator but not by the autoscaler.
        "node_config": {},
        "resources": resources,
        "labels": labels,
    }

    if not is_head:
        node_selector, pod_annotations = _get_pod_scheduling_metadata(group_spec)
        (
            delivered_label_ref_keys,
            node_label_to_ray_key,
        ) = _parse_worker_group_label_refs(group_spec)
        replica_meta: Dict[str, Any] = {}
        if num_of_hosts > 0:
            for accelerator_manager in get_all_accelerator_managers():
                meta = accelerator_manager.get_node_group_replica_metadata(
                    resources, node_selector, pod_annotations
                )
                if meta:
                    replica_meta = dict(meta)
                    if "head_resources" in replica_meta:
                        head_resources = replica_meta["head_resources"]
                        if head_resources is None:
                            replica_meta.pop("head_resources")
                        else:
                            replica_meta["head_resources"] = list(head_resources)
                    break

            if (
                num_of_hosts > 1
                and not replica_meta.get("domain_label_key")
                and not _has_external_multihost_placement(ray_cr_metadata, group_spec)
            ):
                topo_key, topo_found = _lookup_multihost_annotation(
                    ray_cr_metadata,
                    pod_annotations,
                    RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION,
                )
                if topo_found and topo_key and topo_key in node_label_to_ray_key:
                    delivered_ray_key = node_label_to_ray_key[topo_key]
                    excl_raw, excl_found = _lookup_multihost_annotation(
                        ray_cr_metadata,
                        pod_annotations,
                        RAY_MULTIHOST_TOPOLOGY_EXCLUSIVE_ANNOTATION,
                    )
                    if excl_found:
                        domain_exclusive = (
                            isinstance(excl_raw, str) and excl_raw.lower() == "true"
                        )
                    else:
                        domain_exclusive = is_exclusive_tpu_slice_topology_key(topo_key)
                    replica_meta["domain_label_key"] = delivered_ray_key
                    replica_meta["domain_exclusive"] = domain_exclusive

            if replica_meta or num_of_hosts > 1:
                node_type["replica_config"] = {
                    **replica_meta,
                    "workers_per_replica": num_of_hosts,
                }

        runtime_label_keys: List[str] = []
        seen_runtime_keys: Set[str] = set()
        for key in chain(
            delivered_label_ref_keys,
            (
                replica_meta.get("domain_label_key"),
                replica_meta.get("worker_index_label_key"),
            ),
        ):
            if (
                isinstance(key, str)
                and key
                and key not in labels
                and key not in seen_runtime_keys
            ):
                seen_runtime_keys.add(key)
                runtime_label_keys.append(key)
        if runtime_label_keys:
            node_type["runtime_label_keys"] = runtime_label_keys

    idle_timeout_s = group_spec.get(IDLE_SECONDS_KEY)
    if idle_timeout_s is not None:
        node_type["idle_timeout_s"] = float(idle_timeout_s)

    priority = group_spec.get(PRIORITY_KEY)
    if priority is not None:
        node_type[PRIORITY_KEY] = int(priority)

    return node_type


def _get_required_node_labels(pod_spec: Dict[str, Any]) -> Dict[str, str]:
    """Return node labels guaranteed by pod `nodeSelector` and required `nodeAffinity`.

    A label key is guaranteed when set in `nodeSelector` or when every
    non-empty `nodeSelectorTerm` pins it to the same value via `In`
    expressions (Kubernetes ANDs expressions within a term and ORs terms).
    """
    raw_selector = pod_spec.get("nodeSelector")
    node_selector = (
        {
            k: v
            for k, v in raw_selector.items()
            if isinstance(k, str) and isinstance(v, str)
        }
        if isinstance(raw_selector, dict)
        else {}
    )
    affinity = pod_spec.get("affinity")
    node_affinity = affinity.get("nodeAffinity") if isinstance(affinity, dict) else None
    if not isinstance(node_affinity, dict):
        return node_selector
    required = node_affinity.get("requiredDuringSchedulingIgnoredDuringExecution")
    if not isinstance(required, dict):
        return node_selector
    terms = required.get("nodeSelectorTerms")
    if not isinstance(terms, list) or not terms:
        return {}

    common_pins: Optional[Dict[str, str]] = None
    for term in terms:
        if not isinstance(term, dict):
            continue
        raw_me = term.get("matchExpressions")
        raw_mf = term.get("matchFields")
        match_expressions = [] if raw_me is None else raw_me
        match_fields = [] if raw_mf is None else raw_mf
        if (
            not isinstance(match_expressions, list)
            or not isinstance(match_fields, list)
            or (not match_expressions and not match_fields)
        ):
            continue

        allowed: Dict[str, Set[str]] = {k: {v} for k, v in node_selector.items()}
        term_contradiction = False
        for expr in match_expressions:
            if not isinstance(expr, dict) or expr.get("operator") != "In":
                continue
            key = expr.get("key")
            vals = expr.get("values")
            if (
                not isinstance(key, str)
                or not key
                or not isinstance(vals, list)
                or not all(isinstance(v, str) for v in vals)
            ):
                term_contradiction = True
                break
            val_set = set(vals)
            allowed[key] = val_set if key not in allowed else (allowed[key] & val_set)
            if not allowed[key]:
                term_contradiction = True
                break
        if term_contradiction:
            continue

        term_pins = {
            key: next(iter(vals)) for key, vals in allowed.items() if len(vals) == 1
        }
        if common_pins is None:
            common_pins = term_pins
        else:
            common_pins = {
                k: v for k, v in common_pins.items() if term_pins.get(k) == v
            }

    return {} if common_pins is None else common_pins


def _get_pod_scheduling_metadata(
    group_spec: Dict[str, Any],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Returns required pod node labels and pod annotations."""
    template = group_spec.get("template") or {}
    pod_spec = template.get("spec") or {}
    node_selector = _get_required_node_labels(pod_spec)
    annotations = (template.get("metadata") or {}).get("annotations") or {}
    return node_selector, annotations


def _get_ray_resources_from_group_spec(
    group_spec: Dict[str, Any], is_head: bool
) -> Dict[str, int]:
    """Infers Ray resources from group `Resources` field, rayStartCommands, or K8s limits.
    The resources extracted are used in autoscaling calculations.
    """
    # Set resources from top-level group 'Resources' field if it exists.
    group_resources = group_spec.get("resources", {})

    ray_start_params = group_spec.get("rayStartParams", {})
    # In KubeRay, Ray container is always the first application container of a Ray Pod.
    k8s_resources = group_spec["template"]["spec"]["containers"][0].get("resources", {})
    group_name = _HEAD_GROUP_NAME if is_head else group_spec["groupName"]

    num_cpus = _get_num_cpus(
        group_resources, ray_start_params, k8s_resources, group_name
    )
    num_gpus = _get_num_gpus(
        group_resources, ray_start_params, k8s_resources, group_name
    )
    custom_resource_dict = _get_custom_resources(
        group_resources, ray_start_params, group_name
    )
    num_tpus = _get_num_tpus(group_resources, custom_resource_dict, k8s_resources)
    memory = _get_memory(group_resources, ray_start_params, k8s_resources)

    # It's not allowed to use object store memory as a resource request, so we don't
    # add that to the autoscaler's resources annotations.

    resources = {}

    assert isinstance(num_cpus, int)
    resources["CPU"] = num_cpus

    if num_gpus is not None:
        resources["GPU"] = num_gpus

    if num_tpus is not None and "TPU" not in custom_resource_dict:
        # Add TPU Ray resource if not already added by ray_start_params,
        # but specified in k8s_resource_limits.
        resources["TPU"] = num_tpus

    if memory is not None:
        resources["memory"] = memory

    resources.update(custom_resource_dict)

    node_selector, annotations = _get_pod_scheduling_metadata(group_spec)
    for accelerator_manager in get_all_accelerator_managers():
        additional = accelerator_manager.get_node_group_additional_resources(
            resources, node_selector, annotations
        )
        if additional:
            for key, val in additional.items():
                resources.setdefault(key, val)

    return resources


def _get_labels_from_group_spec(
    group_spec: Dict[str, Any],
    group_name: str = "",
    resources: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """Parses Ray node labels for the autoscaling config based on the following
    priority:
    1. Top-level `labels` field in the group spec.
    2. Accelerator labels derived from the Pod `nodeSelector`, `annotations`,
       and required `nodeAffinity` terms.
    3. `labels` field in `rayStartParams` (ignored with a warning).

    Args:
        group_spec: The group specification dictionary.
        group_name: The name of the group (used in warning messages).
        resources: Base Ray resources already parsed for the group.

    Returns:
        A dictionary of labels for the node type.
    """
    labels_dict = {}

    ray_start_params = group_spec.get("rayStartParams", {})
    labels_str = ray_start_params.get("labels")
    # Use a unique log_once key per group to ensure each group's warning is shown.
    log_once_key = (
        f"raystartparams_labels_warning_{group_name}"
        if group_name
        else "raystartparams_labels_warning"
    )
    if labels_str and log_once(log_once_key):
        if group_name:
            logger.warning(
                f"Ignoring labels: {labels_str} set in rayStartParams for group "
                f"'{group_name}'. Group labels are supported in the top-level "
                "Labels field starting in KubeRay v1.5"
            )
        else:
            logger.warning(
                f"Ignoring labels: {labels_str} set in rayStartParams. "
                "Group labels are supported in the top-level Labels field "
                "starting in KubeRay v1.5"
            )

    # Check for top-level structured Labels field.
    raw_labels = group_spec.get("labels")
    if isinstance(raw_labels, dict):
        labels_dict = raw_labels
        # Validate node labels follow expected Kubernetes label syntax.
        validate_node_label_syntax(labels_dict)

    node_selector, annotations = _get_pod_scheduling_metadata(group_spec)
    resolved_resources = resources or {}
    for accelerator_manager in get_all_accelerator_managers():
        accel_labels = accelerator_manager.get_node_group_accelerator_labels(
            resolved_resources, node_selector, annotations
        )
        if accel_labels:
            # Explicit group labels take precedence over derived ones.
            labels_dict = {**accel_labels, **labels_dict}

    return labels_dict


def _get_num_cpus(
    group_resources: Dict[str, str],
    ray_start_params: Dict[str, str],
    k8s_resources: Dict[str, Dict[str, str]],
    group_name: str,
) -> int:
    """Get CPU annotation from `resources` field, ray_start_params or k8s_resources,
    with priority for `resources` field.
    """
    if "CPU" in group_resources:
        if "num-cpus" in ray_start_params:
            logger.warning(
                f"'CPU' specified in both the top-level 'resources' field and in 'rayStartParams'. "
                f"Using the value from 'resources': {group_resources['CPU']}."
            )
        return _round_up_k8s_quantity(group_resources["CPU"])
    if "num-cpus" in ray_start_params:
        return int(ray_start_params["num-cpus"])
    elif "cpu" in k8s_resources.get("limits", {}):
        cpu_quantity: str = k8s_resources["limits"]["cpu"]
        return _round_up_k8s_quantity(cpu_quantity)
    elif "cpu" in k8s_resources.get("requests", {}):
        cpu_quantity: str = k8s_resources["requests"]["cpu"]
        return _round_up_k8s_quantity(cpu_quantity)
    else:
        # Getting the number of CPUs is important, so raise an error if we can't do it.
        raise ValueError(
            f"Autoscaler failed to detect `CPU` resources for group {group_name}."
            "\nSet the `--num-cpus` rayStartParam and/or "
            "the CPU resource limit for the Ray container."
        )


def _get_memory(
    group_resources: Dict[str, str],
    ray_start_params: Dict[str, str],
    k8s_resources: Dict[str, Dict[str, str]],
) -> Optional[int]:
    """Get memory resource annotation from `resources` field, ray_start_params or k8s_resources,
    with priority for `resources` field.
    """
    if "memory" in group_resources:
        if "memory" in ray_start_params:
            logger.warning(
                f"'memory' specified in both the top-level 'resources' field and in 'rayStartParams'. "
                f"Using the value from 'resources': {group_resources['memory']}."
            )
        return _round_up_k8s_quantity(group_resources["memory"])
    if "memory" in ray_start_params:
        return int(ray_start_params["memory"])
    elif "memory" in k8s_resources.get("limits", {}):
        memory_quantity: str = k8s_resources["limits"]["memory"]
        return _round_up_k8s_quantity(memory_quantity)
    elif "memory" in k8s_resources.get("requests", {}):
        memory_quantity: str = k8s_resources["requests"]["memory"]
        return _round_up_k8s_quantity(memory_quantity)
    return None


def _get_num_gpus(
    group_resources: Dict[str, str],
    ray_start_params: Dict[str, str],
    k8s_resources: Dict[str, Dict[str, str]],
    group_name: str,
) -> Optional[int]:
    """Get GPU resource annotation from `resources` field, ray_start_params or k8s_resources,
    with priority for `resources` field.
    """
    if "GPU" in group_resources:
        if "num-gpus" in ray_start_params:
            logger.warning(
                f"'GPU' specified in both the top-level 'resources' field and in 'rayStartParams'. "
                f"Using the value from 'resources': {group_resources['GPU']}."
            )
        return _round_up_k8s_quantity(group_resources["GPU"])
    elif "num-gpus" in ray_start_params:
        return int(ray_start_params["num-gpus"])
    else:
        for key, resource_quantity in chain(
            k8s_resources.get("limits", {}).items(),
            k8s_resources.get("requests", {}).items(),
        ):
            # e.g. nvidia.com/gpu
            if key.endswith("gpu"):
                # Typically, this is a string representing an interger, e.g. "1".
                # Convert to int, making no assumptions on the resource_quantity,
                # besides that it's valid as a K8s resource quantity.
                num_gpus = _round_up_k8s_quantity(resource_quantity)
                if num_gpus > 0:
                    # Only one GPU type supported for now, break out on first
                    # "/gpu" match.
                    return num_gpus
    return None


def _get_num_tpus(
    group_resources: Dict[str, str],
    custom_resource_dict: Dict[str, int],
    k8s_resources: Dict[str, Dict[str, str]],
) -> Optional[int]:
    """Get TPU custom resource annotation from `resources` field, custom_resource_dict in ray_start_params,
    or k8s_resources, with priority for `resources` field.
    """
    if "TPU" in group_resources:
        return _round_up_k8s_quantity(group_resources["TPU"])
    elif "TPU" in custom_resource_dict:
        return custom_resource_dict["TPU"]
    else:
        for typ in ["limits", "requests"]:
            tpu_resource_quantity = k8s_resources.get(typ, {}).get("google.com/tpu")
            if tpu_resource_quantity is not None:
                # Typically, this is a string representing an integer, e.g. "1".
                # Convert to int, making no assumptions on the tpu_resource_quantity,
                # besides that it's valid as a K8s resource quantity.
                num_tpus = _round_up_k8s_quantity(tpu_resource_quantity)
                if num_tpus > 0:
                    return num_tpus
    return None


def _round_up_k8s_quantity(quantity: str) -> int:
    """Rounds a Kubernetes resource quantity up to the nearest integer.

    Args:
        quantity: Resource quantity as a string in the canonical K8s form.

    Returns:
        The quantity, rounded up, as an integer.
    """
    resource_decimal: decimal.Decimal = utils.parse_quantity(quantity)
    rounded = resource_decimal.to_integral_value(rounding=decimal.ROUND_UP)
    return int(rounded)


def _get_custom_resources(
    group_resources: Dict[str, str], ray_start_params: Dict[str, Any], group_name: str
) -> Dict[str, int]:
    """Format custom resources based on the group `resources` field or `resources` Ray start param.

    Currently, the value of the rayStartParam `resources` field must
    be formatted as follows:
    '"{\"Custom1\": 1, \"Custom2\": 5}"'.

    This method first converts the input to a correctly formatted
    json string and then loads that json string to a dict.
    """
    # If the top-level `resources` field is defined, use it as the exclusive source.
    if group_resources:
        if "resources" in ray_start_params:
            logger.warning(
                f"custom resources specified in both the top-level 'resources' field and in 'rayStartParams'. "
                f"Using the values from 'resources': {group_resources}."
            )
        standard_keys = {"CPU", "GPU", "TPU", "memory"}
        try:
            custom_resources = {
                k: _round_up_k8s_quantity(v)
                for k, v in group_resources.items()
                if k not in standard_keys
            }
        except Exception as e:
            logger.error(
                f"Error reading `resource` for group {group_name}."
                " For the correct format, refer to example configuration at "
                "https://github.com/ray-project/ray/blob/master/python/"
                "ray/autoscaler/kuberay/ray-cluster.complete.yaml."
            )
            raise e
        return custom_resources

    # Otherwise, check rayStartParams.
    if "resources" not in ray_start_params:
        return {}
    resources_string = ray_start_params["resources"]
    try:
        # Drop the extra pair of quotes and remove the backslash escapes.
        # resources_json should be a json string.
        resources_json = resources_string[1:-1].replace("\\", "")
        # Load a dict from the json string.
        resources = json.loads(resources_json)
        assert isinstance(resources, dict)
        for key, value in resources.items():
            assert isinstance(key, str)
            assert isinstance(value, int)
    except Exception as e:
        logger.error(
            f"Error reading `resource` rayStartParam for group {group_name}."
            " For the correct format, refer to example configuration at "
            "https://github.com/ray-project/ray/blob/master/python/"
            "ray/autoscaler/kuberay/ray-cluster.complete.yaml."
        )
        raise e
    return resources
