"""Ray autoscaler node provider for Oracle Cloud Infrastructure (OCI).

Cluster nodes are OCI Compute instances in a single compartment. Ray's node
tags are stored as OCI free-form tags on each instance and are used to find
the cluster's nodes (``list_instances`` in the compartment, filtered client
side, which is strongly consistent unlike Resource Search). IP addresses come
from the primary VNIC of each instance.
"""

import copy
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from threading import RLock
from typing import Any, Dict, List, Optional, Set

from ray.autoscaler._private._oci.config import bootstrap_oci, fillout_resources
from ray.autoscaler._private._oci.utils import (
    RUNNING,
    STOPPED,
    STOPPED_STATES,
    TERMINATED_STATES,
    OCIClient,
    is_not_found_or_not_authorized,
    is_out_of_capacity,
    is_service_error,
    short_id,
    validate_freeform_tags,
)
from ray.autoscaler._private.cli_logger import cf, cli_logger
from ray.autoscaler._private.constants import MAX_PARALLEL_SHUTDOWN_WORKERS
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

# Keys in ``node_config`` that are consumed by the provider or the bootstrap
# step rather than passed to ``LaunchInstanceDetails``.
_PROVIDER_ONLY_KEYS = (
    "image_operating_system",
    "image_operating_system_version",
)

# Tags a stopped instance must share with a launch request to be reused.
_REUSE_TAGS = (
    TAG_RAY_CLUSTER_NAME,
    TAG_RAY_NODE_KIND,
    TAG_RAY_USER_NODE_TYPE,
    TAG_RAY_LAUNCH_CONFIG,
)

# OCI answers 409 Conflict ("instance is currently being modified") while an
# instance is provisioning or changing state; the SDK's default retry strategy
# does not retry it, so the provider does, with backoff, for up to this long.
CONFLICT_RETRY_TIMEOUT_S = 300
CONFLICT_RETRY_INITIAL_DELAY_S = 2
CONFLICT_RETRY_MAX_DELAY_S = 15


def synchronized(f):
    def wrapper(self, *args, **kwargs):
        with self.lock:
            return f(self, *args, **kwargs)

    return wrapper


class OCINodeProvider(NodeProvider):
    """Node provider for OCI Compute instances.

    Authentication is resolved by :class:`OCIClient`: an OCI config-file
    profile (API key or session token) when one is available, otherwise
    instance principals, which is how the autoscaler running on the head node
    authenticates by default.
    """

    def __init__(self, provider_config: Dict[str, Any], cluster_name: str):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.client = OCIClient(provider_config)
        self.compartment_id = provider_config["compartment_id"]
        logger.info(
            "OCINodeProvider: authenticated with OCI using %s (region %s, "
            "compartment ...%s)",
            self.client.auth_mode,
            self.client.region,
            short_id(self.compartment_id),
        )
        # Stopped instances are only reused when explicitly requested: OCI
        # keeps billing GPU and dense I/O shapes while they are stopped.
        self.cache_stopped_nodes = provider_config.get("cache_stopped_nodes", False)
        self.lock = RLock()
        # Instance objects from the most recent listing, keyed by OCID.
        self.cached_nodes: Dict[str, Any] = {}
        # node id -> {"internal": ip, "external": ip}
        self.ip_cache: Dict[str, Dict[str, Optional[str]]] = {}
        # Stopped instances claimed by an in-flight create_node() so that
        # concurrent callers never restart the same instance.
        self._claimed_for_reuse: Set[str] = set()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _retry_on_conflict(fn, *args, **kwargs):
        """Call ``fn`` retrying 409 Conflict responses with backoff."""
        deadline = time.time() + CONFLICT_RETRY_TIMEOUT_S
        delay = CONFLICT_RETRY_INITIAL_DELAY_S
        while True:
            try:
                return fn(*args, **kwargs)
            except Exception as e:  # noqa: BLE001
                if not is_service_error(e, 409) or time.time() >= deadline:
                    raise
                logger.info(
                    "OCINodeProvider: %s returned 409 Conflict, retrying in %ss",
                    getattr(fn, "__name__", fn),
                    delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, CONFLICT_RETRY_MAX_DELAY_S)

    # ------------------------------------------------------------------
    # Listing and state
    # ------------------------------------------------------------------
    def _list_instances(self) -> List[Any]:
        # Compartment-wide on purpose: node types may override
        # `availability_domain`, so nodes of one cluster can live in several
        # ADs. The Ray tags, not the AD, identify the cluster's nodes.
        return self.client.list_all(
            self.client.compute.list_instances, self.compartment_id
        )

    @staticmethod
    def _matches(instance, tag_filters: Dict[str, str]) -> bool:
        tags = instance.freeform_tags or {}
        return all(tags.get(k) == v for k, v in tag_filters.items())

    @synchronized
    def _get_filtered_nodes(
        self, tag_filters: Dict[str, str], include_stopped: bool = False
    ) -> Dict[str, Any]:
        filters = {**tag_filters, TAG_RAY_CLUSTER_NAME: self.cluster_name}
        instances = self._list_instances()
        self.cached_nodes = {
            inst.id: inst
            for inst in instances
            if inst.lifecycle_state not in TERMINATED_STATES
            and self._matches(inst, {TAG_RAY_CLUSTER_NAME: self.cluster_name})
        }
        result = {}
        for node_id, inst in self.cached_nodes.items():
            if not self._matches(inst, filters):
                continue
            if inst.lifecycle_state in STOPPED_STATES and not include_stopped:
                # A stopped node is "terminated" from Ray's point of view.
                continue
            result[node_id] = inst
        return result

    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        return list(self._get_filtered_nodes(tag_filters).keys())

    def _get_node(self, node_id: str):
        """Fetch a fresh instance object, updating the cache."""
        try:
            instance = self.client.compute.get_instance(node_id).data
        except Exception as e:  # noqa: BLE001
            if is_not_found_or_not_authorized(e):
                with self.lock:
                    self.cached_nodes.pop(node_id, None)
                return None
            raise
        with self.lock:
            if instance.lifecycle_state in TERMINATED_STATES:
                self.cached_nodes.pop(node_id, None)
            else:
                self.cached_nodes[node_id] = instance
        return instance

    def _get_cached_node(self, node_id: str):
        with self.lock:
            instance = self.cached_nodes.get(node_id)
        if instance is not None:
            return instance
        return self._get_node(node_id)

    def is_running(self, node_id: str) -> bool:
        instance = self._get_node(node_id)
        return instance is not None and instance.lifecycle_state == RUNNING

    def is_terminated(self, node_id: str) -> bool:
        instance = self._get_node(node_id)
        if instance is None or instance.lifecycle_state in TERMINATED_STATES:
            return True
        return instance.lifecycle_state in STOPPED_STATES

    def node_tags(self, node_id: str) -> Dict[str, str]:
        instance = self._get_cached_node(node_id)
        if instance is None:
            return {}
        return dict(instance.freeform_tags or {})

    # ------------------------------------------------------------------
    # IP addresses
    # ------------------------------------------------------------------
    def _lookup_ips(self, node_id: str) -> Dict[str, Optional[str]]:
        """Return the primary VNIC's IPs, or Nones while it is still attaching."""
        instance = self._get_cached_node(node_id)
        if instance is None:
            return {"internal": None, "external": None}
        attachments = self.client.list_all(
            self.client.compute.list_vnic_attachments,
            self.compartment_id,
            instance_id=node_id,
        )
        for attachment in attachments:
            if attachment.lifecycle_state != "ATTACHED" or not attachment.vnic_id:
                continue
            vnic = self.client.network.get_vnic(attachment.vnic_id).data
            if not vnic.is_primary and len(attachments) > 1:
                continue
            return {"internal": vnic.private_ip, "external": vnic.public_ip}
        return {"internal": None, "external": None}

    def _get_ip(self, node_id: str, kind: str) -> Optional[str]:
        with self.lock:
            cached = self.ip_cache.get(node_id, {})
        if cached.get(kind):
            return cached[kind]
        ips = self._lookup_ips(node_id)
        if ips.get("internal"):
            with self.lock:
                self.ip_cache[node_id] = ips
        return ips.get(kind)

    def external_ip(self, node_id: str) -> Optional[str]:
        return self._get_ip(node_id, "external")

    def internal_ip(self, node_id: str) -> Optional[str]:
        return self._get_ip(node_id, "internal")

    # ------------------------------------------------------------------
    # Tags
    # ------------------------------------------------------------------
    @synchronized
    def set_node_tags(self, node_id: str, tags: Dict[str, str]) -> None:
        instance = self._get_node(node_id)
        if instance is None:
            logger.warning(
                "OCINodeProvider: cannot tag ...%s, instance not found",
                short_id(node_id),
            )
            return
        merged = validate_freeform_tags({**(instance.freeform_tags or {}), **tags})
        if merged == (instance.freeform_tags or {}):
            return
        with LogTimer(
            "OCINodeProvider: Set %d tag(s) on ...%s" % (len(tags), short_id(node_id))
        ):
            details = self.client.models().UpdateInstanceDetails(freeform_tags=merged)
            updated = self._retry_on_conflict(
                self.client.compute.update_instance, node_id, details
            ).data
        self.cached_nodes[node_id] = updated

    # ------------------------------------------------------------------
    # Creation
    # ------------------------------------------------------------------
    def _build_launch_details(self, node_config: Dict[str, Any], tags: Dict[str, str]):
        models = self.client.models()
        conf = copy.deepcopy(node_config)
        for key in _PROVIDER_ONLY_KEYS:
            conf.pop(key, None)

        conf.setdefault("compartment_id", self.compartment_id)
        conf.setdefault(
            "availability_domain", self.provider_config.get("availability_domain")
        )
        conf.setdefault(
            "display_name",
            tags.get(TAG_RAY_NODE_NAME)
            or f"ray-{self.cluster_name}-{tags.get(TAG_RAY_NODE_KIND, 'node')}",
        )

        # Boot volume / image.
        source = conf.pop("source_details", None)
        image_id = conf.pop("image_id", None)
        boot_size = conf.pop("boot_volume_size_in_gbs", None)
        boot_vpus = conf.pop("boot_volume_vpus_per_gb", None)
        if source is None:
            source = {"image_id": image_id}
            if boot_size is not None:
                source["boot_volume_size_in_gbs"] = boot_size
            if boot_vpus is not None:
                source["boot_volume_vpus_per_gb"] = boot_vpus
        if isinstance(source, dict):
            source = dict(source)
            source_type = source.pop("source_type", "image")
            if source_type != "image":
                raise ValueError(
                    "Only `source_type: image` is supported in "
                    "`node_config.source_details`."
                )
            if not source.get("image_id"):
                raise ValueError(
                    "`node_config.image_id` is not set; run `ray up` so it can "
                    "be resolved, or set it explicitly."
                )
            source = models.InstanceSourceViaImageDetails(**source)
        conf["source_details"] = source

        # Networking.
        vnic = dict(conf.pop("create_vnic_details", None) or {})
        subnet_id = conf.pop("subnet_id", None) or self.provider_config.get("subnet_id")
        vnic.setdefault("subnet_id", subnet_id)
        vnic.setdefault(
            "assign_public_ip", not self.provider_config.get("use_internal_ips", False)
        )
        if not vnic["subnet_id"]:
            raise ValueError(
                "No subnet configured. Set `provider.subnet_id` or "
                "`node_config.subnet_id`, or let `ray up` create the default VCN."
            )
        conf["create_vnic_details"] = models.CreateVnicDetails(**vnic)

        # Nested structures with dedicated SDK models.
        nested = {
            "shape_config": models.LaunchInstanceShapeConfigDetails,
            "agent_config": models.LaunchInstanceAgentConfigDetails,
            "launch_options": models.LaunchOptions,
            "availability_config": models.LaunchInstanceAvailabilityConfigDetails,
            "instance_options": models.InstanceOptions,
        }
        for key, model_cls in nested.items():
            value = conf.get(key)
            if isinstance(value, dict):
                conf[key] = model_cls(**value)
        preemptible = conf.get("preemptible_instance_config")
        if isinstance(preemptible, dict):
            action = dict(preemptible.get("preemption_action") or {})
            action.pop("type", None)
            action.setdefault("preserve_boot_volume", False)
            conf[
                "preemptible_instance_config"
            ] = models.PreemptibleInstanceConfigDetails(
                preemption_action=models.TerminatePreemptionAction(**action)
            )

        # Tags: Ray's tags take precedence over user tags.
        conf["freeform_tags"] = validate_freeform_tags(
            {**(conf.get("freeform_tags") or {}), **tags}
        )
        try:
            return models.LaunchInstanceDetails(**conf)
        except TypeError as e:
            raise ValueError(
                f"Invalid key in `node_config`: {e}. Keys must be "
                "`LaunchInstanceDetails` fields in snake_case (for example "
                "`shape`, `shape_config`, `image_id`, `metadata`, `fault_domain`)."
            ) from e

    @synchronized
    def _claim_stopped_nodes(self, tags: Dict[str, str], count: int) -> List[Any]:
        """Pick up to ``count`` STOPPED instances matching ``tags`` and mark
        them as claimed, atomically, so concurrent callers get disjoint sets.

        Instances that are still STOPPING are skipped rather than waited for:
        the autoscaler must not block, and they become eligible once stopped.
        """
        filters = {k: tags[k] for k in _REUSE_TAGS if k in tags}
        claimed = []
        for inst in self._get_filtered_nodes(filters, include_stopped=True).values():
            if len(claimed) >= count:
                break
            if inst.lifecycle_state != STOPPED or inst.id in self._claimed_for_reuse:
                continue
            self._claimed_for_reuse.add(inst.id)
            claimed.append(inst)
        return claimed

    def _reuse_stopped_nodes(self, tags: Dict[str, str], count: int) -> Dict[str, Any]:
        claimed = self._claim_stopped_nodes(tags, count)
        reused = {}
        try:
            for inst in claimed:
                cli_logger.print("Restarting stopped instance ...{}", short_id(inst.id))
                # The START call runs outside the lock: it is a network call.
                started = self._retry_on_conflict(
                    self.client.compute.instance_action, inst.id, "START"
                ).data
                with self.lock:
                    self.cached_nodes[inst.id] = started
                    # The ephemeral public IP may change across stop/start.
                    self.ip_cache.pop(inst.id, None)
                self.set_node_tags(inst.id, tags)
                reused[inst.id] = started
        finally:
            # Release the whole batch, including instances never reached
            # because START or the tag update raised for an earlier one;
            # otherwise they would stay reserved for the life of the process.
            with self.lock:
                self._claimed_for_reuse.difference_update(inst.id for inst in claimed)
        return reused

    def create_node(
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Dict[str, Any]:
        tags = {**tags, TAG_RAY_CLUSTER_NAME: self.cluster_name}
        created: Dict[str, Any] = {}

        if self.cache_stopped_nodes:
            created.update(self._reuse_stopped_nodes(tags, count))
            count -= len(created)
        if count <= 0:
            return created

        details = self._build_launch_details(node_config, tags)
        with cli_logger.group(
            "Launching {} OCI instance(s) of shape {}",
            count,
            cf.bold(details.shape),
        ):
            for _ in range(count):
                try:
                    instance = self.client.compute.launch_instance(details).data
                except Exception as e:  # noqa: BLE001
                    self._raise_launch_exception(e, details)
                with self.lock:
                    self.cached_nodes[instance.id] = instance
                created[instance.id] = instance
                cli_logger.print(
                    "Launched instance ...{} ({})",
                    short_id(instance.id),
                    instance.display_name,
                    _tags=dict(state=instance.lifecycle_state),
                )
        return created

    @staticmethod
    def _raise_launch_exception(exc: Exception, details) -> None:
        if is_out_of_capacity(exc):
            category, description = (
                "OutOfCapacity",
                f"OCI has no capacity for shape {details.shape} in "
                f"{details.availability_domain}. Try another availability "
                "domain or shape.",
            )
        elif is_service_error(exc, 400, code="LimitExceeded") or is_service_error(
            exc, 429
        ):
            category, description = (
                "LimitExceeded",
                f"Service limit or quota exceeded for shape {details.shape}: "
                f"{getattr(exc, 'message', exc)}",
            )
        elif is_service_error(exc, 400, 404):
            category, description = (
                "InvalidConfig",
                f"OCI rejected the launch request: {getattr(exc, 'message', exc)}",
            )
        else:
            category, description = ("LaunchFailed", str(exc))
        raise NodeLaunchException(category, description, sys.exc_info()) from exc

    # ------------------------------------------------------------------
    # Termination
    # ------------------------------------------------------------------
    def terminate_node(self, node_id: str) -> None:
        instance = self._get_node(node_id)
        if instance is None or instance.lifecycle_state in TERMINATED_STATES:
            return
        try:
            if self.cache_stopped_nodes:
                if instance.lifecycle_state not in STOPPED_STATES:
                    cli_logger.print(
                        "Stopping instance ...{} (to terminate instead, set "
                        "`cache_stopped_nodes: False` under `provider` in the "
                        "cluster configuration)",
                        short_id(node_id),
                    )
                    self._retry_on_conflict(
                        self.client.compute.instance_action, node_id, "STOP"
                    )
            else:
                cli_logger.print("Terminating instance ...{}", short_id(node_id))
                self._retry_on_conflict(
                    self.client.compute.terminate_instance,
                    node_id,
                    preserve_boot_volume=False,
                )
        except Exception as e:  # noqa: BLE001
            if is_not_found_or_not_authorized(e):
                logger.info("Instance ...%s already gone", short_id(node_id))
            else:
                raise
        with self.lock:
            self.cached_nodes.pop(node_id, None)
            self.ip_cache.pop(node_id, None)

    def terminate_nodes(self, node_ids: List[str]) -> None:
        if not node_ids:
            return
        with ThreadPoolExecutor(
            max_workers=min(len(node_ids), MAX_PARALLEL_SHUTDOWN_WORKERS)
        ) as executor:
            list(executor.map(self.terminate_node, node_ids))

    # ------------------------------------------------------------------
    # Config hooks
    # ------------------------------------------------------------------
    @staticmethod
    def bootstrap_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        return bootstrap_oci(cluster_config)

    @staticmethod
    def fillout_available_node_types_resources(
        cluster_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        return fillout_resources(cluster_config)
