import copy
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

from ray._raylet import RAY_INTERNAL_NAMESPACE_PREFIX, GcsClient

# TODO(rickyx): We should eventually remove these imports
# when we deprecate the v1 kuberay node provider.
from ray.autoscaler._private.kuberay.node_provider import (
    KUBERAY_KIND_HEAD,
    KUBERAY_KIND_WORKER,
    KUBERAY_LABEL_KEY_KIND,
    KUBERAY_LABEL_KEY_TYPE,
    RAY_HEAD_POD_NAME,
    IKubernetesHttpApiClient,
    KubernetesHttpApiClient,
    _worker_group_index,
    _worker_group_max_replicas,
    _worker_group_min_replicas,
    _worker_group_num_of_hosts,
    _worker_group_replicas,
    finalizer_patch,
    idle_suspend_patch,
    worker_delete_patch,
    worker_replica_patch,
)
from ray.autoscaler.v2.instance_manager.cloud_providers.kuberay.ippr_provider import (
    KubeRayIPPRProvider,
)
from ray.autoscaler.v2.instance_manager.node_provider import (
    CloudInstance,
    CloudInstanceId,
    CloudInstanceProviderError,
    ICloudInstanceProvider,
    LaunchNodeError,
    NodeKind,
    ReconcileStepContext,
    TerminateNodeError,
)
from ray.autoscaler.v2.schema import IPPRSpecs, IPPRStatus, NodeType

logger = logging.getLogger(__name__)

IDLE_TERMINATION_OPTIONS_KEY = "idleTerminationOptions"
IDLE_TERMINATION_OPTIONS_TIMEOUT_SECONDS_KEY = "timeoutSeconds"
IDLE_TERMINATION_OPTIONS_POLICY_KEY = "policy"
IDLE_TERMINATION_OPTIONS_POLICY_DELETE = "Delete"
IDLE_TERMINATION_OPTIONS_POLICY_SUSPEND = "Suspend"
IDLE_TERMINATION_CLEANUP_FINALIZER = "ray.io/idle-termination-cleanup-finalizer"
IDLE_SUSPEND_KEY = "idleSuspend"
KUBERAY_LABEL_KEY_REPLICA_NAME = "ray.io/worker-group-replica-name"


class KubeRayProvider(ICloudInstanceProvider):
    """This class is a thin wrapper around the Kubernetes API client. It modifies
    the RayCluster resource spec on the Kubernetes API server to scale the cluster:

    It launches new instances/nodes by submitting patches to the Kubernetes API
    to update the RayCluster CRD.
    """

    def __init__(
        self,
        cluster_name: str,
        provider_config: Dict[str, Any],
        gcs_client: GcsClient,
        k8s_api_client: Optional[IKubernetesHttpApiClient] = None,
    ):
        """Initializes a new KubeRayProvider.

        Args:
            cluster_name: The name of the RayCluster resource.
            provider_config: The configuration for the RayCluster.
            gcs_client: The client to the GCS server. Will be used for resizing raylets.
            k8s_api_client: The client to the Kubernetes
                API server. This can be used to mock the Kubernetes API server for testing.
        """
        self._cluster_name = cluster_name
        self._namespace = provider_config["namespace"]

        self._k8s_api_client = k8s_api_client or KubernetesHttpApiClient(
            namespace=self._namespace
        )
        self._gcs_client = gcs_client

        # Below are states that are cached locally (FIFO-bounded via insertion-ordered dicts).
        self._requests: Dict[str, None] = {}
        self._accepted_launch_workers: Dict[str, Dict[NodeType, int]] = {}
        self._claimed_pre_step_lids: Dict[Tuple[NodeType, str], None] = {}
        self._max_tracked_request_entries = 4096
        self._launch_errors_queue = []
        self._terminate_errors_queue = []

        # Below are states for idle-cluster termination tracking.
        # Monotonic timestamp when no driver was first observed; None resets it.
        self._no_driver_observed_since: Optional[float] = None
        # Latest GCS job end time seen; a newer one means a driver came and went.
        self._last_seen_job_end_time = 0
        # idle-termination timeout (seconds) from the CR; None disables the feature.
        self._idle_termination_timeout_seconds: Optional[float] = None
        self._idle_termination_policy: Optional[str] = None

        # Below are states that are fetched from the Kubernetes API server.
        self._ray_cluster = None
        self._cached_instances: Optional[Dict[CloudInstanceId, CloudInstance]] = None
        self._last_pod_list_rv: Optional[str] = None
        self._last_seen_cr_workers: Optional[Dict[NodeType, int]] = None
        self._external_unmaterialized: Dict[NodeType, int] = defaultdict(int)
        self._ippr_provider = KubeRayIPPRProvider(
            gcs_client=gcs_client, k8s_api_client=self._k8s_api_client
        )

        # Transient reconcile-step context populated only during reconcile_step.
        self._active_step: Optional["KubeRayProvider._ActiveReconcileStep"] = None
        self._deferred_patches: Optional[Dict[str, Dict[str, Any]]] = None

    @dataclass
    class _ActiveReconcileStep:
        synced: bool = False
        standalone_batch: bool = False
        known_cloud_instance_ids: Set[CloudInstanceId] = field(default_factory=set)
        pre_sh_in_flight: Dict[NodeType, Dict[str, int]] = field(default_factory=dict)
        pre_mh_in_flight: Dict[NodeType, Dict[str, int]] = field(default_factory=dict)
        launched_sh: Dict[NodeType, Dict[str, int]] = field(default_factory=dict)
        launched_mh: Dict[NodeType, Dict[str, int]] = field(default_factory=dict)

    @dataclass
    class ScaleRequest:
        """Represents a scale request that contains the current states and go-to states
        for the ray cluster.

        This class will be converted to patches to be submitted to the Kubernetes API
        server:
        - For launching new instances, it will adjust the `replicas` field in the
            workerGroupSpecs.
        - For terminating instances, it will adjust the `workersToDelete` field in the
            workerGroupSpecs.

        """

        # The desired number of workers for each node type.
        desired_num_workers: Dict[NodeType, int] = field(default_factory=dict)
        # The workers to delete for each node type.
        workers_to_delete: Dict[NodeType, List[CloudInstanceId]] = field(
            default_factory=dict
        )
        # The worker groups with empty workersToDelete field.
        # This is needed since we will also need to clear the workersToDelete field
        # for the worker groups that have finished deletes.
        worker_groups_without_pending_deletes: Set[NodeType] = field(
            default_factory=set
        )
        # The worker groups that still have workers to be deleted.
        worker_groups_with_pending_deletes: Set[NodeType] = field(default_factory=set)
        # Worker groups targeted by a terminate request (including pods already
        # present in workersToDelete so pending-delete checks stay group-scoped).
        targeted_delete_groups: Set[NodeType] = field(default_factory=set)
        # Worker pods rejected from termination due to invalid numOfHosts <= 0.
        invalid_delete_ids: List[Tuple[CloudInstanceId, NodeType, int]] = field(
            default_factory=list
        )

    ################################
    # Interface for ICloudInstanceProvider
    ################################

    @property
    def _in_reconcile_step(self) -> bool:
        return self._active_step is not None

    def _record_bounded_entry(
        self, store: Dict[Any, Any], key: Any, value: Any = None
    ) -> None:
        store.pop(key, None)
        store[key] = value
        while len(store) > self._max_tracked_request_entries:
            del store[next(iter(store))]

    def _hosts_by_type(
        self, ray_cluster: Optional[Dict[str, Any]] = None
    ) -> Dict[NodeType, int]:
        cluster = self._ray_cluster if ray_cluster is None else ray_cluster
        if not cluster:
            return {}
        return {
            wg["groupName"]: _worker_group_num_of_hosts(cluster, idx)
            for idx, wg in enumerate(
                cluster.get("spec", {}).get("workerGroupSpecs", [])
            )
        }

    def _claim_untracked_pre_step_in_flight(self) -> None:
        step = self._active_step
        if (
            step is None
            or self._last_seen_cr_workers is None
            or self._ray_cluster is None
        ):
            return
        hosts_by_type = self._hosts_by_type()
        active_pre_lids = {
            (nt, lid)
            for src in (step.pre_sh_in_flight, step.pre_mh_in_flight)
            for nt, counts in src.items()
            for lid, cnt in counts.items()
            if cnt > 0
        }
        if self._claimed_pre_step_lids:
            self._claimed_pre_step_lids = {
                (nt, lid): None
                for nt, lid in self._claimed_pre_step_lids
                if (nt, lid) in active_pre_lids
            }
        for nt, counts in step.pre_sh_in_flight.items():
            for lid, cnt in counts.items():
                if (
                    cnt > 0
                    and lid not in self._requests
                    and (nt, lid) not in self._claimed_pre_step_lids
                ):
                    self._claimed_pre_step_lids[(nt, lid)] = None
                    self._external_unmaterialized[nt] = max(
                        0, self._external_unmaterialized.get(nt, 0) - cnt
                    )
        for nt, counts in step.pre_mh_in_flight.items():
            hosts = max(1, hosts_by_type.get(nt, 1))
            for lid, reps in counts.items():
                if (
                    reps > 0
                    and lid not in self._requests
                    and (nt, lid) not in self._claimed_pre_step_lids
                ):
                    self._claimed_pre_step_lids[(nt, lid)] = None
                    self._external_unmaterialized[nt] = max(
                        0, self._external_unmaterialized.get(nt, 0) - reps * hosts
                    )

    def _apply_reconcile_step_context(self, context: ReconcileStepContext) -> None:
        if self._active_step is None:
            return
        self._active_step.known_cloud_instance_ids = set(
            context.known_cloud_instance_ids
        )
        self._active_step.pre_sh_in_flight = {
            nt: dict(counts) for nt, counts in context.single_host_in_flight.items()
        }
        self._active_step.pre_mh_in_flight = {
            nt: dict(counts) for nt, counts in context.unclaimed_replicas_by_lid.items()
        }
        self._claim_untracked_pre_step_in_flight()

    def _enter_reconcile_step(self, context: ReconcileStepContext) -> None:
        self._active_step = self._ActiveReconcileStep()
        self._apply_reconcile_step_context(context)

    def update_reconcile_step(self, context: ReconcileStepContext) -> None:
        self._apply_reconcile_step_context(context)

    def _exit_reconcile_step(self) -> None:
        self._active_step = None

    def get_node_types_with_pending_deletes(
        self,
        terminating_cloud_instance_ids: Optional[Set[CloudInstanceId]] = None,
    ) -> Set[NodeType]:
        self._ensure_synced_for_step()
        cached = self._cached_instances or {}
        pending_groups, _, _ = self._get_workers_delete_info(
            self._ray_cluster, set(cached.keys())
        )
        if terminating_cloud_instance_ids:
            for cid in terminating_cloud_instance_ids:
                c_inst = cached.get(cid)
                if c_inst is not None and c_inst.node_kind == NodeKind.WORKER:
                    pending_groups.add(c_inst.node_type)
        return pending_groups

    def get_non_terminated(self) -> Dict[CloudInstanceId, CloudInstance]:
        self._sync_with_api_server()
        self._evaluate_idle_termination()
        return copy.deepcopy(dict(self._cached_instances or {}))

    def terminate(self, ids: List[CloudInstanceId], request_id: str) -> None:
        if request_id in self._requests:
            # This request is already processed.
            logger.warning(f"Request {request_id} is already processed for: {ids}")
            return

        logger.info("Terminating worker pods: {}".format(ids))
        scale_request = None
        try:
            scale_request = self._initialize_scale_request(
                to_launch={}, to_delete_instances=ids, request_id=request_id
            )

            pending_groups = (
                scale_request.worker_groups_with_pending_deletes
                & scale_request.targeted_delete_groups
            )
            if pending_groups:
                errors_msg = (
                    "There are workers to be deleted from: "
                    f"{pending_groups}. "
                    "Waiting for them to be deleted before adding new workers "
                    " to be deleted"
                )
                logger.warning(errors_msg)
                self._add_terminate_errors(
                    ids,
                    request_id,
                    details=errors_msg,
                )
                return

            self._submit_scale_request(scale_request)
            if self._active_step is not None:
                # Re-sync before any subsequent launch/pending-delete check in
                # the same step so updated pod/workersToDelete visibility applies.
                self._active_step.synced = False

            for cid, node_type, bad_hosts in scale_request.invalid_delete_ids:
                err_msg = (
                    f"Worker group {node_type} has invalid numOfHosts={bad_hosts}; "
                    f"rejecting termination of {cid}."
                )
                logger.warning(err_msg)
                self._add_terminate_errors([cid], request_id, details=err_msg)

            # Only add to processed requests if at least one group was handled or no
            # invalid numOfHosts errors occurred.
            if not scale_request.invalid_delete_ids or scale_request.workers_to_delete:
                self._record_bounded_entry(self._requests, request_id)

        except Exception as e:
            if self._active_step is not None:
                self._active_step.synced = False
            logger.exception(f"Error terminating nodes: {scale_request or 'N/A'}")
            self._add_terminate_errors(ids, request_id, details=str(e), e=e)

    def launch_many(self, requests: Dict[str, Dict[NodeType, int]]) -> None:
        if not requests:
            return

        # Sync once upfront before snapshotting transactional state for coalesced PATCH.
        try:
            self._ensure_synced_for_step()
        except Exception as e:
            if self._active_step is not None:
                self._active_step.synced = False
            logger.exception("Error syncing KubeRay state before batch launch")
            for request_id, shape in requests.items():
                if request_id not in self._requests:
                    self._add_launch_errors(shape, request_id, details=str(e), e=e)
            return

        snap_cr = copy.deepcopy(self._ray_cluster)
        snap_reqs = dict(self._requests)
        snap_acc = copy.deepcopy(self._accepted_launch_workers)
        snap_ext = defaultdict(int, self._external_unmaterialized)
        snap_claimed = dict(self._claimed_pre_step_lids)
        snap_last_seen_cr = (
            None
            if self._last_seen_cr_workers is None
            else dict(self._last_seen_cr_workers)
        )
        snap_step = copy.deepcopy(self._active_step)
        snap_err_len = len(self._launch_errors_queue)
        attempted_fresh = [
            (rid, shape) for rid, shape in requests.items() if rid not in snap_reqs
        ]

        created_temp_step = self._active_step is None
        if created_temp_step:
            wgs = (self._ray_cluster or {}).get("spec", {}).get("workerGroupSpecs", [])
            self._active_step = self._ActiveReconcileStep(
                synced=True,
                standalone_batch=True,
                known_cloud_instance_ids=set((self._cached_instances or {}).keys()),
                pre_mh_in_flight={
                    wg["groupName"]: {}
                    for idx, wg in enumerate(wgs)
                    if _worker_group_num_of_hosts(self._ray_cluster, idx) > 1
                },
            )

        self._deferred_patches = {}
        try:
            for request_id, shape in requests.items():
                self.launch(shape=shape, request_id=request_id)
            final_patches = list(self._deferred_patches.values())
        finally:
            self._deferred_patches = None
            if created_temp_step:
                self._active_step = None

        if not final_patches:
            return

        try:
            logger.info(f"Submitting coalesced scale patch: {final_patches}")
            resp = self._patch(f"rayclusters/{self._cluster_name}", final_patches)
            if isinstance(resp, dict) and "spec" in resp:
                self._ray_cluster = resp
            if self._ray_cluster is not None:
                patched_types = {
                    self._ray_cluster["spec"]["workerGroupSpecs"][
                        int(p["path"].split("/")[3])
                    ]["groupName"]
                    for p in final_patches
                    if p["path"].endswith("/replicas")
                }
                self._update_external_cr_capacity(
                    autoscaler_patched_types=patched_types
                )
        except Exception as e:
            self._ray_cluster = snap_cr
            self._requests = snap_reqs
            self._accepted_launch_workers = snap_acc
            self._external_unmaterialized = snap_ext
            self._claimed_pre_step_lids = snap_claimed
            self._last_seen_cr_workers = snap_last_seen_cr
            self._active_step = snap_step
            if self._active_step is not None:
                self._active_step.synced = False
            del self._launch_errors_queue[snap_err_len:]
            logger.exception(
                f"Error submitting coalesced launch patch: {final_patches}"
            )
            for request_id, shape in attempted_fresh:
                self._add_launch_errors(shape, request_id, details=str(e), e=e)

    def launch(self, shape: Dict[NodeType, int], request_id: str) -> None:
        if request_id in self._requests:
            # This request is already processed. Clamp to the retried shape so
            # partial single-host timeout retries do not re-add materialized pods
            # or co-launched sibling worker groups.
            if self._active_step is not None:
                accepted = self._accepted_launch_workers.get(request_id, {})
                replay_shape = {
                    nt: min(req_w, accepted.get(nt, 0))
                    for nt, req_w in shape.items()
                    if req_w > 0
                }
                self._record_step_launch(replay_shape, request_id, {})
            return

        scale_request = None
        try:
            scale_request = self._initialize_scale_request(
                to_launch=shape, to_delete_instances=[], request_id=request_id
            )

            target_groups = {nt for nt, count in shape.items() if count > 0}
            pending_groups = (
                scale_request.worker_groups_with_pending_deletes & target_groups
            )
            if pending_groups:
                error_msg = (
                    "There are workers to be deleted from: "
                    f"{pending_groups}. "
                    "Waiting for them to be deleted before creating new workers."
                )
                logger.warning(error_msg)
                self._add_launch_errors(
                    shape,
                    request_id,
                    details=error_msg,
                )
                return

            rejected_nodes = self._submit_scale_request(scale_request)

            # This request_id has been handled (full, partial, or no-op). Cache its
            # accepted worker counts so a replay does not add to_launch again or
            # discard prior cap/alignment rejections.
            self._record_bounded_entry(self._requests, request_id)
            self._record_bounded_entry(
                self._accepted_launch_workers,
                request_id,
                {
                    nt: max(0, req_w - min(req_w, rejected_nodes.get(nt, 0)))
                    for nt, req_w in shape.items()
                    if req_w > 0
                },
            )

            if self._active_step is not None:
                self._record_step_launch(shape, request_id, rejected_nodes)

            nodes_to_reject = {}
            for node_type, rejected in rejected_nodes.items():
                requested = shape.get(node_type, 0)
                if requested > 0 and rejected > 0:
                    nodes_to_reject[node_type] = min(rejected, requested)

            if nodes_to_reject:
                self._add_launch_errors(
                    nodes_to_reject,
                    request_id,
                    details=(
                        "Launch rejected by maxReplicas limit or unaligned "
                        "numOfHosts count."
                    ),
                )

        except Exception as e:
            if self._active_step is not None:
                self._active_step.synced = False
            logger.exception(f"Error launching nodes: {scale_request or 'N/A'}")
            self._add_launch_errors(shape, request_id, details=str(e), e=e)

    def poll_errors(self) -> List[CloudInstanceProviderError]:
        errors = []
        errors += self._launch_errors_queue
        errors += self._terminate_errors_queue
        self._launch_errors_queue = []
        self._terminate_errors_queue = []
        return errors

    def get_ippr_specs(self) -> IPPRSpecs:
        """Return the cached, validated IPPR specs for the cluster.

        The IPPR specs are refreshed during the provider's periodic sync with the
        API server by reading the RayCluster annotation and validating it against
        the IPPR schema.
        """
        return self._ippr_provider.get_ippr_specs()

    def get_ippr_statuses(self) -> Dict[str, IPPRStatus]:
        """Return the latest per-pod IPPR statuses keyed by pod name.

        These statuses are refreshed from the current pod list during the provider's
        periodic sync with the API server.
        """
        return self._ippr_provider.get_ippr_statuses()

    def do_ippr_requests(self, resizes: List[IPPRStatus]) -> None:
        """Execute IPPR resize requests via the underlying IPPR provider.

        Args:
            resizes: The list of per-pod IPPR actions produced by the scheduler.
        """
        self._ippr_provider.do_ippr_requests(resizes)

    ############################
    # Private
    ############################

    def _ensure_synced_for_step(self) -> None:
        step = self._active_step
        if step is None or not step.synced:
            self._sync_with_api_server()
            if step is not None:
                step.synced = True

    @staticmethod
    def _group_observed_pods(
        pods: List[CloudInstance], num_of_hosts: int
    ) -> Tuple[Dict[str, List[CloudInstance]], List[CloudInstance], int, int]:
        by_replica: Dict[str, List[CloudInstance]] = defaultdict(list)
        ungrouped_pods: List[CloudInstance] = []
        for pod in pods:
            if num_of_hosts > 1 and pod.replica_id:
                by_replica[pod.replica_id].append(pod)
            else:
                ungrouped_pods.append(pod)
        obs_ungrouped_reps = (len(ungrouped_pods) + num_of_hosts - 1) // num_of_hosts
        effective_observed = (len(by_replica) + obs_ungrouped_reps) * num_of_hosts
        return by_replica, ungrouped_pods, obs_ungrouped_reps, effective_observed

    def _record_step_launch(
        self,
        shape: Dict[NodeType, int],
        request_id: str,
        rejected_nodes: Dict[NodeType, int],
    ) -> None:
        step = self._active_step
        if step is None:
            return
        hosts_by_type = self._hosts_by_type()
        for node_type, req_w in shape.items():
            if req_w <= 0:
                continue
            hosts = hosts_by_type.get(node_type, 1)
            if hosts <= 0:
                continue
            replica_managed = hosts > 1 and node_type in step.pre_mh_in_flight
            unit_size = hosts if replica_managed else 1
            pre_map = (
                step.pre_mh_in_flight[node_type]
                if replica_managed
                else step.pre_sh_in_flight.get(node_type, {})
            )
            step_map = (
                step.launched_mh.setdefault(node_type, {})
                if replica_managed
                else step.launched_sh.setdefault(node_type, {})
            )
            cur_w = max(step_map.get(request_id, 0) * unit_size, req_w)
            rej = min(req_w, rejected_nodes.get(node_type, 0))
            accepted_units = max(0, cur_w - rej) // unit_size
            extra_units = max(0, accepted_units - pre_map.get(request_id, 0))
            if extra_units > 0:
                step_map[request_id] = extra_units
            else:
                step_map.pop(request_id, None)

    def _initialize_scale_request(
        self,
        to_launch: Dict[NodeType, int],
        to_delete_instances: List[CloudInstanceId],
        request_id: str = "",
    ) -> "KubeRayProvider.ScaleRequest":
        """Initialize the scale request based on the current state of the cluster and
        the desired state (to launch, to delete).

        Args:
            to_launch: The desired number of workers to launch for each node type.
            to_delete_instances: The instances to delete.
            request_id: The launch or terminate request ID for in-step accounting.

        Returns:
            The scale request.
        """

        # Update the cached states (once per reconcile step, or every standalone call).
        self._ensure_synced_for_step()
        ray_cluster = self.ray_cluster
        cur_instances = self.instances

        # Get the worker groups that have pending deletes and the worker groups that
        # have finished deletes, and the set of workers included in the workersToDelete
        # field of any worker group.
        (
            worker_groups_with_pending_deletes,
            worker_groups_without_pending_deletes,
            worker_to_delete_set,
        ) = self._get_workers_delete_info(ray_cluster, set(cur_instances.keys()))

        worker_groups = ray_cluster["spec"].get("workerGroupSpecs", [])
        hosts_by_type = self._hosts_by_type(ray_cluster)

        to_delete_instances_by_type: Dict[NodeType, List[CloudInstance]] = defaultdict(
            list
        )
        targeted_delete_groups: Set[NodeType] = set()
        to_delete_id_set: Set[CloudInstanceId] = set()
        invalid_delete_ids: List[Tuple[CloudInstanceId, NodeType, int]] = []
        for to_delete_id in to_delete_instances:
            to_delete_instance = cur_instances.get(to_delete_id)
            if (
                to_delete_instance is None
                or to_delete_instance.node_kind == NodeKind.HEAD
            ):
                continue
            nt = to_delete_instance.node_type
            num_hosts = hosts_by_type.get(nt, 1)
            if num_hosts <= 0:
                invalid_delete_ids.append((to_delete_id, nt, num_hosts))
                continue
            targeted_delete_groups.add(nt)
            if to_delete_id in to_delete_id_set or to_delete_id in worker_to_delete_set:
                continue
            to_delete_id_set.add(to_delete_id)
            to_delete_instances_by_type[nt].append(to_delete_instance)

        pods_by_type: Dict[NodeType, List[CloudInstance]] = defaultdict(list)
        for instance in cur_instances.values():
            if (
                instance.node_kind != NodeKind.WORKER
                or instance.cloud_instance_id in worker_to_delete_set
            ):
                continue
            pods_by_type[instance.node_type].append(instance)

        # Calculate the desired number of workers by type.
        num_workers_dict: Dict[NodeType, int] = defaultdict(int)
        step = self._active_step
        for worker_group in worker_groups:
            node_type = worker_group["groupName"]
            req_w = to_launch.get(node_type, 0)
            is_targeted = req_w > 0 or node_type in to_delete_instances_by_type
            if step is not None and not is_targeted:
                continue

            num_of_hosts = hosts_by_type[node_type]
            if num_of_hosts <= 0:
                continue

            min_w = (worker_group.get("minReplicas") or 0) * num_of_hosts
            (
                by_replica,
                ungrouped_pods,
                obs_ungrouped_reps,
                effective_observed,
            ) = self._group_observed_pods(pods_by_type.get(node_type, []), num_of_hosts)

            # KubeRay reconciles a labeled multi-host group by replica name: each
            # replica counts whole even while < num_of_hosts pods exist, and naming
            # any of its pods in workersToDelete deletes the whole replica. Name all
            # of its pods so the replica reads as pending deletion until every pod
            # is gone. Single-host pods and unlabeled multi-host pods stay in
            # `ungrouped_pods`, using 1-worker or ceiling-replica units uniformly.
            removed_replicas = [
                r_pods
                for r_pods in by_replica.values()
                if any(p.cloud_instance_id in to_delete_id_set for p in r_pods)
            ]
            for r_pods in removed_replicas:
                for pod in r_pods:
                    if pod.cloud_instance_id not in to_delete_id_set:
                        to_delete_id_set.add(pod.cloud_instance_id)
                        to_delete_instances_by_type[node_type].append(pod)

            removed_ungrouped = sum(
                1 for p in ungrouped_pods if p.cloud_instance_id in to_delete_id_set
            )
            removed_workers = len(removed_replicas) * num_of_hosts + removed_ungrouped

            known_cids = step.known_cloud_instance_ids if step is not None else set()
            ext_w = self._external_unmaterialized.get(node_type, 0)
            if step is None or step.standalone_batch:
                ext_w += max(0, min_w - effective_observed - ext_w)
            req_pos = max(0, req_w)
            if num_of_hosts > 1 and (
                step is None or node_type in step.pre_mh_in_flight
            ):
                pre_map = (
                    step.pre_mh_in_flight.get(node_type, {}) if step is not None else {}
                )
                step_map = (
                    step.launched_mh.get(node_type, {}) if step is not None else {}
                )
                new_labeled_reps = sum(
                    1
                    for r_pods in by_replica.values()
                    if all(p.cloud_instance_id not in known_cids for p in r_pods)
                )
                new_ungrouped = sum(
                    1 for p in ungrouped_pods if p.cloud_instance_id not in known_cids
                )
                known_ungrouped_reps = (
                    len(ungrouped_pods) - new_ungrouped + num_of_hosts - 1
                ) // num_of_hosts
                newly_materialized_reps = new_labeled_reps + max(
                    0, obs_ungrouped_reps - known_ungrouped_reps
                )
                req_reps, cur_unaligned = divmod(req_pos, num_of_hosts)
                unlabeled_gap = (-len(ungrouped_pods)) % num_of_hosts
                if (
                    step is not None
                    and unlabeled_gap > 0
                    and cur_unaligned == unlabeled_gap
                ):
                    # Exact backfill of an incomplete unlabeled replica is already
                    # accounted for in `obs_ungrouped_reps` ceiling units.
                    cur_unaligned = 0
                in_flight_reps = self._compute_in_flight_units(
                    pre_map, step_map, request_id, req_reps, newly_materialized_reps
                )
                num_workers_dict[node_type] = (
                    effective_observed
                    + ext_w
                    + in_flight_reps * num_of_hosts
                    + cur_unaligned
                    - removed_workers
                )
            else:
                pre_sh_map = (
                    step.pre_sh_in_flight.get(node_type, {}) if step is not None else {}
                )
                step_sh_map = (
                    step.launched_sh.get(node_type, {}) if step is not None else {}
                )
                new_pods = sum(
                    1
                    for r_pods in by_replica.values()
                    for p in r_pods
                    if p.cloud_instance_id not in known_cids
                ) + sum(
                    1 for p in ungrouped_pods if p.cloud_instance_id not in known_cids
                )
                in_flight_w = self._compute_in_flight_units(
                    pre_sh_map, step_sh_map, request_id, req_pos, new_pods
                )
                removed_rep_ids = {id(r_pods) for r_pods in removed_replicas}
                surviving_actual_pods = (
                    sum(
                        len(r_pods)
                        for r_pods in by_replica.values()
                        if id(r_pods) not in removed_rep_ids
                    )
                    + len(ungrouped_pods)
                    - removed_ungrouped
                )
                num_workers_dict[node_type] = (
                    max(
                        effective_observed - removed_workers,
                        surviving_actual_pods + in_flight_w,
                    )
                    + ext_w
                )

        for node_type, count in to_launch.items():
            if count > 0 and node_type not in num_workers_dict:
                num_workers_dict[node_type] += count

        scale_request = KubeRayProvider.ScaleRequest(
            desired_num_workers=num_workers_dict,
            workers_to_delete=to_delete_instances_by_type,
            worker_groups_without_pending_deletes=worker_groups_without_pending_deletes,
            worker_groups_with_pending_deletes=worker_groups_with_pending_deletes,
            targeted_delete_groups=targeted_delete_groups,
            invalid_delete_ids=invalid_delete_ids,
        )

        return scale_request

    def _submit_scale_request(
        self, scale_request: "KubeRayProvider.ScaleRequest"
    ) -> Dict[NodeType, int]:
        """Submits a scale request to the Kubernetes API server.

        This method will convert the scale request to patches and submit the patches
        to the Kubernetes API server.

        Args:
            scale_request: The scale request.

        Returns:
            A dictionary mapping node types to the number of nodes that were rejected
            due to the maxReplicas cap.

        Raises:
            Exception: An exception is raised if the Kubernetes API server returns an
                error.
        """
        # Get the current ray cluster spec.
        patch_payload = []
        patched_replicas: List[Tuple[int, int]] = []
        patched_deletes: List[Tuple[int, List[CloudInstanceId]]] = []

        raycluster = self.ray_cluster

        rejected_nodes = {}

        # Collect patches for replica counts.
        for node_type, num_workers in scale_request.desired_num_workers.items():
            group_index = _worker_group_index(raycluster, node_type)
            group_max_replicas = _worker_group_max_replicas(raycluster, group_index)
            group_min_replicas = _worker_group_min_replicas(raycluster, group_index)
            group_num_of_hosts = _worker_group_num_of_hosts(raycluster, group_index)
            if group_num_of_hosts <= 0:
                if num_workers > 0:
                    logger.warning(
                        "Worker group %s has invalid numOfHosts=%s; rejecting launch "
                        "of %d workers.",
                        node_type,
                        group_num_of_hosts,
                        num_workers,
                    )
                    rejected_nodes[node_type] = num_workers
                continue
            # the num_workers from the scale request is multiplied by numOfHosts, so we need to divide it back.
            unaligned_workers = num_workers % group_num_of_hosts
            target_replicas = num_workers // group_num_of_hosts
            # Cap the replica count to maxReplicas.
            original_target_replicas = target_replicas
            if group_max_replicas is not None and group_max_replicas < target_replicas:
                logger.warning(
                    "Autoscaler attempted to create "
                    + "more than maxReplicas pods of type {}.".format(node_type)
                )
                target_replicas = group_max_replicas

                # Calculate how many nodes were rejected due to the cap
                rejected_replicas = original_target_replicas - target_replicas
                if rejected_replicas > 0:
                    rejected_nodes[node_type] = rejected_replicas * group_num_of_hosts
            # Raise the replica count to minReplicas, matching KubeRay's clamp.
            if group_min_replicas > target_replicas:
                logger.info(
                    "Autoscaler attempted to request "
                    + "fewer than minReplicas pods of type {}.".format(node_type)
                )
                target_replicas = group_min_replicas
            if (
                unaligned_workers > 0
                and self._active_step is not None
                and node_type in self._active_step.pre_mh_in_flight
            ):
                rejected_nodes[node_type] = (
                    rejected_nodes.get(node_type, 0) + unaligned_workers
                )
            # Check if we need to change the target count.
            if target_replicas == _worker_group_replicas(raycluster, group_index):
                # No patch required.
                continue
            # Need to patch replica count. Format the patch and add it to the payload.
            patch = worker_replica_patch(group_index, target_replicas)
            patch_payload.append(patch)
            patched_replicas.append((group_index, target_replicas))

        # Maps node_type to nodes to delete for that group.
        for (
            node_type,
            workers_to_delete_of_type,
        ) in scale_request.workers_to_delete.items():
            group_index = _worker_group_index(raycluster, node_type)
            worker_ids_to_delete = [
                worker.cloud_instance_id for worker in workers_to_delete_of_type
            ]
            patch = worker_delete_patch(group_index, worker_ids_to_delete)
            patch_payload.append(patch)
            patched_deletes.append((group_index, worker_ids_to_delete))

        # Clear the workersToDelete field for the worker groups that have been deleted.
        for node_type in scale_request.worker_groups_without_pending_deletes:
            if node_type in scale_request.workers_to_delete:
                # This node type is still being deleted.
                continue
            group_index = _worker_group_index(raycluster, node_type)
            patch = worker_delete_patch(group_index, [])
            patch_payload.append(patch)
            patched_deletes.append((group_index, []))

        if len(patch_payload) > 0:
            patched_types = {
                raycluster["spec"]["workerGroupSpecs"][group_index]["groupName"]
                for group_index, _ in patched_replicas
            }
            if self._deferred_patches is not None:
                for p_op in patch_payload:
                    self._deferred_patches[p_op["path"]] = p_op
                resp = None
            else:
                logger.info(f"Submitting a scale request: {scale_request}")
                resp = self._patch(f"rayclusters/{self._cluster_name}", patch_payload)
            if isinstance(resp, dict) and "spec" in resp:
                self._ray_cluster = resp
            elif self._ray_cluster is not None:
                worker_groups = self._ray_cluster.get("spec", {}).get(
                    "workerGroupSpecs", []
                )
                for group_index, target_replicas in patched_replicas:
                    worker_groups[group_index]["replicas"] = target_replicas
                for group_index, worker_ids in patched_deletes:
                    worker_groups[group_index].setdefault("scaleStrategy", {})[
                        "workersToDelete"
                    ] = list(worker_ids)
            if self._deferred_patches is None:
                self._update_external_cr_capacity(
                    autoscaler_patched_types=patched_types
                )

        return rejected_nodes

    def _add_launch_errors(
        self,
        shape: Dict[NodeType, int],
        request_id: str,
        details: str,
        e: Optional[Exception] = None,
    ) -> None:
        """Adds launch errors to the error queue.

        Args:
            shape: The shape of the nodes that failed to launch.
            request_id: The request id of the launch request.
            details: The details of the error.
            e: The exception that caused the error.
        """
        for node_type, count in shape.items():
            self._launch_errors_queue.append(
                LaunchNodeError(
                    node_type=node_type,
                    timestamp_ns=time.time_ns(),
                    count=count,
                    request_id=request_id,
                    details=details,
                    cause=e,
                )
            )

    def _add_terminate_errors(
        self,
        ids: List[CloudInstanceId],
        request_id: str,
        details: str,
        e: Optional[Exception] = None,
    ) -> None:
        """Adds terminate errors to the error queue.

        Args:
            ids: The ids of the nodes that failed to terminate.
            request_id: The request id of the terminate request.
            details: The details of the error.
            e: The exception that caused the error.
        """
        for id in ids:
            self._terminate_errors_queue.append(
                TerminateNodeError(
                    cloud_instance_id=id,
                    timestamp_ns=time.time_ns(),
                    request_id=request_id,
                    details=details,
                    cause=e,
                )
            )

    @staticmethod
    def _compute_in_flight_units(
        pre_map: Dict[str, int],
        step_map: Dict[str, int],
        request_id: str,
        req_units: int,
        newly_materialized: int,
    ) -> int:
        pre_rem = max(0, sum(pre_map.values()) - newly_materialized)
        step_extra = sum(c for k, c in step_map.items() if k != request_id) + max(
            step_map.get(request_id, 0),
            max(0, req_units - pre_map.get(request_id, 0)),
        )
        return pre_rem + step_extra

    def _update_external_cr_capacity(
        self, autoscaler_patched_types: Optional[Set[NodeType]] = None
    ) -> None:
        if self._ray_cluster is None:
            return
        patched = autoscaler_patched_types or set()
        cached = self._cached_instances or {}
        _, _, worker_to_delete_set = self._get_workers_delete_info(
            self._ray_cluster, set(cached.keys())
        )
        pods_by_type: Dict[NodeType, List[CloudInstance]] = defaultdict(list)
        for cid, inst in cached.items():
            if inst.node_kind == NodeKind.WORKER and cid not in worker_to_delete_set:
                pods_by_type[inst.node_type].append(inst)
        cur_cr: Dict[NodeType, int] = {}
        for idx, wg in enumerate(
            self._ray_cluster.get("spec", {}).get("workerGroupSpecs", [])
        ):
            nt = wg["groupName"]
            hosts = _worker_group_num_of_hosts(self._ray_cluster, idx)
            if hosts <= 0:
                continue
            replicas_w = (wg.get("replicas") or 0) * hosts
            min_w = (wg.get("minReplicas") or 0) * hosts
            cur_cr[nt] = replicas_w
            if nt in patched:
                continue
            _, _, _, eff_obs = self._group_observed_pods(pods_by_type[nt], hosts)
            unbacked = max(0, replicas_w - max(eff_obs, min_w))
            if self._last_seen_cr_workers is None:
                self._external_unmaterialized[nt] = unbacked
            else:
                prev_spec_w = self._last_seen_cr_workers.get(nt, 0)
                ext = max(
                    0,
                    self._external_unmaterialized.get(nt, 0)
                    + (replicas_w - prev_spec_w),
                )
                self._external_unmaterialized[nt] = min(ext, unbacked)
        self._last_seen_cr_workers = cur_cr
        self._claim_untracked_pre_step_in_flight()

    def _sync_with_api_server(self) -> None:
        """Fetches the RayCluster resource from the Kubernetes API server."""
        self._ray_cluster = self._get(f"rayclusters/{self._cluster_name}")
        self._refresh_idle_termination_config()
        self._ippr_provider.validate_and_set_ippr_specs(self._ray_cluster)
        self._cached_instances = self._fetch_instances()
        self._update_external_cr_capacity()
        self._ippr_provider.sync_with_raylets()

    def _refresh_idle_termination_config(self) -> None:
        """Reads IdleTerminationOptions from the RayCluster CR."""
        opts = self._ray_cluster["spec"].get(IDLE_TERMINATION_OPTIONS_KEY, {})
        secs = opts.get(IDLE_TERMINATION_OPTIONS_TIMEOUT_SECONDS_KEY)
        self._idle_termination_timeout_seconds = (
            float(secs) if secs is not None else None
        )
        policy = opts.get(
            IDLE_TERMINATION_OPTIONS_POLICY_KEY, IDLE_TERMINATION_OPTIONS_POLICY_SUSPEND
        )
        self._idle_termination_policy = policy

    @property
    def ray_cluster(self) -> Dict[str, Any]:
        return copy.deepcopy(self._ray_cluster)

    @property
    def instances(self) -> Dict[CloudInstanceId, CloudInstance]:
        return copy.deepcopy(self._cached_instances or {})

    @staticmethod
    def _get_workers_delete_info(
        ray_cluster_spec: Dict[str, Any], node_set: Set[CloudInstanceId]
    ) -> Tuple[Set[NodeType], Set[NodeType], Set[CloudInstanceId]]:
        """Gets the worker groups that have pending deletes and the worker groups that
        have finished deletes.

        Args:
            ray_cluster_spec: The RayCluster CR spec dict.
            node_set: The set of currently known cloud instance IDs.

        Returns:
            A tuple of:

            - worker_groups_with_pending_deletes: The worker groups that have pending
              deletes.
            - worker_groups_with_finished_deletes: The worker groups that have finished
              deletes.
            - worker_to_delete_set: A set of Pods that are included in the
              workersToDelete field of any worker group.
        """

        worker_groups_with_pending_deletes = set()
        worker_groups_with_deletes = set()
        worker_to_delete_set = set()

        worker_groups = ray_cluster_spec["spec"].get("workerGroupSpecs", [])
        for worker_group in worker_groups:
            workersToDelete = worker_group.get("scaleStrategy", {}).get(
                "workersToDelete", []
            )
            if not workersToDelete:
                # No workers to delete in this group.
                continue

            node_type = worker_group["groupName"]
            worker_groups_with_deletes.add(node_type)

            for worker in workersToDelete:
                worker_to_delete_set.add(worker)
                if worker in node_set:
                    worker_groups_with_pending_deletes.add(node_type)

        worker_groups_with_finished_deletes = (
            worker_groups_with_deletes - worker_groups_with_pending_deletes
        )
        return (
            worker_groups_with_pending_deletes,
            worker_groups_with_finished_deletes,
            worker_to_delete_set,
        )

    def _fetch_instances(self) -> Dict[CloudInstanceId, CloudInstance]:
        """Fetches the pods from the Kubernetes API server and convert them to Ray
        CloudInstance.

        Returns:
            A dict of CloudInstanceId to CloudInstance.
        """
        # Specifying a resource version in list requests is important for scalability
        # and monotonic in-step consistency:
        # https://kubernetes.io/docs/reference/using-api/api-concepts/#semantics-for-get-and-list
        resource_version = (
            self._last_pod_list_rv or self._get_head_pod_resource_version()
            if self._in_reconcile_step
            else self._get_head_pod_resource_version()
        )
        if resource_version:
            logger.info(
                f"Listing pods for RayCluster {self._cluster_name}"
                f" in namespace {self._namespace}"
                f" at pods resource version >= {resource_version}."
            )

        # Filter pods by cluster_name.
        label_selector = requests.utils.quote(f"ray.io/cluster={self._cluster_name}")

        resource_path = f"pods?labelSelector={label_selector}"
        if resource_version:
            resource_path += (
                f"&resourceVersion={resource_version}"
                + "&resourceVersionMatch=NotOlderThan"
            )

        try:
            pod_list = self._get(resource_path)
        except Exception:
            self._last_pod_list_rv = None
            raise
        fetched_resource_version = pod_list.get("metadata", {}).get(
            "resourceVersion", ""
        )
        self._last_pod_list_rv = fetched_resource_version or None
        logger.info(
            f"Fetched pod data at resource version" f" {fetched_resource_version}."
        )

        # Extract node data from the pod list.
        cloud_instances = {}
        for pod in pod_list["items"]:
            # Kubernetes sets metadata.deletionTimestamp immediately after admitting a
            # request to delete an object. Full removal of the object may take some time
            # after the deletion timestamp is set. See link for details:
            # https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-deletion
            if "deletionTimestamp" in pod["metadata"]:
                # Ignore pods marked for termination.
                continue
            cloud_instance = self._cloud_instance_from_pod(pod)
            if cloud_instance:
                cloud_instances[pod["metadata"]["name"]] = cloud_instance

        self._ippr_provider.sync_ippr_status_from_pods(pod_list["items"])

        return cloud_instances

    @staticmethod
    def _cloud_instance_from_pod(pod: Dict[str, Any]) -> Optional[CloudInstance]:
        """Convert a pod to a Ray CloudInstance.

        Args:
            pod: The pod resource dict.

        Returns:
            The CloudInstance representing the pod, or None if the pod is not a
            tracked Ray node (e.g. a redis-cleanup pod).
        """
        labels = pod["metadata"]["labels"]
        if labels[KUBERAY_LABEL_KEY_KIND] == KUBERAY_KIND_HEAD:
            kind = NodeKind.HEAD
            type = labels[KUBERAY_LABEL_KEY_TYPE]
        elif labels[KUBERAY_LABEL_KEY_KIND] == KUBERAY_KIND_WORKER:
            kind = NodeKind.WORKER
            type = labels[KUBERAY_LABEL_KEY_TYPE]
        else:
            # Other ray nodes types defined by KubeRay.
            # e.g. this could also be `redis-cleanup`
            # We will not track these nodes.
            return None

        # TODO: we should prob get from the pod's env var (RAY_CLOUD_INSTANCE_ID)
        # directly.
        cloud_instance_id = pod["metadata"]["name"]
        replica_id = (
            (labels.get(KUBERAY_LABEL_KEY_REPLICA_NAME) or None)
            if kind == NodeKind.WORKER
            else None
        )
        return CloudInstance(
            cloud_instance_id=cloud_instance_id,
            node_type=type,
            node_kind=kind,
            is_running=KubeRayProvider._is_running(pod),
            replica_id=replica_id,
        )

    @staticmethod
    def _is_running(pod) -> bool:
        """Convert pod state to Ray NodeStatus

        A cloud instance is considered running if the pod is in the running state,
        else it could be pending/containers-terminated.

        When it disappears from the list, it is considered terminated.
        """
        if (
            "containerStatuses" not in pod["status"]
            or not pod["status"]["containerStatuses"]
        ):
            return False

        state = pod["status"]["containerStatuses"][0]["state"]
        if "running" in state:
            return True

        return False

    def _get(self, remote_path: str) -> Dict[str, Any]:
        """Get a resource from the Kubernetes API server."""
        return self._k8s_api_client.get(remote_path)

    def _patch(self, remote_path: str, payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Patch a resource on the Kubernetes API server."""
        return self._k8s_api_client.patch(remote_path, payload)

    def _evaluate_idle_termination(self) -> None:
        """Apply idleTerminationOptions.policy once no driver held for the timeout.

        Detached actors do not count as a driver.
        """
        # Feature disabled or a driver is attached: reset the anchor.
        if self._idle_termination_timeout_seconds is None:
            self._no_driver_observed_since = None
            return
        has_active_driver, latest_job_end_time = self._driver_status()
        if has_active_driver:
            self._no_driver_observed_since = None
            return

        # A driver finished since the last check: it was attached during the
        # no-driver window, so restart the timer.
        if latest_job_end_time > self._last_seen_job_end_time:
            self._last_seen_job_end_time = latest_job_end_time
            self._no_driver_observed_since = None

        # Anchor on the first loop with no driver, then dispatch once the
        # no-driver window reaches the timeout.
        now = time.monotonic()
        if self._no_driver_observed_since is None:
            self._no_driver_observed_since = now
        if (
            now - self._no_driver_observed_since
            < self._idle_termination_timeout_seconds
        ):
            return
        self._apply_idle_termination_policy()

    def _driver_status(self) -> Tuple[bool, int]:
        """Returns whether a non-internal driver is alive and the latest job end time.

        Fails closed: a failed GCS query reports a driver as present.
        """
        try:
            jobs = self._gcs_client.get_all_job_info(
                skip_submission_job_info_field=True,
                skip_is_running_tasks_field=True,
            )
        except Exception:
            logger.exception(
                "Failed to query GCS job table; treating as drivers attached."
            )
            return True, self._last_seen_job_end_time

        has_active_driver = False
        latest_job_end_time = 0
        for job in jobs.values():
            # Ray-internal drivers (e.g. the dashboard) are not user activity.
            if job.config.ray_namespace.startswith(RAY_INTERNAL_NAMESPACE_PREFIX):
                continue
            if job.is_dead:
                latest_job_end_time = max(latest_job_end_time, job.end_time)
            else:
                has_active_driver = True
        return has_active_driver, latest_job_end_time

    def _apply_idle_termination_policy(self) -> None:
        """Applies the configured idle termination policy to the RayCluster CR."""
        if self._ray_cluster.get("metadata", {}).get("deletionTimestamp"):
            logger.info(f"RayCluster {self._cluster_name} is already being deleted.")
            return
        if self._idle_termination_policy == IDLE_TERMINATION_OPTIONS_POLICY_DELETE:
            self._delete_idle_ray_cluster()
        elif self._idle_termination_policy == IDLE_TERMINATION_OPTIONS_POLICY_SUSPEND:
            self._suspend_idle_ray_cluster()
        else:
            logger.warning(
                f"Unknown idleTerminationOptions.policy {self._idle_termination_policy!r}; taking no action."
            )

    def _delete_idle_ray_cluster(self) -> None:
        """Deletes an idle RayCluster by first appending IDLE_TERMINATION_CLEANUP_FINALIZER.
        Send a DELETE request to k8s apiserver if appending the finalizer is successful.
        """
        path = f"rayclusters/{self._cluster_name}"
        # Append the IDLE_TERMINATION_CLEANUP_FINALIZER finalizer
        # using a read-modify-write to preserve existing finalizers.
        finalizers = self._ray_cluster.get("metadata", {}).get("finalizers", [])
        if IDLE_TERMINATION_CLEANUP_FINALIZER not in finalizers:
            resource_version = self._ray_cluster.get("metadata", {}).get(
                "resourceVersion"
            )
            if resource_version is None:
                logger.error(
                    f"RayCluster {self._cluster_name} has no metadata.resourceVersion; "
                    "skipping idle termination delete."
                )
                return

            # metadata.finalizers is an array so we use a JSON Patch add-operation to append IDLE_TERMINATION_CLEANUP_FINALIZER
            payload = finalizer_patch(
                IDLE_TERMINATION_CLEANUP_FINALIZER, finalizers, resource_version
            )
            try:
                patched_raycluster = self._k8s_api_client.patch(
                    path, payload, content_type="application/json-patch+json"
                )

                if not isinstance(
                    patched_raycluster, dict
                ) or IDLE_TERMINATION_CLEANUP_FINALIZER not in patched_raycluster.get(
                    "metadata", {}
                ).get(
                    "finalizers", []
                ):
                    logger.error(
                        f"Unable to persist {IDLE_TERMINATION_CLEANUP_FINALIZER} to metadata.finalizers for {self._cluster_name}"
                    )
                    return

            except requests.HTTPError as e:
                if e.response.status_code == 422:
                    logger.warning(
                        f"Patch adding {IDLE_TERMINATION_CLEANUP_FINALIZER} to RayCluster {self._cluster_name} was rejected; "
                        f"will retry adding {IDLE_TERMINATION_CLEANUP_FINALIZER} in the next reconcile cycle."
                    )
                else:
                    logger.exception(
                        f"Failed to add {IDLE_TERMINATION_CLEANUP_FINALIZER} finalizer to {self._cluster_name}"
                    )
                return
            except Exception:
                logger.exception(
                    f"Failed to add {IDLE_TERMINATION_CLEANUP_FINALIZER} finalizer to {self._cluster_name}"
                )
                return

        try:
            # DELETE the idle RayCluster.
            self._k8s_api_client.delete(path)
            logger.info(f"Deleted {self._cluster_name}")
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                # HTTP status code 404 is treated as a successful delete.
                logger.info(f"{self._cluster_name} was already deleted.")
            else:
                logger.exception(f"Failed to delete {self._cluster_name}")
        except Exception:
            logger.exception(f"Failed to delete {self._cluster_name}")

    def _suspend_idle_ray_cluster(self) -> None:
        """Merge-Patch spec.idleSuspend=true for an idle RayCluster"""
        path = f"rayclusters/{self._cluster_name}"
        spec_idle_suspend = self._ray_cluster.get("spec", {}).get(IDLE_SUSPEND_KEY)
        if spec_idle_suspend:
            logger.info(f"spec.idleSuspend is already true in {self._cluster_name}")
            return

        # Merge-patch spec.idleSuspend=true when the policy is Suspend.
        payload = idle_suspend_patch(True)

        try:
            patched_raycluster = self._k8s_api_client.patch(
                path,
                payload,
                content_type="application/merge-patch+json",
            )
            if (
                not isinstance(patched_raycluster, dict)
                or patched_raycluster.get("spec", {}).get(IDLE_SUSPEND_KEY) is not True
            ):
                logger.error(
                    f"Unable to persist {IDLE_SUSPEND_KEY}=true for {self._cluster_name}"
                )
                return

        except Exception:
            logger.exception(
                f"Failed to MERGE-PATCH {IDLE_SUSPEND_KEY}=true on RayCluster {self._cluster_name}",
            )
            return

        logger.info(f"Set {IDLE_SUSPEND_KEY}=true on RayCluster {self._cluster_name}")

    def _get_head_pod_resource_version(self) -> str:
        """Extract a recent pods resource version by reading the head pod's
        metadata.resourceVersion of the response.
        """
        if not RAY_HEAD_POD_NAME:
            return None
        pod_resp = self._get(f"pods/{RAY_HEAD_POD_NAME}")
        return pod_resp["metadata"]["resourceVersion"]
