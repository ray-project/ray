import asyncio
import logging
import os
import threading
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional, Set

from google.protobuf.timestamp_pb2 import Timestamp

from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.core.generated.platform_event_pb2 import PlatformEvent, Source
from ray.dashboard.modules.platform_events.providers.base import PlatformEventProvider

try:
    from kubernetes import client, config as k8s_config, watch
    from kubernetes.client.rest import ApiException
except ImportError:
    client = None
    k8s_config = None
    watch = None
    ApiException = None

logger = logging.getLogger(__name__)

POD_MEMBERSHIP_WATCH_ID = "Pod/__cluster_members__"
POD_EVENTS_WATCH_ID = "Event/__cluster_pods__"
# Cap on the number of non-cluster pod names cached by `resolve_cluster_pod`
# to bound memory in busy multi-tenant namespaces. The oldest entry is evicted
# (FIFO) once it grows past this size.
MAX_NON_CLUSTER_POD_CACHE = 1024
# Key for KubeRay label that identifies the RayCluster name of a resource.
KUBERAY_LABEL_KEY_CLUSTER = "ray.io/cluster"


class KubernetesEventProvider(PlatformEventProvider):
    """
    Kubernetes-specific implementation of PlatformEventProvider.

    Watches the Kubernetes Event API for events related to the active RayCluster
    and its parent owners (such as RayJob or RayService). It parses raw K8s event
    objects, maps them to standardized, platform-agnostic RayEvent protobuf
    objects, and delivers them to the manager's callback. The callback may be
    invoked from any number of watcher threads, so consumers must pass a
    thread-safe callback.
    """

    def __init__(self, callback: Callable[[RayEvent], None]):
        super().__init__(callback)
        self._cluster_name: Optional[str] = None
        self._ray_job_name: Optional[str] = None
        self._ray_service_name: Optional[str] = None
        self._namespace: Optional[str] = None

        # Dict mapping "kind/name" target IDs to their last seen Kubernetes resource versions
        self._last_resource_versions: Dict[str, Optional[str]] = {}

        # The Kubernetes CoreV1Api client, populated in `_init_k8s_client`
        self._k8s_v1_api: Optional[Any] = None

        # Lifecycle and thread synchronization controls
        self._stop_event: threading.Event = threading.Event()
        self._async_stop_event: asyncio.Event = asyncio.Event()

        # Dict mapping "kind/name" target IDs to active kubernetes.watch.Watch instances
        self._active_watches: Dict[str, Any] = {}
        self._watches_lock: threading.Lock = threading.Lock()
        self._threads: List[threading.Thread] = []
        self._cleaned_up: bool = False

        # Set of pod names belonging to this RayCluster (label ray.io/cluster=<name>).
        # Maintained by `_run_pod_membership_watch` and read by `_run_pod_events_watch`
        # to filter namespace-wide pod events down to this cluster's pods.
        self._cluster_pod_names: Set[str] = set()
        # Set of pod names resolved to NOT belong to this cluster, so the
        # events watcher doesn't re-resolve them on every event. Always checked after
        # `_cluster_pod_names`, so it can never override real membership.
        self._non_cluster_pod_names: "OrderedDict[str, None]" = OrderedDict()
        self._pods_lock: threading.Lock = threading.Lock()

    async def _init_k8s_client(self) -> bool:
        if not k8s_config:
            logger.info(
                "Kubernetes dependencies not found. Kubernetes events will be disabled."
            )
            return False

        # RAY_CLUSTER_NAME is set by KubeRay Operator on every Ray cluster.
        cluster_name = os.environ.get("RAY_CLUSTER_NAME")
        if not cluster_name:
            logger.warning(
                "RAY_CLUSTER_NAME not found. Kubernetes events will not be available."
            )
            return False

        # RAY_CLUSTER_NAMESPACE is set by KubeRay Operator on every Ray cluster pod.
        cluster_namespace = os.environ.get("RAY_CLUSTER_NAMESPACE")
        if not cluster_namespace:
            logger.warning(
                "RAY_CLUSTER_NAMESPACE not found. Kubernetes events will not be available."
            )
            return False

        self._namespace = cluster_namespace

        try:
            # Try in-cluster config first
            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            try:
                # Fallback to kubeconfig (local dev)
                k8s_config.load_kube_config()
            except k8s_config.ConfigException:
                logger.warning(
                    "Kubernetes client not installed or configuration failed. "
                    "Kubernetes events will not be available."
                )
                return False

        self._k8s_v1_api = client.CoreV1Api()
        logger.info("Kubernetes client initialized.")

        self._cluster_name = cluster_name
        logger.info(
            f"Discovered RayCluster name: {self._cluster_name} in namespace: {self._namespace}"
        )
        try:
            custom_api = client.CustomObjectsApi()
            loop = asyncio.get_running_loop()
            cluster_obj = await loop.run_in_executor(
                None,
                lambda: custom_api.get_namespaced_custom_object(
                    group="ray.io",
                    version="v1",
                    namespace=self._namespace,
                    plural="rayclusters",
                    name=self._cluster_name,
                    _request_timeout=30,
                ),
            )
            c_metadata = cluster_obj.get("metadata", {})
            owner_refs = c_metadata.get("ownerReferences", [])
            for ref in owner_refs:
                if ref.get("kind") == "RayJob":
                    self._ray_job_name = ref.get("name")
                    break
                elif ref.get("kind") == "RayService":
                    self._ray_service_name = ref.get("name")
                    break
        except Exception as ce:
            logger.info(f"Could not read RayCluster owners for discovery: {ce}")
        if self._ray_job_name:
            logger.info(f"Discovered parent RayJob: {self._ray_job_name}")
        if self._ray_service_name:
            logger.info(f"Discovered parent RayService: {self._ray_service_name}")

        return True

    async def run(self) -> None:
        if await self._init_k8s_client():
            # Targets to watch
            targets = [("RayCluster", self._cluster_name)]
            if self._ray_job_name:
                targets.append(("RayJob", self._ray_job_name))
            if self._ray_service_name:
                targets.append(("RayService", self._ray_service_name))

            # Pre-initialize keys to avoid race conditions during dict resizing in threads
            for kind, name in targets:
                target_id = f"{kind}/{name}"
                self._last_resource_versions[target_id] = None
            self._last_resource_versions[POD_EVENTS_WATCH_ID] = None

            # Atomically list and build the pod membership cache synchronously
            # on startup before starting any event watcher threads.
            label_selector = f"{KUBERAY_LABEL_KEY_CLUSTER}={self._cluster_name}"
            loop = asyncio.get_running_loop()
            pods_rv = await loop.run_in_executor(
                None, self._relist_pod_membership, label_selector
            )

            # Use the list's resource version so the watch resumes exactly where it left off
            if pods_rv is not None:
                self._last_resource_versions[POD_MEMBERSHIP_WATCH_ID] = pods_rv
            else:
                logger.warning(
                    "Initial pod membership LIST failed, pod events watcher may drop events until next list."
                )

            # Seed the pod-events watch from the EVENT collection's resource version (a separate LIST).
            # Resourceversions are per resource-type so we can't rely on the pod LIST's resource version.
            # Starting from NONE instead would replay all historical events in the namespace (a potential
            # flood), so we only fallback to that if LIST fails.
            events_rv = await loop.run_in_executor(
                None, self._latest_event_resource_version
            )
            if events_rv is not None:
                self._last_resource_versions[POD_EVENTS_WATCH_ID] = events_rv
            else:
                logger.warning(
                    "Initial events LIST failed, pod-events watcher may miss some historical events until the next watch reset."
                )

            try:
                # Start a dedicated, named OS thread for each target to ensure strict execution guarantees
                for kind, name in targets:
                    t = threading.Thread(
                        target=self._run_kuberay_resource_events_watch,
                        args=(kind, name),
                        name=f"kubernetes_events_watch_{kind}_{name}",
                        daemon=True,
                    )
                    t.start()
                    self._threads.append(t)

                # Pod-membership watcher: starts from the RV we just fetched
                pod_thread = threading.Thread(
                    target=self._run_pod_membership_watch,
                    name="kubernetes_events_watch_pods",
                    daemon=True,
                )
                pod_thread.start()
                self._threads.append(pod_thread)

                # Pod-events watcher: namespace-wide event watch filtered
                # client-side against the membership cache.
                pod_events_thread = threading.Thread(
                    target=self._run_pod_events_watch,
                    name="kubernetes_events_watch_pod_events",
                    daemon=True,
                )
                pod_events_thread.start()
                self._threads.append(pod_events_thread)

                # Block the run coroutine until cleanup is called
                await self._async_stop_event.wait()
            finally:
                await self.cleanup()

    def _run_kuberay_resource_events_watch(self, kind: str, name: str) -> None:
        """Watch K8s events for a single named KubeRay CRD instance.
        `kind` is one of 'RayCluster', 'RayJob', or 'RayService'
        `name` is its metadata.name. Uses a field-selected event watch
        scoped to that exact object.
        """
        if not name:
            logger.error(
                f"Attempted to watch events for kind {kind} without a specific name. Aborting."
            )
            return

        target_id = f"{kind}/{name}"
        logger.info(f"Starting Kubernetes event watcher thread for {target_id}")
        if not self._k8s_v1_api or not watch:
            logger.warning(
                f"Halting Kubernetes event watcher for {target_id}: "
                "Kubernetes client or watch library is not initialized."
            )
            return

        w = watch.Watch()
        with self._watches_lock:
            self._active_watches[target_id] = w
        try:
            while not self._stop_event.is_set():
                try:
                    stream = w.stream(
                        self._k8s_v1_api.list_namespaced_event,
                        namespace=self._namespace,
                        field_selector=(
                            f"involvedObject.kind={kind},involvedObject.name={name}"
                        ),
                        resource_version=self._last_resource_versions.get(target_id),
                        timeout_seconds=60,
                        _request_timeout=70,
                    )

                    for event in stream:
                        if self._is_watch_error_event(event, target_id):
                            self._last_resource_versions[target_id] = None
                            break
                        k8s_event_obj = event["object"]
                        rv = k8s_event_obj.metadata.resource_version
                        self._last_resource_versions[target_id] = rv
                        self._process_k8s_event(k8s_event_obj)

                except ApiException as e:
                    if e.status == 410:
                        logger.warning(
                            f"Resource version expired for {target_id}, resetting watch"
                        )
                        self._last_resource_versions[target_id] = None
                    else:
                        logger.error(
                            f"Kubernetes API error watching events for {target_id}: {e}"
                        )
                        self._stop_event.wait(5)
                except Exception as e:
                    logger.error(
                        f"Error watching Kubernetes events for {target_id}: {e}"
                    )
                    self._stop_event.wait(5)
        finally:
            with self._watches_lock:
                self._active_watches.pop(target_id, None)

    def _relist_pod_membership(self, label_selector: str) -> Optional[str]:
        """LIST pods matching the cluster label and atomically rebuild the
        membership cache. Returns the LIST's resourceversion (to start
        watching from), or None on failure.
        """
        if not self._k8s_v1_api or not watch:
            return None
        try:
            pods = self._k8s_v1_api.list_namespaced_pod(
                namespace=self._namespace,
                label_selector=label_selector,
                # resource version = "0" serves this LIST from the apiserver's
                # watch cache instead of a quorum read against etcd.
                resource_version="0",
                _request_timeout=30,
            )
        except Exception as e:
            logger.error(f"Failed to list pods for membership cache: {e}")
            return None
        names = {p.metadata.name for p in pods.items}
        with self._pods_lock:
            self._cluster_pod_names = names
            return pods.metadata.resource_version

    def _run_pod_membership_watch(self) -> None:
        """Maintain the cluster's pod-name set via a label-selected pod watch.
        Filtered by `ray.io/cluster=<cluster_name>`."""

        if not self._k8s_v1_api or not watch:
            logger.warning(
                "Halting pod-membership watcher: "
                "Kubernetes client or watch library is not initialized."
            )
            return

        label_selector = f"{KUBERAY_LABEL_KEY_CLUSTER}={self._cluster_name}"
        logger.info(
            f"Starting Kubernetes pod-membership watcher (selector: {label_selector})"
        )

        w = watch.Watch()
        target_id = POD_MEMBERSHIP_WATCH_ID
        with self._watches_lock:
            self._active_watches[target_id] = w
        try:
            while not self._stop_event.is_set():
                if self._last_resource_versions.get(target_id) is None:
                    rv = self._relist_pod_membership(label_selector)
                    if rv is None:
                        self._stop_event.wait(1)
                        continue
                    self._last_resource_versions[target_id] = rv

                try:
                    stream = w.stream(
                        self._k8s_v1_api.list_namespaced_pod,
                        namespace=self._namespace,
                        label_selector=label_selector,
                        resource_version=self._last_resource_versions.get(target_id),
                        timeout_seconds=60,
                        _request_timeout=70,
                    )
                    for event in stream:
                        if self._is_watch_error_event(event, target_id):
                            self._last_resource_versions[target_id] = None
                            break
                        pod_obj = event["object"]
                        self._last_resource_versions[
                            target_id
                        ] = pod_obj.metadata.resource_version
                        self._update_pod_membership(
                            event["type"], pod_obj.metadata.name
                        )
                except ApiException as e:
                    if e.status == 410:
                        logger.warning(
                            "Resource version expired for pod-membership watch, "
                            "will re-list pods and resume"
                        )
                        self._last_resource_versions[target_id] = None
                    else:
                        logger.error(f"Kubernetes API error watching pods: {e}")
                        self._stop_event.wait(5)
                except Exception as e:
                    logger.error(f"Error watching Kubernetes pods: {e}")
                    self._stop_event.wait(5)
        finally:
            with self._watches_lock:
                self._active_watches.pop(target_id, None)

    def _update_pod_membership(self, event_type: str, pod_name: str) -> None:
        """Apply a single pod watch event to the membership cache."""
        with self._pods_lock:
            if event_type == "DELETED":
                self._cluster_pod_names.discard(pod_name)
            else:
                self._cluster_pod_names.add(pod_name)
                self._non_cluster_pod_names.pop(pod_name, None)

    def _run_pod_events_watch(self) -> None:
        """Watch namespace-wide pod events, dropping events for non-ray-cluster pods.

        K8s field selectors don't support set membership across pod names,
        so we open a single watch scoped to `involvedObject.kind=Pod` and
        filter client-side via `_resolve_cluster_pod`.
        """
        if not self._k8s_v1_api or not watch:
            logger.warning(
                "Halting pod-events watcher: "
                "Kubernetes client or watch library is not initialized."
            )
            return

        logger.info("Starting Kubernetes pod-events watcher")

        w = watch.Watch()
        target_id = POD_EVENTS_WATCH_ID
        with self._watches_lock:
            self._active_watches[target_id] = w
        try:
            while not self._stop_event.is_set():
                try:
                    stream = w.stream(
                        self._k8s_v1_api.list_namespaced_event,
                        namespace=self._namespace,
                        field_selector="involvedObject.kind=Pod",
                        resource_version=self._last_resource_versions.get(target_id),
                        timeout_seconds=60,
                        _request_timeout=70,
                    )
                    for event in stream:
                        if self._is_watch_error_event(event, target_id):
                            self._last_resource_versions[target_id] = None
                            break
                        k8s_event_obj = event["object"]
                        self._last_resource_versions[
                            target_id
                        ] = k8s_event_obj.metadata.resource_version
                        if self._resolve_cluster_pod(
                            k8s_event_obj.involved_object.name
                        ):
                            self._process_k8s_event(k8s_event_obj)
                except ApiException as e:
                    if e.status == 410:
                        logger.warning(
                            "Resource version expired for pod-events watch, "
                            "re-listing to resume"
                        )
                        self._last_resource_versions[target_id] = None
                    else:
                        logger.error(f"Kubernetes API error watching pod events: {e}")
                        self._stop_event.wait(5)
                except Exception as e:
                    logger.error(f"Error watching Kubernetes pod events: {e}")
                    self._stop_event.wait(5)
        finally:
            with self._watches_lock:
                self._active_watches.pop(target_id, None)

    def _is_watch_error_event(self, event, target_id):
        if event.get("type") == "ERROR":
            logger.warning(
                f"Error watching {target_id}, re-esablishing stream: "
                f"{event.get('raw_object', event.get('object'))}"
            )
            return True
        return False

    def _latest_event_resource_version(self) -> Optional[str]:
        """Return the latest event resource version of the namespace's
        event collection via a minimal LIST or None on failure."""
        if not self._k8s_v1_api:
            return None
        try:
            events = self._k8s_v1_api.list_namespaced_event(
                namespace=self._namespace,
                field_selector="involvedObject.kind=Pod",
                limit=1,
                # resource version = "0" serves this LIST from the apiserver's
                # watch cache instead of a quorum read against etcd.
                resource_version="0",
                _request_timeout=30,
            )
            return events.metadata.resource_version
        except ApiException as e:
            logger.error(
                f"Kubernetes API error fetching latest event resource version: {e}"
            )
            return None

    def _resolve_cluster_pod(self, pod_name: str) -> bool:
        """Return whether pod_name belongs to this cluster."""
        with self._pods_lock:
            if pod_name in self._cluster_pod_names:
                return True
            if pod_name in self._non_cluster_pod_names:
                return False
        is_member = self._lookup_pod_is_member(pod_name)
        if is_member is None:
            return False

        with self._pods_lock:
            if is_member:
                self._cluster_pod_names.add(pod_name)
            else:
                while len(self._non_cluster_pod_names) >= MAX_NON_CLUSTER_POD_CACHE:
                    self._non_cluster_pod_names.popitem(last=False)
                self._non_cluster_pod_names[pod_name] = None
        return is_member

    def _lookup_pod_is_member(self, pod_name: str) -> Optional[bool]:
        """GET a single pod to authoritatively determine cluster membership."""
        if not self._k8s_v1_api:
            return None
        try:
            pod = self._k8s_v1_api.read_namespaced_pod(
                name=pod_name, namespace=self._namespace, _request_timeout=30
            )
        except ApiException as e:
            if e.status == 404:
                return None
            logger.error(f"Failed to resolve membership for pod {pod_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error looking up pod {pod_name}: {e}")
            return None
        labels = pod.metadata.labels or {}
        return labels.get(KUBERAY_LABEL_KEY_CLUSTER) == self._cluster_name

    def _process_k8s_event(self, event_obj: Any) -> None:
        """Parse a K8s event into a RayEvent proto and deliver it via the
        consumer callback. Invoked from a watcher worker thread."""
        involved_object = event_obj.involved_object
        uid = event_obj.metadata.uid

        event_timestamp = (
            event_obj.last_timestamp
            or event_obj.first_timestamp
            or event_obj.event_time
        )
        timestamp = Timestamp()
        if event_timestamp:
            timestamp.FromDatetime(event_timestamp)
        else:
            logger.warning(
                f"Kubernetes event {uid} involvedObject={involved_object.kind}/{involved_object.name} "
                "is missing both last_timestamp and first_timestamp. Leaving event timestamp unpopulated."
            )

        k8s_event_type = event_obj.type or "Normal"
        severity = (
            RayEvent.Severity.WARNING
            if k8s_event_type == "Warning"
            else RayEvent.Severity.INFO
        )

        source_metadata = {"namespace": event_obj.metadata.namespace or self._namespace}
        if self._cluster_name:
            source_metadata["ray_cluster_name"] = self._cluster_name

        source_proto = Source(
            platform=Source.Platform.KUBERNETES,
            component=event_obj.source.component if event_obj.source else "",
            metadata=source_metadata,
        )

        custom_fields = {}
        if event_obj.count and event_obj.count > 1:
            custom_fields["count"] = str(event_obj.count)

        platform_event = PlatformEvent(
            source=source_proto,
            object_kind=involved_object.kind,
            object_name=involved_object.name,
            message=event_obj.message or "",
            reason=event_obj.reason or "",
            custom_fields=custom_fields,
        )

        ray_event = RayEvent(
            event_id=uid.encode(),
            source_type=RayEvent.SourceType.CLUSTER_LIFECYCLE,
            event_type=RayEvent.EventType.PLATFORM_EVENT,
            timestamp=timestamp,
            severity=severity,
            message=event_obj.message or "",
            platform_event=platform_event,
        )

        # Deliver the parsed RayEvent to the central callback
        self._callback(ray_event)

    async def cleanup(self) -> None:
        if self._cleaned_up:
            return
        self._cleaned_up = True

        self._stop_event.set()
        self._async_stop_event.set()

        with self._watches_lock:
            watches = list(self._active_watches.values())
        for w in watches:
            try:
                w.stop()
            except Exception as e:
                logger.warning(f"Error stopping watch: {e}")

        loop = asyncio.get_running_loop()

        def join_all_threads():
            for t in self._threads:
                try:
                    t.join(timeout=1.0)
                except Exception as e:
                    logger.warning(f"Error joining thread {t.name}: {e}")

        await loop.run_in_executor(None, join_all_threads)
        logger.info("Kubernetes events watcher stopped.")
