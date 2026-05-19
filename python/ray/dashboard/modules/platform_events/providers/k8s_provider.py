import asyncio
import logging
import os
import threading
from typing import Any, Callable, Dict, List, Optional

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

            try:
                # Start a dedicated, named OS thread for each target to ensure strict execution guarantees
                for kind, name in targets:
                    t = threading.Thread(
                        target=self._run_k8s_watch,
                        args=(kind, name),
                        name=f"platform_events_watch_{kind}_{name}",
                        daemon=True,
                    )
                    t.start()
                    self._threads.append(t)

                # Block the run coroutine until cleanup is called
                await self._async_stop_event.wait()
            finally:
                await self.cleanup()

    def _run_k8s_watch(self, kind: str, name: str) -> None:
        """Blocking method to run in a separate thread."""
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

    def _process_k8s_event(self, event_obj: Any) -> None:
        """Parse a K8s event into a RayEvent proto and deliver it via the
        consumer callback. Invoked from a watcher worker thread."""
        involved_object = event_obj.involved_object
        uid = event_obj.metadata.uid

        event_timestamp = event_obj.last_timestamp or event_obj.first_timestamp
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
