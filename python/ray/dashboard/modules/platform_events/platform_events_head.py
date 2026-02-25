import asyncio
import logging
import collections
import threading
import time
import os
from typing import Optional, Set

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils

try:
    from kubernetes import client, config as k8s_config, watch
    from kubernetes.client.rest import ApiException
except ImportError:
    client = None
    k8s_config = None
    watch = None
    ApiException = None

from ray.core.generated.platform_event_pb2 import PlatformEvent, Source
from ray.core.generated.events_base_event_pb2 import RayEvent

logger = logging.getLogger(__name__)

routes = dashboard_optional_utils.DashboardHeadRouteTable
logger.info(f"PlatformEventsHead using DashboardHeadRouteTable id: {id(routes)}")

# Max number of events to cache in memory.
MAX_EVENTS_TO_CACHE = 1000
# Interval for refreshing Ray pod list (seconds)
POD_REFRESH_INTERVAL_SECONDS = 60

class PlatformEventsHead(dashboard_utils.DashboardHeadModule):

    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)
        self._events = collections.deque(maxlen=MAX_EVENTS_TO_CACHE)
        self._event_uids = set()
        self._cluster_name = None
        self._ray_job_name = None
        self._ray_service_name = None
        self._last_resource_versions = {}  # Dict of target_id -> resource_version
        self._k8s_v1_api = None
        self._stop_event = threading.Event()
        # We don't need a lock because we'll update _events on the main loop
        self._loop = None
        # Set of Ray pod names for filtering Pod events
        self._ray_pod_names: Set[str] = set()
        self._ray_pod_names_lock = threading.Lock()

    async def _init_k8s_client(self):
        if not k8s_config:
            logger.info("Kubernetes library not found. Platform events will be disabled.")
            return False
            
        if self._k8s_v1_api:
            return True

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
                    "Platform events will not be available."
                )
                return False

        self._k8s_v1_api = client.CoreV1Api()
        logger.info("Kubernetes client initialized.")
        
        # Discovery: Find the cluster name this pod belongs to via environment variables
        self._cluster_name = os.environ.get("RAY_CLUSTER_NAME")
        if self._cluster_name:
            logger.info(f"Discovered RayCluster name: {self._cluster_name}")
            # Discovery: Find parent RayJob or RayService
            try:
                namespace = os.environ.get("RAY_CLUSTER_NAMESPACE", "default")
                # Check RayCluster resource (labels and ownerReferences)
                try:
                    custom_api = client.CustomObjectsApi()
                    cluster_obj = custom_api.get_namespaced_custom_object(
                        group="ray.io",
                        version="v1",
                        namespace=namespace,
                        plural="rayclusters",
                        name=self._cluster_name,
                        _request_timeout=30,
                    )
                    c_metadata = cluster_obj.get("metadata", {})
                    c_labels = c_metadata.get("labels", {})
                    
                    # Check Labels (KubeRay standard)
                    orig_name = c_labels.get("ray.io/originated-from-cr-name")
                    orig_kind = c_labels.get("ray.io/originated-from-crd")
                    
                    if orig_kind == "RayJob":
                        self._ray_job_name = orig_name
                    elif orig_kind == "RayService":
                        self._ray_service_name = orig_name
                        
                    # Check OwnerReferences as a fallback (Very robust)
                    if not self._ray_job_name and not self._ray_service_name:
                        owner_refs = c_metadata.get("ownerReferences", [])
                        for ref in owner_refs:
                            if ref.get("kind") == "RayJob":
                                self._ray_job_name = ref.get("name")
                                break
                            elif ref.get("kind") == "RayService":
                                self._ray_service_name = ref.get("name")
                                break
                                
                except Exception as ce:
                    logger.info(f"Could not read RayCluster labels/owners for discovery: {ce}")

                if self._ray_job_name:
                    logger.info(f"Discovered parent RayJob: {self._ray_job_name}")
                if self._ray_service_name:
                    logger.info(f"Discovered parent RayService: {self._ray_service_name}")
            except Exception as e:
                logger.info(f"Failed to discover parent RayJob/RayService: {e}")
        else:
            logger.info("RAY_CLUSTER_NAME not found. Platform events will pull all cluster events in the namespace.")

        return True

    def _refresh_ray_pods(self):
        """Refresh the set of Ray pod names belonging to this cluster."""
        if not self._k8s_v1_api or not self._cluster_name:
            return
        
        namespace = os.environ.get("RAY_CLUSTER_NAMESPACE", "default")
        try:
            # KubeRay labels all Ray pods with ray.io/cluster=<cluster-name>
            label_selector = f"ray.io/cluster={self._cluster_name}"
            pods = self._k8s_v1_api.list_namespaced_pod(
                namespace=namespace,
                label_selector=label_selector,
                _request_timeout=30,  # Timeout after 30 seconds
            )
            
            new_pod_names = {pod.metadata.name for pod in pods.items}
            
            with self._ray_pod_names_lock:
                added = new_pod_names - self._ray_pod_names
                removed = self._ray_pod_names - new_pod_names
                self._ray_pod_names = new_pod_names
            
            if added or removed:
                logger.debug(
                    f"Ray pods updated: +{len(added)} -{len(removed)}, "
                    f"total={len(new_pod_names)}"
                )
        except Exception as e:
            logger.warning(f"Failed to refresh Ray pod list: {e}")

    def _run_pod_refresh_loop(self):
        """Background thread to periodically refresh Ray pod names."""
        logger.info("Starting Ray pod refresh loop")
        while not self._stop_event.is_set():
            self._refresh_ray_pods()
            # Wait for interval or stop event
            self._stop_event.wait(POD_REFRESH_INTERVAL_SECONDS)
        logger.info("Ray pod refresh loop stopped")

    def _is_relevant_pod(self, pod_name: str) -> bool:
        """Check if a pod belongs to our Ray cluster."""
        with self._ray_pod_names_lock:
            return pod_name in self._ray_pod_names

    def _run_k8s_watch(self, kind: str, name: Optional[str]):
        """Blocking method to run in a separate thread."""
        target_id = f"{kind}/{name}" if name else kind
        logger.info(f"Starting Platform event watcher thread for {target_id}")
        if not self._k8s_v1_api or not watch:
            return

        w = watch.Watch()
        
        # Find namespace
        namespace = os.environ.get("RAY_CLUSTER_NAMESPACE", "default")
            
        while not self._stop_event.is_set():
            try:
                # Field selector for specific target
                field_selector = f"involvedObject.kind={kind}"
                if name:
                    field_selector += f",involvedObject.name={name}"

                # This blocks waiting for events
                stream = w.stream(
                    self._k8s_v1_api.list_namespaced_event,
                    namespace=namespace,
                    field_selector=field_selector,
                    resource_version=self._last_resource_versions.get(target_id),
                    timeout_seconds=60,
                    _request_timeout=70,  # Slightly longer than server timeout
                )
                
                for event in stream:
                    k8s_event_obj = event['object']
                    event_type = event['type']
                    
                    # Update resource version for next resume
                    self._last_resource_versions[target_id] = k8s_event_obj.metadata.resource_version
                    
                    # Use call_soon_threadsafe to process in main loop
                    if self._loop:
                         self._loop.call_soon_threadsafe(
                             self._process_k8s_event_callback, k8s_event_obj, event_type
                         )
                    
            except ApiException as e:
                if e.status == 410:
                    # Resource version too old, reset and retry
                    logger.warning(f"Resource version expired for {target_id}, resetting watch")
                    self._last_resource_versions.pop(target_id, None)
                    continue
                if self._stop_event.is_set():
                    break
                logger.error(f"K8s API error watching events for {target_id}: {e}")
                self._stop_event.wait(5)
            except Exception as e:
                if self._stop_event.is_set():
                    break
                logger.error(f"Error watching Platform events for {target_id}: {e}")
                # Wait for backoff or stop
                self._stop_event.wait(5)

    def _process_k8s_event_callback(self, event_obj, event_type):
        """Callback running in the main loop."""
        involved_object = event_obj.involved_object
        
        # Filter Pod events to only include Ray pods
        if involved_object.kind == "Pod":
            if not self._is_relevant_pod(involved_object.name):
                return
        
        # Deduping
        uid = event_obj.metadata.uid
        if uid in self._event_uids:
            return
        
        # Create Protocol Buffer message
        source_proto = Source(
            platform="KUBERNETES",
            component=event_obj.source.component if event_obj.source else "k8s",
            metadata={"namespace": event_obj.metadata.namespace or "default"}
        )
        
        # Get timestamp from last_timestamp, first_timestamp, or current time
        event_timestamp = event_obj.last_timestamp or event_obj.first_timestamp
        # Convert to nanoseconds
        timestamp_ns = int(event_timestamp.timestamp() * 1e9) if event_timestamp else int(time.time() * 1e9)
        
        proto_platform_event = PlatformEvent(
            source=source_proto,
            object_kind=involved_object.kind,
            object_name=involved_object.name,
            message=event_obj.message or "",
            reason=event_obj.reason or "",
        )

        # Wrap in RayEvent for unified framework integration
        from google.protobuf.timestamp_pb2 import Timestamp
        google_timestamp = Timestamp()
        if event_timestamp:
            google_timestamp.FromDatetime(event_timestamp)
        else:
            google_timestamp.GetCurrentTime()

        ray_event = RayEvent(
            event_id=event_obj.metadata.uid.encode(),
            source_type=RayEvent.SourceType.CLUSTER_LIFECYCLE,
            event_type=RayEvent.EventType.PLATFORM_EVENT,
            timestamp=google_timestamp,
            severity=RayEvent.Severity.WARNING if event_type == "Warning" else RayEvent.Severity.INFO,
            message=event_obj.message or "",
            platform_event=proto_platform_event,
        )
        
        self._events.append(ray_event)
        self._event_uids.add(uid)
        
        # Prune UID set and kinds dict to keep memory usage stable
        if len(self._event_uids) > MAX_EVENTS_TO_CACHE * 2:
            # Simple approach: rebuild from current events. More robust would be LRU if needed.
            self._event_uids.clear()
            for e in self._events:
                self._event_uids.add(e.event_id)

    @routes.get("/api/v0/platform_events")
    async def get_platform_events(self, req):
        # Access _events directly as we are on the main loop
        events_list = list(self._events)
        
        data = {
            "events": [
                {
                    "source_event_id": e.event_id.decode("utf-8"),
                    "severity": RayEvent.Severity.Name(e.severity),
                    "source": {
                        "platform": e.platform_event.source.platform,
                        "component": e.platform_event.source.component,
                        "metadata": dict(e.platform_event.source.metadata),
                    },
                    "object_kind": e.platform_event.object_kind,
                    "object_name": e.platform_event.object_name,
                    "message": e.platform_event.message,
                    "reason": e.platform_event.reason,
                    "timestamp_ns": e.timestamp.seconds * 10**9 + e.timestamp.nanos,
                } for e in events_list
            ],
        }
        
        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="Retrieved platform events",
            **data
        )

    @staticmethod
    def is_minimal_module():
        return False

    async def run(self):
        self._loop = asyncio.get_running_loop()
        if await self._init_k8s_client():
            # Targets to watch
            targets = [("RayCluster", self._cluster_name)]
            if self._ray_job_name:
                targets.append(("RayJob", self._ray_job_name))
            if self._ray_service_name:
                targets.append(("RayService", self._ray_service_name))
            
            # Add Pod watcher if we have a cluster name for filtering
            if self._cluster_name:
                # Initial pod discovery before starting watcher
                self._refresh_ray_pods()
                targets.append(("Pod", None))  # Watch all Pod events, filter client-side
                
                # Start background thread to refresh pod list periodically
                pod_refresh_thread = threading.Thread(
                    target=self._run_pod_refresh_loop, daemon=True
                )
                pod_refresh_thread.start()
            
            # Run watchers in separate threads to avoid blocking the asyncio loop
            for kind, name in targets:
                t = threading.Thread(target=self._run_k8s_watch, args=(kind, name), daemon=True)
                t.start()
            
    async def cleanup(self):
        self._stop_event.set()
        # Note: we don't join the thread here because it's daemon=True 
        # and might be blocked in K8s API call. 
        # Setting the event will stop it on next retry/timeout.
        logger.info("Platform events watcher stopped.")
