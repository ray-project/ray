import asyncio
import logging
import collections
import threading
import time
from typing import Optional

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils

try:
    from kubernetes import client, config, watch
    from kubernetes.client.rest import ApiException
except ImportError:
    client = None
    config = None
    watch = None
    ApiException = None

from ray.dashboard.modules.k8s_events.k8s_events_pb2 import K8sEvent

logger = logging.getLogger(__name__)

routes = dashboard_optional_utils.DashboardHeadRouteTable
logger.info(f"K8sEventsHead using DashboardHeadRouteTable id: {id(routes)}")

# Max number of events to cache in memory.
MAX_EVENTS_TO_CACHE = 1000

class K8sEventsHead(dashboard_utils.DashboardHeadModule):

    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)
        self._events = collections.deque(maxlen=MAX_EVENTS_TO_CACHE)
        self._k8s_v1_api = None
        # We don't need a lock because we'll update _events on the main loop
        self._loop = None

    async def _init_k8s_client(self):
        if self._k8s_v1_api:
            return True

        try:
            # Try in-cluster config first
            config.load_incluster_config()
        except config.ConfigException:
            try:
                # Fallback to kubeconfig (local dev)
                config.load_kube_config()
            except config.ConfigException:
                logger.warning(
                    "Kubernetes client not installed or configuration failed. "
                    "K8s events will not be available."
                )
                return False

        self._k8s_v1_api = client.CoreV1Api()
        logger.info("Kubernetes client initialized.")
        return True

    def _run_k8s_watch(self):
        """Blocking method to run in a separate thread."""
        logger.info("Starting K8s event watcher thread.")
        if not self._k8s_v1_api:
            return

        w = watch.Watch()
        
        # Determine namespace
        namespace = "default"
        try:
            with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
                namespace = f.read().strip()
        except FileNotFoundError:
            pass 
            
        while True:
            try:
                # This blocks waiting for events
                stream = w.stream(
                    self._k8s_v1_api.list_namespaced_event,
                    namespace=namespace,
                    timeout_seconds=60
                )
                
                for event in stream:
                    k8s_event_obj = event['object']
                    event_type = event['type']
                    # Use call_soon_threadsafe to process in main loop
                    if self._loop:
                         self._loop.call_soon_threadsafe(
                             self._process_k8s_event_callback, k8s_event_obj, event_type
                         )
                    
            except Exception as e:
                logger.error(f"Error watching K8s events: {e}")
                time.sleep(5) # Backoff

    def _process_k8s_event_callback(self, event_obj, event_type):
        """Callback running in the main loop."""
        involved_object = event_obj.involved_object
        
        # Filter for Pod and RayCluster events
        if involved_object.kind not in ['Pod', 'RayCluster']:
            return

        logger.debug(f"Received K8s event: {event_type} {involved_object.kind} {involved_object.name}")

        # Create Protocol Buffer message
        proto_event = K8sEvent(
            event_id=event_obj.metadata.uid,
            source=event_obj.source.component if event_obj.source else "k8s",
            severity=K8sEvent.Severity.WARNING if event_type == "Warning" else K8sEvent.Severity.INFO,
            message=event_obj.message or "",
            reason=event_obj.reason or "",
            timestamp_ms=int(event_obj.last_timestamp.timestamp() * 1000) if event_obj.last_timestamp else int(time.time() * 1000),
            related_node_id=involved_object.name, # Heuristic
        )
        
        self._events.append(proto_event)

    @routes.get("/api/v0/k8s_events")
    async def get_k8s_events(self, req):
        node_id = req.query.get("node_id")
        
        # Access _events directly as we are on the main loop
        events_list = list(self._events)
        
        if node_id:
            logger.info(f"Get K8s events request for node_id: {node_id}")
            # TODO: Implement accurate NodeID -> PodName mapping.
            # For PoC, we disable filtering to ensure events are visible.
            # events_list = [e for e in events_list if e.related_node_id == node_id]
            pass

        data = {
            "events": [
                {
                    "event_id": e.event_id,
                    "severity": "WARNING" if e.severity == K8sEvent.Severity.WARNING else "INFO",
                    "message": e.message,
                    "reason": e.reason,
                    "timestamp_ms": e.timestamp_ms,
                    "related_node_id": e.related_node_id,
                } for e in events_list
            ]
        }
        
        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="Retrieved K8s events",
            **data
        )

    @staticmethod
    def is_minimal_module():
        return False

    async def run(self):
        self._loop = asyncio.get_running_loop()
        if await self._init_k8s_client():
            # Run watcher in a separate thread to avoid blocking the asyncio loop
            t = threading.Thread(target=self._run_k8s_watch, daemon=True)
            t.start()
