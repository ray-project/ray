import asyncio
import collections
import logging
import os
import threading
from typing import Optional

from google.protobuf.timestamp_pb2 import Timestamp

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.core.generated.platform_event_pb2 import PlatformEvent, Source

try:
    from kubernetes import client, config as k8s_config, watch
    from kubernetes.client.rest import ApiException
except ImportError:
    client = None
    k8s_config = None
    watch = None
    ApiException = None

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable
# Max number of events to cache in memory.
MAX_EVENTS_TO_CACHE = 1000


class PlatformEventsHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)
        # Dedup dict: k8s event UID -> None for bounded eviction.
        self._event_uids = collections.OrderedDict()
        self._cluster_name = None
        self._ray_job_name = None
        self._ray_service_name = None
        # Dict of "kind/name" -> resource_version
        self._last_resource_versions = {}
        self._k8s_v1_api = None
        self._stop_event = threading.Event()
        self._loop = None
        self._events: collections.deque = collections.deque(maxlen=MAX_EVENTS_TO_CACHE)

    async def _init_k8s_client(self):
        if not k8s_config:
            logger.info(
                "Kubernetes library not found. Platform events will be disabled."
            )
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

        self._cluster_name = os.environ.get("RAY_CLUSTER_NAME")
        if self._cluster_name:
            logger.info(f"Discovered RayCluster name: {self._cluster_name}")
            namespace = os.environ.get("RAY_CLUSTER_NAMESPACE", "default")
            try:
                custom_api = client.CustomObjectsApi()
                cluster_obj = await self._loop.run_in_executor(
                    None,
                    lambda: custom_api.get_namespaced_custom_object(
                        group="ray.io",
                        version="v1",
                        namespace=namespace,
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
        else:
            logger.warning(
                "RAY_CLUSTER_NAME not found. Platform events will not be available."
            )
            return False

        return True

    async def run(self):
        self._loop = asyncio.get_running_loop()
        if await self._init_k8s_client():
            # Targets to watch
            targets = [("RayCluster", self._cluster_name)]
            if self._ray_job_name:
                targets.append(("RayJob", self._ray_job_name))
            if self._ray_service_name:
                targets.append(("RayService", self._ray_service_name))

            # Pre-initialize keys to avoid race conditions during dict resizing in threads
            for kind, name in targets:
                target_id = f"{kind}/{name}" if name else kind
                self._last_resource_versions[target_id] = None

            # Run watchers in separate threads to avoid blocking the asyncio loop
            for kind, name in targets:
                t = threading.Thread(
                    target=self._run_k8s_watch, args=(kind, name), daemon=True
                )
                t.start()

    def _run_k8s_watch(self, kind: str, name: Optional[str]):
        """Blocking method to run in a separate thread."""
        target_id = f"{kind}/{name}" if name else kind
        logger.info(f"Starting Platform event watcher thread for {target_id}")
        if not self._k8s_v1_api or not watch:
            logger.warning(
                f"Halting platform event watcher for {target_id}: "
                "Kubernetes client or watch library is not initialized."
            )
            return

        w = watch.Watch()
        namespace = os.environ.get("RAY_CLUSTER_NAMESPACE", "default")
        while not self._stop_event.is_set():
            try:
                field_selector = f"involvedObject.kind={kind}"
                if name:
                    field_selector += f",involvedObject.name={name}"
                stream = w.stream(
                    self._k8s_v1_api.list_namespaced_event,
                    namespace=namespace,
                    field_selector=field_selector,
                    resource_version=self._last_resource_versions.get(target_id),
                    timeout_seconds=60,
                    _request_timeout=70,
                )

                for event in stream:
                    k8s_event_obj = event["object"]
                    rv = k8s_event_obj.metadata.resource_version
                    self._last_resource_versions[target_id] = rv
                    if self._loop:
                        self._loop.call_soon_threadsafe(
                            self._process_k8s_event_callback, k8s_event_obj
                        )

            except ApiException as e:
                if e.status == 410:
                    # Resource version too old, reset and retry
                    logger.warning(
                        f"Resource version expired for {target_id}, resetting watch"
                    )
                    # Set to None instead of popping to avoid structural dict changes in threads.
                    self._last_resource_versions[target_id] = None
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

    def _process_k8s_event_callback(self, event_obj):
        """Callback running in the main asyncio loop.

        Builds a RayEvent proto from a raw k8s event object and caches it
        in memory for retrieval via the REST API.

        TODO: Support async publishing to /api/v0/external/ray_events once
        https://github.com/ray-project/ray/pull/61859 is merged.
        """
        involved_object = event_obj.involved_object

        # Deduplicate by k8s event UID to suppress repeated watch deliveries.
        uid = event_obj.metadata.uid
        if uid in self._event_uids:
            return

        k8s_event_type = event_obj.type or "Normal"
        severity = (
            RayEvent.Severity.WARNING
            if k8s_event_type == "Warning"
            else RayEvent.Severity.INFO
        )

        source_metadata = {"namespace": event_obj.metadata.namespace or "default"}
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

        event_timestamp = event_obj.last_timestamp or event_obj.first_timestamp
        timestamp = Timestamp()
        if event_timestamp:
            timestamp.FromDatetime(event_timestamp)
        else:
            timestamp.GetCurrentTime()

        ray_event = RayEvent(
            event_id=uid.encode(),
            source_type=RayEvent.SourceType.CLUSTER_LIFECYCLE,
            event_type=RayEvent.EventType.PLATFORM_EVENT,
            timestamp=timestamp,
            severity=severity,
            message=event_obj.message or "",
            platform_event=platform_event,
        )

        self._events.append(ray_event)
        self._event_uids[uid] = None
        # Evict oldest UIDs to bound memory.
        if len(self._event_uids) > MAX_EVENTS_TO_CACHE * 2:
            while len(self._event_uids) > MAX_EVENTS_TO_CACHE:
                self._event_uids.popitem(last=False)

    @routes.get("/api/v0/platform_events")
    @dashboard_optional_utils.aiohttp_cache
    async def get_platform_events(self, req):
        """Return recently observed platform events as a JSON array."""
        payload = [
            dashboard_utils.message_to_dict(
                e, always_print_fields_with_no_presence=True
            )
            for e in list(self._events)
        ]
        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="Retrieved platform events",
            events=payload,
        )

    @staticmethod
    def is_minimal_module():
        return False

    async def cleanup(self):
        self._stop_event.set()
        # Threads are daemon=True and may be blocked in a K8s API call;
        # setting the stop event lets them exit on their next retry/timeout.
        logger.info("Platform events watcher stopped.")
