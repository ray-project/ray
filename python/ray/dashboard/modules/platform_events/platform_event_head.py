import asyncio
import collections
import concurrent.futures
import logging
import os
import threading

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
    @classmethod
    def is_enabled(cls) -> bool:
        is_enabled = os.environ.get(
            "RAY_DASHBOARD_INGEST_K8S_EVENTS", "False"
        ).lower() in ("true", "1")
        if not is_enabled:
            logger.info(
                "Skipping PlatformEventsHead because RAY_DASHBOARD_INGEST_K8S_EVENTS is not enabled."
            )
        return is_enabled

    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)
        self._cluster_name = None
        self._ray_job_name = None
        self._ray_service_name = None
        self._namespace = None
        # Dict of "kind/name" -> resource_version
        self._last_resource_versions = {}
        self._k8s_v1_api = None
        self._stop_event = threading.Event()
        self._loop = None
        self._events = collections.OrderedDict()
        self._active_watches = {}
        self._watches_lock = threading.Lock()
        self._executor = None

    async def _init_k8s_client(self):
        if not k8s_config:
            logger.info(
                "Kubernetes library not found. Platform events will be disabled."
            )
            return False

        # RAY_CLUSTER_NAME is set by KubeRay Operator on every Ray cluster.
        cluster_name = os.environ.get("RAY_CLUSTER_NAME")
        if not cluster_name:
            logger.warning(
                "RAY_CLUSTER_NAME not found. Platform events will not be available."
            )
            return False

        # RAY_CLUSTER_NAMESPACE is set by KubeRay Operator on every Ray cluster pod.
        self._namespace = os.environ.get("RAY_CLUSTER_NAMESPACE", "default")

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

        self._cluster_name = cluster_name
        logger.info(
            f"Discovered RayCluster name: {self._cluster_name} in namespace: {self._namespace}"
        )
        try:
            custom_api = client.CustomObjectsApi()
            cluster_obj = await self._loop.run_in_executor(
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

    async def run(self):
        await super().run()
        self._loop = asyncio.get_running_loop()
        if await self._init_k8s_client():
            # Targets to watch
            targets = [("RayCluster", self._cluster_name)]
            if self._ray_job_name:
                targets.append(("RayJob", self._ray_job_name))
            if self._ray_service_name:
                targets.append(("RayService", self._ray_service_name))

            self._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=len(targets), thread_name_prefix="platform_events"
            )

            # Pre-initialize keys to avoid race conditions during dict resizing in threads
            for kind, name in targets:
                target_id = f"{kind}/{name}"
                self._last_resource_versions[target_id] = None

            # Run watchers in separate threads to avoid blocking the asyncio loop
            futures = []
            for kind, name in targets:
                futures.append(
                    self._loop.run_in_executor(
                        self._executor, self._run_k8s_watch, kind, name
                    )
                )

            try:
                # TODO: graceful shutdown - _stop_event is only set inside cleanup(),
                # which is called from this finally block, so watchers only stop after
                # the gather is already cancelled. Check whether dashboard head modules
                # support a proper shutdown hook that would let us set _stop_event before
                # awaiting the futures.
                await asyncio.gather(*futures)
            finally:
                await self.cleanup()

    def _run_k8s_watch(self, kind: str, name: str):
        """Blocking method to run in a separate thread."""
        if not name:
            logger.error(
                f"Attempted to watch events for kind {kind} without a specific name. Aborting."
            )
            return

        target_id = f"{kind}/{name}"
        logger.info(f"Starting Platform event watcher thread for {target_id}")
        if not self._k8s_v1_api or not watch:
            logger.warning(
                f"Halting platform event watcher for {target_id}: "
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
                        if self._loop:
                            self._loop.call_soon_threadsafe(
                                self._process_k8s_event_callback, k8s_event_obj
                            )

                except ApiException as e:
                    if e.status == 410:
                        # K8s resource versions are monotonically increasing integers
                        # the API server uses to mark a point in the event stream. Passing it
                        # to the watch resumes from that point. The K8s API server only retains
                        # a bounded history (typically one hour), so a log disconnect can make our
                        # saved version too old, for which the API server returns 410 (Gone)
                        # Reset to None to start a fresh watch from the current state.
                        logger.warning(
                            f"Resource version expired for {target_id}, resetting watch"
                        )
                        # Set to None instead of popping to avoid structural dict changes in threads.
                        self._last_resource_versions[target_id] = None
                    else:
                        logger.error(
                            f"K8s API error watching events for {target_id}: {e}"
                        )
                        self._stop_event.wait(5)
                except Exception as e:
                    logger.error(f"Error watching Platform events for {target_id}: {e}")
                    self._stop_event.wait(5)
        finally:
            with self._watches_lock:
                self._active_watches.pop(target_id, None)

    def _process_k8s_event_callback(self, event_obj):
        """Callback running in the main asyncio loop.

        Builds a RayEvent proto from a raw k8s event object and caches it
        in memory for retrieval via the REST API.

        TODO: Support async publishing to /api/v0/external/ray_events once
        https://github.com/ray-project/ray/pull/61859 is merged.
        """
        involved_object = event_obj.involved_object
        uid = event_obj.metadata.uid

        event_timestamp = event_obj.last_timestamp or event_obj.first_timestamp
        timestamp = Timestamp()
        if event_timestamp:
            timestamp.FromDatetime(event_timestamp)
        else:
            timestamp.GetCurrentTime()

        if uid in self._events:
            ray_event = self._events[uid]
            ray_event.timestamp.CopyFrom(timestamp)
            ray_event.message = event_obj.message or ""
            ray_event.platform_event.message = event_obj.message or ""

            if event_obj.count and event_obj.count > 1:
                ray_event.platform_event.custom_fields["count"] = str(event_obj.count)
            else:
                ray_event.platform_event.custom_fields.pop("count", None)

            # Move to end to maintain most-recent order
            self._events.move_to_end(uid)
            return

        # Create new event if not found
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

        ray_event = RayEvent(
            event_id=uid.encode(),
            source_type=RayEvent.SourceType.CLUSTER_LIFECYCLE,
            event_type=RayEvent.EventType.PLATFORM_EVENT,
            timestamp=timestamp,
            severity=severity,
            message=event_obj.message or "",
            platform_event=platform_event,
        )

        # Add new event and enforce max size
        if len(self._events) >= MAX_EVENTS_TO_CACHE:
            self._events.popitem(last=False)

        self._events[uid] = ray_event

    @routes.get("/api/v0/platform_events")
    @dashboard_optional_utils.aiohttp_cache
    async def get_platform_events(self, req):
        """Return recently observed platform events as a JSON array."""
        # Sort by timestamp ascending (oldest first) to fix out-of-order
        # issues caused by 410 Gone reconnects.
        sorted_events = sorted(
            self._events.values(),
            key=lambda e: e.timestamp.seconds + e.timestamp.nanos / 1e9,
        )
        payload = [
            dashboard_utils.message_to_dict(
                e, always_print_fields_with_no_presence=True
            )
            for e in sorted_events
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
        with self._watches_lock:
            watches = list(self._active_watches.values())
        for w in watches:
            try:
                w.stop()
            except Exception as e:
                logger.warning(f"Error stopping watch: {e}")
        if self._executor:
            self._executor.shutdown(wait=False)
        logger.info("Platform events watcher stopped.")
