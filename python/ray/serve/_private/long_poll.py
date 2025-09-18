import asyncio
import contextvars
import logging
import os
import random
from asyncio import sleep
from asyncio.events import AbstractEventLoop
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Callable, DefaultDict, Dict, Optional, Set, Tuple, Union

import ray
from ray._common.utils import get_or_create_event_loop
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.generated.serve_pb2 import (
    DeploymentTargetInfo,
    EndpointInfo as EndpointInfoProto,
    EndpointSet,
    LongPollRequest,
    LongPollResult,
    UpdatedObject as UpdatedObjectProto,
)
from ray.util import metrics

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Each LongPollClient will send requests to LongPollHost to poll changes
# as blocking awaitable. This doesn't scale if we have many client instances
# that will slow down, or even block controller actor's event loop if near
# its max_concurrency limit. Therefore we timeout a polling request after
# a few seconds and let each client retry on their end.
# We randomly select a timeout within this range to avoid a "thundering herd"
# when there are many clients subscribing at the same time.
LISTEN_FOR_CHANGE_REQUEST_TIMEOUT_S = (
    float(os.environ.get("LISTEN_FOR_CHANGE_REQUEST_TIMEOUT_S_LOWER_BOUND", "30")),
    float(os.environ.get("LISTEN_FOR_CHANGE_REQUEST_TIMEOUT_S_UPPER_BOUND", "60")),
)


class LongPollNamespace(Enum):
    def __repr__(self):
        return f"{self.__class__.__name__}.{self.name}"

    DEPLOYMENT_TARGETS = auto()
    ROUTE_TABLE = auto()
    GLOBAL_LOGGING_CONFIG = auto()
    DEPLOYMENT_CONFIG = auto()


@dataclass
class UpdatedObject:
    object_snapshot: Any
    # The identifier for the object's version. There is not sequential relation
    # among different object's snapshot_ids.
    snapshot_id: int


# Type signature for the update state callbacks. E.g.
# async def update_state(updated_object: Any):
#     do_something(updated_object)
UpdateStateCallable = Callable[[Any], None]
KeyType = Union[str, LongPollNamespace, Tuple[LongPollNamespace, str]]


class LongPollState(Enum):
    TIME_OUT = auto()


class LongPollClient:
    """The asynchronous long polling client.

    Args:
        host_actor: handle to actor embedding LongPollHost.
        key_listeners: a dictionary mapping keys to
          callbacks to be called on state update for the corresponding keys.
        call_in_event_loop: an asyncio event loop
          to post the callback into.
    """

    def __init__(
        self,
        host_actor,
        key_listeners: Dict[KeyType, UpdateStateCallable],
        call_in_event_loop: AbstractEventLoop,
    ) -> None:
        # We used to allow this to be optional, but due to Ray Client issue
        # we now enforce all long poll client to post callback to event loop
        # See https://github.com/ray-project/ray/issues/20971
        assert call_in_event_loop is not None

        self.host_actor = host_actor
        self.key_listeners = key_listeners
        self.event_loop = call_in_event_loop
        self.snapshot_ids: Dict[KeyType, int] = {
            # The initial snapshot id for each key is < 0,
            # but real snapshot keys in the long poll host are always >= 0,
            # so this will always trigger an initial update.
            key: -1
            for key in self.key_listeners.keys()
        }
        self.is_running = True

        # NOTE(edoakes): we schedule the initial _poll_next call with an empty context
        # so that Ray will not recursively cancel the underlying `listen_for_change`
        # task. See: https://github.com/ray-project/ray/issues/52476.
        self.event_loop.call_soon_threadsafe(
            self._poll_next, context=contextvars.Context()
        )

    def stop(self) -> None:
        """Stop the long poll client after the next RPC returns."""
        self.is_running = False

    def add_key_listeners(
        self, key_listeners: Dict[KeyType, UpdateStateCallable]
    ) -> None:
        """Add more key listeners to the client.
        The new listeners will only be included in the *next* long poll request;
        the current request will continue with the existing listeners.

        If a key is already in the client, the new listener will replace the old one,
        but the snapshot ID will be preserved, so the new listener will only be called
        on the *next* update to that key.
        """
        # We need to run the underlying method in the same event loop that runs
        # the long poll loop, because we need to mutate the mapping of snapshot IDs,
        # which also needs to be serialized by the long poll's RPC to the
        # Serve Controller. If those happened concurrently in different threads,
        # we could get a `RuntimeError: dictionary changed size during iteration`.
        # See https://github.com/ray-project/ray/pull/52793 for more details.
        self.event_loop.call_soon_threadsafe(self._add_key_listeners, key_listeners)

    def _add_key_listeners(
        self, key_listeners: Dict[KeyType, UpdateStateCallable]
    ) -> None:
        """Inner method that actually adds the key listeners, to be called
        via call_soon_threadsafe for thread safety.
        """
        # Only initialize snapshot ids for *new* keys.
        self.snapshot_ids.update(
            {key: -1 for key in key_listeners.keys() if key not in self.key_listeners}
        )
        self.key_listeners.update(key_listeners)

    def _on_callback_completed(self, trigger_at: int):
        """Called after a single callback is completed.

        When the total number of callback completed equals to trigger_at,
        _poll_next() will be called. This is designed to make sure we only
        _poll_next() after all the state callbacks completed. This is a
        way to serialize the callback invocations between object versions.
        """
        self._callbacks_processed_count += 1
        if self._callbacks_processed_count == trigger_at:
            self._poll_next()

    def _poll_next(self):
        """Poll the update. The callback is expected to scheduler another
        _poll_next call.
        """
        if not self.is_running:
            return

        self._callbacks_processed_count = 0
        self._current_ref = self.host_actor.listen_for_change.remote(self.snapshot_ids)
        self._current_ref._on_completed(lambda update: self._process_update(update))

    def _schedule_to_event_loop(self, callback):
        # Schedule the next iteration only if the loop is running.
        # The event loop might not be running if users used a cached
        # version across loops.
        if self.event_loop.is_running():
            self.event_loop.call_soon_threadsafe(callback)
        else:
            logger.error("The event loop is closed, shutting down long poll client.")
            self.is_running = False

    def _process_update(self, updates: Dict[str, UpdatedObject]):
        if isinstance(updates, (ray.exceptions.RayActorError)):
            # This can happen during shutdown where the controller is
            # intentionally killed, the client should just gracefully
            # exit.
            logger.debug("LongPollClient failed to connect to host. Shutting down.")
            self.is_running = False
            return

        if isinstance(updates, ConnectionError):
            logger.warning("LongPollClient connection failed, shutting down.")
            self.is_running = False
            return

        if isinstance(updates, (ray.exceptions.RayTaskError)):
            # Some error happened in the controller. It could be a bug or
            # some undesired state.
            logger.error("LongPollHost errored\n" + updates.traceback_str)
            # We must call this in event loop so it works in Ray Client.
            # See https://github.com/ray-project/ray/issues/20971
            self._schedule_to_event_loop(self._poll_next)
            return

        if updates == LongPollState.TIME_OUT:
            logger.debug("LongPollClient polling timed out. Retrying.")
            self._schedule_to_event_loop(self._poll_next)
            return

        logger.debug(
            f"LongPollClient {self} received updates for keys: "
            f"{list(updates.keys())}.",
            extra={"log_to_stderr": False},
        )
        if not updates:  # no updates, no callbacks to run, just poll again
            self._schedule_to_event_loop(self._poll_next)
        for key, update in updates.items():
            self.snapshot_ids[key] = update.snapshot_id
            callback = self.key_listeners[key]

            # Bind the parameters because closures are late-binding.
            # https://docs.python-guide.org/writing/gotchas/#late-binding-closures # noqa: E501
            def chained(callback=callback, arg=update.object_snapshot):
                callback(arg)
                self._on_callback_completed(trigger_at=len(updates))

            self._schedule_to_event_loop(chained)


class LongPollHost:
    """The server side object that manages long pulling requests.

    The desired use case is to embed this in an Ray actor. Client will be
    expected to call actor.listen_for_change.remote(...). On the host side,
    you can call host.notify_changed({key: object}) to update the state and
    potentially notify whoever is polling for these values.

    Internally, we use snapshot_ids for each object to identify client with
    outdated object and immediately return the result. If the client has the
    up-to-date version, then the listen_for_change call will only return when
    the object is updated.
    """

    def __init__(
        self,
        listen_for_change_request_timeout_s: Tuple[
            int, int
        ] = LISTEN_FOR_CHANGE_REQUEST_TIMEOUT_S,
    ):
        # Map object_key -> int
        self.snapshot_ids: Dict[KeyType, int] = {}
        # Map object_key -> object
        self.object_snapshots: Dict[KeyType, Any] = {}
        # Map object_key -> set(asyncio.Event waiting for updates)
        self.notifier_events: DefaultDict[KeyType, Set[asyncio.Event]] = defaultdict(
            set
        )

        self._listen_for_change_request_timeout_s = listen_for_change_request_timeout_s
        self.transmission_counter = metrics.Counter(
            "serve_long_poll_host_transmission_counter",
            description="The number of times the long poll host transmits data.",
            tag_keys=("namespace_or_state",),
        )

    def _get_num_notifier_events(self, key: Optional[KeyType] = None):
        """Used for testing."""
        if key is not None:
            return len(self.notifier_events[key])
        else:
            return sum(len(events) for events in self.notifier_events.values())

    def _count_send(
        self, timeout_or_data: Union[LongPollState, Dict[KeyType, UpdatedObject]]
    ):
        """Helper method that tracks the data sent by listen_for_change.

        Records number of times long poll host sends data in the
        ray_serve_long_poll_host_send_counter metric.
        """

        if isinstance(timeout_or_data, LongPollState):
            # The only LongPollState is TIME_OUT– the long poll
            # connection has timed out.
            self.transmission_counter.inc(
                value=1, tags={"namespace_or_state": "TIMEOUT"}
            )
        else:
            data = timeout_or_data
            for key in data.keys():
                self.transmission_counter.inc(
                    value=1, tags={"namespace_or_state": str(key)}
                )

    async def listen_for_change(
        self,
        keys_to_snapshot_ids: Dict[KeyType, int],
    ) -> Union[LongPollState, Dict[KeyType, UpdatedObject]]:
        """Listen for changed objects.

        This method will return a dictionary of updated objects. It returns
        immediately if any of the snapshot_ids are outdated,
        otherwise it will block until there's an update.
        """
        # If there are no keys to listen for,
        # just wait for a short time to provide backpressure,
        # then return an empty update.
        if not keys_to_snapshot_ids:
            await sleep(1)

            updated_objects = {}
            self._count_send(updated_objects)
            return updated_objects

        # If there are any keys with outdated snapshot ids,
        # return their updated values immediately.
        updated_objects = {}
        for key, client_snapshot_id in keys_to_snapshot_ids.items():
            try:
                existing_id = self.snapshot_ids[key]
            except KeyError:
                # The caller may ask for keys that we don't know about (yet),
                # just ignore them.
                # This can happen when, for example,
                # a deployment handle is manually created for an app
                # that hasn't been deployed yet (by bypassing the safety checks).
                continue

            if existing_id != client_snapshot_id:
                updated_objects[key] = UpdatedObject(
                    self.object_snapshots[key], existing_id
                )
        if len(updated_objects) > 0:
            self._count_send(updated_objects)
            return updated_objects

        # Otherwise, register asyncio events to be waited.
        async_task_to_events = {}
        async_task_to_watched_keys = {}
        for key in keys_to_snapshot_ids.keys():
            # Create a new asyncio event for this key.
            event = asyncio.Event()

            # Make sure future caller of notify_changed will unblock this
            # asyncio Event.
            self.notifier_events[key].add(event)

            task = get_or_create_event_loop().create_task(event.wait())
            async_task_to_events[task] = event
            async_task_to_watched_keys[task] = key

        done, not_done = await asyncio.wait(
            async_task_to_watched_keys.keys(),
            return_when=asyncio.FIRST_COMPLETED,
            timeout=random.uniform(*self._listen_for_change_request_timeout_s),
        )

        for task in not_done:
            task.cancel()
            try:
                event = async_task_to_events[task]
                self.notifier_events[async_task_to_watched_keys[task]].remove(event)
            except KeyError:
                # Because we use `FIRST_COMPLETED` above, a task in `not_done` may
                # actually have had its event removed in `notify_changed`.
                pass

        if len(done) == 0:
            self._count_send(LongPollState.TIME_OUT)
            return LongPollState.TIME_OUT
        else:
            updated_objects = {}
            for task in done:
                updated_object_key = async_task_to_watched_keys[task]
                updated_objects[updated_object_key] = UpdatedObject(
                    self.object_snapshots[updated_object_key],
                    self.snapshot_ids[updated_object_key],
                )
            self._count_send(updated_objects)
            return updated_objects

    async def listen_for_change_java(
        self,
        keys_to_snapshot_ids_bytes: bytes,
    ) -> bytes:
        """Listen for changed objects. only call by java proxy/router now.
        Args:
            keys_to_snapshot_ids_bytes (Dict[str, int]): the protobuf bytes of
              keys_to_snapshot_ids (Dict[str, int]).
        """
        request_proto = LongPollRequest.FromString(keys_to_snapshot_ids_bytes)
        keys_to_snapshot_ids = {
            self._parse_xlang_key(xlang_key): snapshot_id
            for xlang_key, snapshot_id in request_proto.keys_to_snapshot_ids.items()
        }
        keys_to_updated_objects = await self.listen_for_change(keys_to_snapshot_ids)
        return self._listen_result_to_proto_bytes(keys_to_updated_objects)

    def _parse_poll_namespace(self, name: str):
        if name == LongPollNamespace.ROUTE_TABLE.name:
            return LongPollNamespace.ROUTE_TABLE
        elif name == LongPollNamespace.DEPLOYMENT_TARGETS.name:
            return LongPollNamespace.DEPLOYMENT_TARGETS
        else:
            return name

    def _parse_xlang_key(self, xlang_key: str) -> KeyType:
        if xlang_key is None:
            raise ValueError("func _parse_xlang_key: xlang_key is None")
        if xlang_key.startswith("(") and xlang_key.endswith(")"):
            fields = xlang_key[1:-1].split(",")
            if len(fields) == 2:
                enum_field = self._parse_poll_namespace(fields[0].strip())
                if isinstance(enum_field, LongPollNamespace):
                    return enum_field, fields[1].strip()
        else:
            return self._parse_poll_namespace(xlang_key)
        raise ValueError("can not parse key type from xlang_key {}".format(xlang_key))

    def _build_xlang_key(self, key: KeyType) -> str:
        if isinstance(key, tuple):
            return "(" + key[0].name + "," + key[1] + ")"
        elif isinstance(key, LongPollNamespace):
            return key.name
        else:
            return key

    def _object_snapshot_to_proto_bytes(
        self, key: KeyType, object_snapshot: Any
    ) -> bytes:
        if key == LongPollNamespace.ROUTE_TABLE:
            # object_snapshot is Dict[DeploymentID, EndpointInfo]
            # NOTE(zcin): the endpoint dictionary broadcasted to Java
            # HTTP proxies should use string as key because Java does
            # not yet support 2.x or applications
            xlang_endpoints = {
                str(endpoint_tag): EndpointInfoProto(route=endpoint_info.route)
                for endpoint_tag, endpoint_info in object_snapshot.items()
            }
            return EndpointSet(endpoints=xlang_endpoints).SerializeToString()
        elif isinstance(key, tuple) and key[0] == LongPollNamespace.DEPLOYMENT_TARGETS:
            # object_snapshot.running_replicas is List[RunningReplicaInfo]
            actor_name_list = [
                replica_info.replica_id.to_full_id_str()
                for replica_info in object_snapshot.running_replicas
            ]
            return DeploymentTargetInfo(
                replica_names=actor_name_list,
                is_available=object_snapshot.is_available,
            ).SerializeToString()
        else:
            return str.encode(str(object_snapshot))

    def _listen_result_to_proto_bytes(
        self, keys_to_updated_objects: Dict[KeyType, UpdatedObject]
    ) -> bytes:
        xlang_keys_to_updated_objects = {
            self._build_xlang_key(key): UpdatedObjectProto(
                snapshot_id=updated_object.snapshot_id,
                object_snapshot=self._object_snapshot_to_proto_bytes(
                    key, updated_object.object_snapshot
                ),
            )
            for key, updated_object in keys_to_updated_objects.items()
        }
        data = {
            "updated_objects": xlang_keys_to_updated_objects,
        }
        proto = LongPollResult(**data)
        return proto.SerializeToString()

    def notify_changed(self, updates: Mapping[KeyType, Any]) -> None:
        """
        Update the current snapshot of some objects
        and notify any long poll clients.
        """
        for object_key, updated_object in updates.items():
            try:
                self.snapshot_ids[object_key] += 1
            except KeyError:
                # Initial snapshot id must be >= 0, so that the long poll client
                # can send a negative initial snapshot id to get a fast update.
                # They should also be randomized; see
                # https://github.com/ray-project/ray/pull/45881#discussion_r1645243485
                self.snapshot_ids[object_key] = random.randint(0, 1_000_000)
            self.object_snapshots[object_key] = updated_object
            logger.debug(f"LongPollHost: Notify change for key {object_key}.")

            for event in self.notifier_events.pop(object_key, set()):
                event.set()
