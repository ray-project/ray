import asyncio
from asyncio.events import AbstractEventLoop
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum, auto
import logging
import os
import random
from typing import Any, Tuple, Callable, DefaultDict, Dict, Set, Union

import ray
from ray.serve.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Each LongPollClient will send requests to LongPollHost to poll changes
# as blocking awaitable. This doesn't scale if we have many client instances
# that will slow down, or even block controller actor's event loop if near
# its max_concurrency limit. Therefore we timeout a polling request after
# a few seconds and let each client retry on their end.
# We randomly select a timeout within this range to avoid a "thundering herd"
# when there are many clients subscribing at the same time.
LISTEN_FOR_CHANGE_REQUEST_TIMEOUT_S = (
    int(os.environ.get("LISTEN_FOR_CHANGE_REQUEST_TIMEOUT_S_LOWER_BOUND", "30")),
    int(os.environ.get("LISTEN_FOR_CHANGE_REQUEST_TIMEOUT_S_UPPER_BOUND", "60")),
)


class LongPollNamespace(Enum):
    def __repr__(self):
        return f"{self.__class__.__name__}.{self.name}"

    RUNNING_REPLICAS = auto()
    ROUTE_TABLE = auto()


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


class LongPollClient:
    """The asynchronous long polling client.

    Args:
        host_actor(ray.ActorHandle): handle to actor embedding LongPollHost.
        key_listeners(Dict[str, AsyncCallable]): a dictionary mapping keys to
          callbacks to be called on state update for the corresponding keys.
        call_in_event_loop(AbstractEventLoop): an asyncio event loop
          to post the callback into.
    """

    def __init__(
        self,
        host_actor,
        key_listeners: Dict[KeyType, UpdateStateCallable],
        call_in_event_loop: AbstractEventLoop,
    ) -> None:
        assert len(key_listeners) > 0
        # We used to allow this to be optional, but due to Ray Client issue
        # we now enforce all long poll client to post callback to event loop
        # See https://github.com/ray-project/ray/issues/20971
        assert call_in_event_loop is not None

        self.host_actor = host_actor
        self.key_listeners = key_listeners
        self.event_loop = call_in_event_loop
        self._reset()

        self.is_running = True

    def _reset(self):
        self.snapshot_ids: Dict[KeyType, int] = {
            key: -1 for key in self.key_listeners.keys()
        }
        self.object_snapshots: Dict[KeyType, Any] = dict()

        self._current_ref = None
        self._callbacks_processed_count = 0
        self._poll_next()

    def _on_callback_completed(self, trigger_at: int):
        """Called after a single callback is completed.

        When the total number of callback completed equals to trigger_at,
        _poll_next() will be called. This is designed to make sure we only
        _poll_next() after all the state callbacks completed. This is a
        way to serialize the callback invocations between object versions.
        """
        self._callbacks_processed_count += 1

        if self._callbacks_processed_count == trigger_at:
            self._callbacks_processed_count = 0
            self._poll_next()

    def _poll_next(self):
        """Poll the update. The callback is expected to scheduler another
        _poll_next call.
        """
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
            if isinstance(updates.as_instanceof_cause(), (asyncio.TimeoutError)):
                logger.debug("LongPollClient polling timed out. Retrying.")
            else:
                # Some error happened in the controller. It could be a bug or
                # some undesired state.
                logger.error("LongPollHost errored\n" + updates.traceback_str)
            # We must call this in event loop so it works in Ray Client.
            # See https://github.com/ray-project/ray/issues/20971
            self._schedule_to_event_loop(self._poll_next)
            return

        logger.debug(
            f"LongPollClient {self} received updates for keys: "
            f"{list(updates.keys())}."
        )
        for key, update in updates.items():
            self.object_snapshots[key] = update.object_snapshot
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
    you can call host.notify_changed(key, object) to update the state and
    potentially notify whoever is polling for these values.

    Internally, we use snapshot_ids for each object to identify client with
    outdated object and immediately return the result. If the client has the
    up-to-date verison, then the listen_for_change call will only return when
    the object is updated.
    """

    def __init__(self):
        # Map object_key -> int
        self.snapshot_ids: DefaultDict[KeyType, int] = defaultdict(
            lambda: random.randint(0, 1_000_000)
        )
        # Map object_key -> object
        self.object_snapshots: Dict[KeyType, Any] = dict()
        # Map object_key -> set(asyncio.Event waiting for updates)
        self.notifier_events: DefaultDict[KeyType, Set[asyncio.Event]] = defaultdict(
            set
        )

    async def listen_for_change(
        self,
        keys_to_snapshot_ids: Dict[KeyType, int],
    ) -> Dict[KeyType, UpdatedObject]:
        """Listen for changed objects.

        This method will returns a dictionary of updated objects. It returns
        immediately if the snapshot_ids are outdated, otherwise it will block
        until there's one updates.
        """
        watched_keys = keys_to_snapshot_ids.keys()
        existent_keys = set(watched_keys).intersection(set(self.snapshot_ids.keys()))

        # If there are any outdated keys (by comparing snapshot ids)
        # return immediately.
        client_outdated_keys = {
            key: UpdatedObject(self.object_snapshots[key], self.snapshot_ids[key])
            for key in existent_keys
            if self.snapshot_ids[key] != keys_to_snapshot_ids[key]
        }
        if len(client_outdated_keys) > 0:
            return client_outdated_keys

        # Otherwise, register asyncio events to be waited.
        async_task_to_watched_keys = {}
        for key in watched_keys:
            # Create a new asyncio event for this key
            event = asyncio.Event()
            task = asyncio.get_event_loop().create_task(event.wait())
            async_task_to_watched_keys[task] = key

            # Make sure future caller of notify_changed will unblock this
            # asyncio Event.
            self.notifier_events[key].add(event)

        done, not_done = await asyncio.wait(
            async_task_to_watched_keys.keys(),
            return_when=asyncio.FIRST_COMPLETED,
            timeout=random.uniform(*LISTEN_FOR_CHANGE_REQUEST_TIMEOUT_S),
        )

        [task.cancel() for task in not_done]

        if len(done) == 0:
            raise asyncio.TimeoutError("Polling request timed out.")
        else:
            updated_object_key: str = async_task_to_watched_keys[done.pop()]
            return {
                updated_object_key: UpdatedObject(
                    self.object_snapshots[updated_object_key],
                    self.snapshot_ids[updated_object_key],
                )
            }

    def notify_changed(
        self,
        object_key: KeyType,
        updated_object: Any,
    ):
        self.snapshot_ids[object_key] += 1
        self.object_snapshots[object_key] = updated_object
        logger.debug(f"LongPollHost: Notify change for key {object_key}.")

        if object_key in self.notifier_events:
            for event in self.notifier_events.pop(object_key):
                event.set()
