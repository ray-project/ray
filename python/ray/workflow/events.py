import asyncio
from collections import defaultdict
from dataclasses import dataclass
import logging
import ray
from ray.workflow.common import (
    EVENT_ACTOR_NAME,
    MANAGEMENT_ACTOR_NAMESPACE,
)
from ray.workflow.event_listener import EventListenerType, Event

from typing import Any, Dict, List, Set

logger = logging.getLogger(__name__)


def init_manager() -> None:
    get_or_create_manager(warn_on_creation=False)


def get_or_create_manager(warn_on_creation=False) -> "ray.ActorHandle":
    try:
        return ray.get_actor(
            EVENT_ACTOR_NAME, namespace=MANAGEMENT_ACTOR_NAMESPACE)
    except ValueError:
        if warn_on_creation:
            logger.warning("Cannot access workflow event coordinator. It "
                           "could be because "
                           "the workflow manager exited unexpectedly. A new "
                           "coordinator is being created.")
        handle = Manager.options(
            name=EVENT_ACTOR_NAME,
            namespace=MANAGEMENT_ACTOR_NAMESPACE,
            lifetime="detached",
        ).remote()
        ray.get(handle.ping.remote())
        return handle


@dataclass
class _EventResult:
    """
    An internal helper container to store the results of an event.
    """
    step_id: str
    result: Event


_DUMMY_STEP_ID = "__workflow_internal_NEW_EVENT_REGISTERED"


@ray.remote(num_cpus=0)
class Manager:
    def __init__(self):
        # Maps a workflow id to all of its dependencies.
        self.workflow_dependencies: Dict[str, Set[str]] = defaultdict(set)
        # The set of workflows which are waiting for an event to occur. This is
        # the reverse mapping of `self.workflow_dependencies`.
        self.event_dependents: Dict[str, Set[str]] = defaultdict(set)
        # A marker that's set whenever a new event is registered.
        self.new_event_marker = asyncio.Event()
        # The set of events which have been finished, by their step id. The
        # results are checkpointed in storage.
        self.finished_events: Set[str] = {}
        # All the pending events we're managing. This is a mapping from the
        # step id to the resulting event.
        self.events: Dict[str, Any] = {
            _DUMMY_STEP_ID: self.new_event_marker.wait()
        }
        self._loop_ref = self.monitor_loop()

    async def ping(self) -> None:
        logger.info("PONG")
        pass

    async def start_event_listener(
            self, step_id: str, event_listener_type: EventListenerType,
            args: List[Any], kwargs: Dict[str, Any]) -> _EventResult:
        event_listener = event_listener_type()
        result = await event_listener.poll_for_event(*args, **kwargs)
        return _EventResult(step_id, result)

    async def register_workflow_dependency(
            self, waiting_workflow_id: str, event_step_id: str,
            event_listener_type: EventListenerType, args: List[Any],
            kwargs: Dict[str, Any]) -> None:
        """Register that `workflow_id` depends on the step `event_data`'s result
        before it can continue to execute.

        Args:
            workflow_id (str): The id of the workflow to be resumed when the
                event occurs.
            event_data (WorkflowData): The information needed to begin to wait
                for the event.

        """
        if event_step_id in self.finished_events:
            return

        self.workflow_dependencies[waiting_workflow_id].add(event_step_id)
        self.event_dependents[event_step_id].add(waiting_workflow_id)

        if event_step_id not in self.events:
            self.events[event_step_id] = self.start_event_listener(
                event_step_id, event_listener_type, args, kwargs)
            self.new_event_marker.set()

    async def commit_output(self, step_id: str, result: Any):
        logger.info(f"COMMITTING OUTPUT OF STEP: {step_id} which is: {result}")
        pass

    async def monitor_loop(self):
        while True:
            all_promises = self.events.values()
            first_finished = asyncio.wait(
                all_promises, return_when=asyncio.FIRST_COMPLETED)
            if first_finished is True:
                # This means `self.new_event_marker` was set.
                self.new_event_marker.clear()
                self.events[_DUMMY_STEP_ID] = self.new_event_marker.wait()
            else:
                assert isinstance(first_finished, _EventResult)
                event_result = first_finished

                # TODO Checkpoint event.
                dependents = self.event_dependents[event_result.step_id]
                for dependent in dependents:
                    self.workflow_dependencies[dependent].remove(
                        event_result.step_id)
                    if len(self.workflow_dependencies[dependent]) == 0:
                        del self.workflow_dependencies[dependent]
                del self.events[event_result.step_id]
                self.finished_events.add(event_result.step_id)
