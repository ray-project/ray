import logging
import time
from typing import Any, Dict, List, Tuple, Optional, TYPE_CHECKING
import asyncio
import threading

import ray
from ray.workflow import common, recovery, storage, workflow_storage, workflow_access
from ray.util.annotations import PublicAPI
from ray.workflow.event_listener import EventListener, EventListenerType, TimerListener
from ray.workflow.workflow_access import (
    flatten_workflow_output,
    get_or_create_management_actor,
    get_management_actor,
)

if TYPE_CHECKING:
    from ray.actor import ActorHandle
    from ray.workflow.common import StepID, WorkflowExecutionResult

logger = logging.getLogger(__name__)

class WorkflowEvents:
    # id of the workflow
    workflow_id: str
    # mapping of event signature to its listener
    event_step_signature_listener: Dict[str, EventListenerType] = {}
    # signature of incoming event and its checkpoint status
    event_step_checkpointed: Dict[str, bool] = {}
    # outer_most_step_id
    event_outer_most_step_id: Dict[str, str] = {}

async def poll_event_step(workflow_id, event_signature, event_listener_handle, *args, **kwargs):
    event_listener = event_listener_handle()
    event_content = await event_listener.poll_for_event(*args, **kwargs)
    return (workflow_id, event_signature, event_content)

@ray.remote(num_cpus=0)
class EventCoordinatorActor:
    def __init__(self, wma: "workflow_access.WorkflowManagementActor"):
        self.wma = wma
        self.event_registry: Dict[str, WorkflowEvents] = {}
        self.sigature_workflow_step: Dict[str, List[Tuple[str,str]]] = {}
        self.from_upstream_to_downstream: Dict[str, Dict[str,List[str]]] = {}
        self.from_downstream_to_upstream: Dict[str, Dict[str,str]] = {}
        self.wait_list: List[Any] = []

        self.write_lock = asyncio.Lock()
        try:
           self.thread = threading.Thread(target=asyncio.run, args=(self.pollEvent(),))
           self.thread.daemon = True
           self.thread.start()
        except BaseException as err:
            print(err)
            print ("Error: unable to start thread")

    async def transferFaninStepOwnership(self, workflow_id:str, current_step_id:str, \
        downstream_steps:List[Any]) -> None:
        fanin_steps = []
        fanout_steps = {}
        for fanin in downstream_steps:
            (event_listener_handle, event_signature, fanin_step_id, outer_most_step_id, args, kwargs) = fanin
            await self.transferEventStepOwnership(event_listener_handle, event_signature, \
                workflow_id, current_step_id, outer_most_step_id, *args, **kwargs)
            fanin_steps.append(fanin_step_id)
            fanout_steps[fanin_step_id] = current_step_id
        self.from_upstream_to_downstream[workflow_id][current_step_id] = fanin_steps
        self.from_downstream_to_upstream[workflow_id] = fanout_steps

    async def transferEventStepOwnership(self, event_listener_handle, event_signature, \
        workflow_id, current_step_id, outer_most_step_id, *args, **kwargs) -> None:
        async with self.write_lock:
            if workflow_id in self.event_registry.keys():
                we = self.event_registry[workflow_id]
            else:
                we = WorkflowEvents()
            if current_step_id not in we.event_step_checkpointed:
                we.event_step_checkpointed[current_step_id] = False
                we.event_outer_most_step_id[current_step_id] = outer_most_step_id
                we.event_step_signature_listener[event_signature] = event_listener_handle
            self.event_registry[workflow_id] = we
            if len(self.sigature_workflow_step[event_signature]) < 1:
                self.sigature_workflow_step[event_signature] = []
            self.sigature_workflow_step[event_signature].append((workflow_id, current_step_id))
            self.wait_list.append((workflow_id, event_signature))

    async def pollEvent(self):
        while True:
            await asyncio.sleep(1)
            if len(self.wait_list) > 0:
                listeners = set()
                for waiting in self.wait_list:
                    (workflow_id, event_signature) = waiting
                    we = self.event_registry[workflow_id]
                    event_listener_handle = we.event_step_signature_listener[event_signature]
                    listeners.add(poll_event_step(workflow_id, event_signature, event_listener_handle))
                ready, not_ready = await asyncio.wait(listeners, timeout=5, \
                    return_when=asyncio.FIRST_COMPLETED)

                for item in ready:
                    (workflow_id, event_signature, event_content) = item.result()
                    for pair in self.sigature_workflow_step[event_signature]:
                        (workflow_id, current_step_id) = pair
                        if workflow_id not in self.event_registry.keys():
                            raise ValueError(workflow_id+" not found")
                        we = self.event_registry[workflow_id]
                        outer_most_step_id = we.event_outer_most_step_id[current_step_id]
                        await self.checkpointEvent(workflow_id, current_step_id, outer_most_step_id, event_content)
                        we.event_step_checkpointed[current_step_id] = True

                    async with self.write_lock:
                        self.wait_list.remove((workflow_id, event_signature))

    '''
    async def notifyEvent(self, event_signature, content) -> None:
        # receive event from event listener
        if event_signature not in self.sigature_workflow_step.keys():
            raise ValueError(event_signature+" not found")

        for pair in self.sigature_workflow_step[event_signature]:
            (workflow_id, current_step_id) = pair
            if workflow_id not in self.event_registry.keys():
                raise ValueError(workflow_id+" not found")
            we = self.event_registry[workflow_id]
            outer_most_step_id = we.event_outer_most_step_id[current_step_id]
            await self.checkpointEvent(workflow_id, current_step_id, outer_most_step_id, content)
            we.event_step_checkpointed[current_step_id] = True
    '''

    async def checkpointEvent(self, workflow_id, current_step_id, outer_most_step_id, content) -> None:
        ws = WorkflowStorage(workflow_id, storage.create_storage(self.wma.get_storage_url()))
        ws.save_step_output(
            current_step_id, content, exception=None, outer_most_step_id=outer_most_step_id
        )

def init_event_coordinator_actor() -> None:
    """Initialize EventCoordinatorActor"""
    workflow_manager = get_management_actor()
    event_coordinator = EventCoordinatorActor.options(
        name=common.EVENT_COORDINATOR_NAME,
        namespace=common.EVENT_COORDINATOR_NAMESPACE,
        lifetime="detached",
    ).remote(workflow_manager)
    actor_handle = ray.get_actor(common.EVENT_COORDINATOR_NAME, namespace=common.EVENT_COORDINATOR_NAMESPACE)

def get_event_coordinator_actor() -> "ActorHandle":
    return ray.get_actor(
        common.EVENT_COORDINATOR_NAME, namespace=common.EVENT_COORDINATOR_NAMESPACE
    )

def get_or_create_event_coordinator_actor() -> "ActorHandle":
    """Get or create EventCoordinatorActor"""
    try:
        event_coordinator = get_event_coordinator_actor()
    except ValueError:
        store = storage.get_global_storage()
        # the actor does not exist
        logger.warning(
            "Cannot access event coordinator. It could be because "
            "the event coordinator exited unexpectedly. A new "
            "event coordinator is being created. "
        )
        event_coordinator = EventCoordinatorActor.options(
            name=common.EVENT_COORDINATOR_NAME,
            namespace=common.EVENT_COORDINATOR_NAMESPACE,
            lifetime="detached",
        ).remote(get_management_actor())
    return event_coordinator
