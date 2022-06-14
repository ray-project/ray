import logging
import time
from typing import Any, Dict, List, Tuple, Optional, TYPE_CHECKING
import asyncio
import threading

import ray
from ray.workflow import common, recovery, step_executor, storage, workflow_storage, workflow_access
from ray.util.annotations import PublicAPI
from ray.workflow.event_listener import EventListener, EventListenerType, TimerListener
from ray.workflow.workflow_access import (
    flatten_workflow_output,
    get_or_create_management_actor,
    get_management_actor,
)
from ray.workflow.storage import Storage
from ray.workflow.workflow_storage import WorkflowStorage
from ray.workflow import serialization
from ray.workflow import serialization_context

if TYPE_CHECKING:
    from ray.actor import ActorHandle
    from ray.workflow.common import StepID, WorkflowExecutionResult

logger = logging.getLogger(__name__)

@ray.remote(num_cpus=0)
class EventCoordinatorActor:
    """Track event steps and checkpoint arrived data before resuming a workflow """

    def __init__(self, wma: "workflow_access.WorkflowManagementActor"):
        import nest_asyncio
        nest_asyncio.apply()

        self.wma = wma
        self.event_registry: Dict[str, Dict[str, asyncio.Future]] = {}
        self.transfer_lock = asyncio.Lock()

    async def poll_event_checkpoint_then_resume \
        (self, job_id: str, workflow_id:str, current_step_id:str, outer_most_step_id:str, \
        event_listener_type:EventListener, *args, **kwargs) -> Tuple[str, str]:
        """Poll event listener; checkpoint event; resume the workflow """

        event_listener = event_listener_type()
        event_content = await event_listener.poll_for_event(*args, **kwargs)
        await self.checkpointEvent(workflow_id, current_step_id, outer_most_step_id, event_content)
        ray.get(self.wma.run_or_resume.remote(job_id, workflow_id, ignore_existing = True, is_eca_initiated = True))

        return (workflow_id, current_step_id)

    async def transferEventStepOwnership(self, \
        job_id: str, workflow_id:str, current_step_id:str, outer_most_step_id:str, \
        event_listener_type:EventListener, *args, **kwargs) -> str:
        """Transfer one event step from workflow execution to event coordinator.

        Args:
            job_id: The ID of the job.
            workflow_id: The ID of the workflow.
            current_step_id: The ID of the current event step.
            outer_most_step_id: The ID of the outer most step.
            event_listener_type: The event listener class used to connect to the event provider.
            *args, **kwargs: Optional parameters, such as connection info, to be passed to the event listener.

        Returns:
            "REGISTERED"
        """

        async with self.transfer_lock:
            if workflow_id not in self.event_registry:
                self.event_registry[workflow_id] = {}
            if current_step_id not in self.event_registry[workflow_id]:
                self.event_registry[workflow_id][current_step_id] = asyncio.ensure_future( \
                self.poll_event_checkpoint_then_resume(job_id, workflow_id, current_step_id, outer_most_step_id, \
                event_listener_type, *args, **kwargs))

        return "REGISTERED"

    async def checkpointEvent(self, \
        workflow_id:str, current_step_id:str, outer_most_step_id:str, content:Any) -> None:
        """Save received event content to workflow checkpoint """

        ws = WorkflowStorage(workflow_id)
        ws.save_step_output(
            current_step_id, content, exception=None, outer_most_step_id=outer_most_step_id
        )

    async def cancelWorkflowListeners(self, workflow_id:str) -> str:
        """Cancel all event listeners associated with the workflow.
        Args:
            workflow_id: The ID of the workflow.

        Returns:
            "CANCELLED"
        """

        async with self.write_lock:
            if workflow_id in self.event_registry.keys():
                listeners = self.event_registry[workflow_id]
                for v in listeners.values():
                    if not v.done():
                        v.cancel()
            self.event_registry.pop(workflow_id)
        return "CANCELED"

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
