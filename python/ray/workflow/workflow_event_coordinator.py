import logging
import time
from typing import Any, Dict, List, Tuple, Optional, TYPE_CHECKING
import asyncio

from dataclasses import dataclass
import ray
from ray.workflow import common
from ray.workflow import recovery
from ray.workflow import storage
from ray.workflow import workflow_storage
from ray.workflow import workflow_access
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.actor import ActorHandle
    from ray.workflow.common import StepID, WorkflowExecutionResult

logger = logging.getLogger(__name__)

@ray.remote(num_cpus=0)
class EventCoordinatorActor:
    def __init__(self, wma: "workflow_access.WorkflowManagementActor"):
        import nest_asyncio
        nest_asyncio.apply()
        self.wma = wma
        self.event_registry = dict()
    async def transferEventStepOwnership(self, event_provider_handle, event_signature, \
        workflow_id, current_step_id, outer_most_step_id):
        # register actor handle with event_provider
        self.event_registry[event_signature] = (workflow_id, current_step_id, outer_most_step_id)
        event_provider_handle.register_eca.remote(common.EVENT_COORDINATOR_NAME)

    async def notifyEvent(self, event_signature, content):
        # receive event from Provider
        await self.checkpointEvent(event_signature, content)

    async def checkpointEvent(self, event_signature, event_content) -> None:
        if event_signature in self.event_registry:
            (workflow_id, current_step_id, outer_most_step_id) = self.event_registry[event_signature]
            ws = WorkflowStorage(workflow_id, storage.create_storage(self.wma.get_storage_url()))
            ws.save_step_output(
                current_step_id, event_content, exception=None, outer_most_step_id=outer_most_step_id
            )
        else:
            # handle event signature not found
            pass

def init_event_coordinator_actor() -> None:
    """Initialize EventCoordinatorActor"""
    workflow_manager = get_management_actor()
    event_coordinator = EventCoordinatorActor.options(
        name=common.EVENT_COORDINATOR_NAME,
        namespace=common.EVENT_COORDINATOR_NAMESPACE,
        lifetime="detached",
    ).remote(workflow_manager)
    actor_handle = ray.get_actor(common.EVENT_COORDINATOR_NAME)

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
            "Cannot access workflow manager. It could be because "
            "the workflow manager exited unexpectedly. A new "
            "workflow manager is being created with storage "
            f"'{store}'."
        )
        event_coordinator = EventCoordinatorActor.options(
            name=common.EVENT_COORDINATOR_NAME,
            namespace=common.EVENT_COORDINATOR_NAMESPACE,
            lifetime="detached",
        ).remote(get_management_actor())
    return event_coordinator
