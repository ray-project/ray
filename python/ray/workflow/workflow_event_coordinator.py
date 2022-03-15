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
    def transferEventStepOwnership(self, event_provider_handle, workflow_id, current_step_id, outer_most_step_id):
        # register actor handle with event_provider
        pass
    async def notifyEvent(self, event):
        # receive event from Provider
        pass


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
