import logging
import time
from typing import Union, Optional

import ray

from ray.experimental.workflow import workflow_context
from ray.experimental.workflow.common import Workflow
from ray.experimental.workflow.step_executor import commit_step
from ray.experimental.workflow.storage import (
    Storage, create_storage, get_global_storage, set_global_storage)
from ray.experimental.workflow.workflow_access import (
    WorkflowManagementActor, MANAGEMENT_ACTOR_NAME, flatten_workflow_output)

logger = logging.getLogger(__name__)


def run(entry_workflow: Workflow,
        storage: Optional[Union[str, Storage]] = None,
        workflow_id: Optional[str] = None) -> ray.ObjectRef:
    """Run a workflow asynchronously. See "api.run()" for details."""
    if workflow_id is None:
        # Workflow ID format: {Entry workflow UUID}.{Unix time to nanoseconds}
        workflow_id = f"{entry_workflow.id}.{time.time():.9f}"
    if isinstance(storage, str):
        set_global_storage(create_storage(storage))
    elif isinstance(storage, Storage):
        set_global_storage(storage)
    elif storage is not None:
        raise TypeError("'storage' should be None, str, or Storage type.")
    storage_url = get_global_storage().storage_url
    logger.info(f"Workflow job created. [id=\"{workflow_id}\", storage_url="
                f"\"{storage_url}\"].")
    try:
        workflow_context.init_workflow_step_context(workflow_id, storage_url)
        commit_step(entry_workflow)
        try:
            actor = ray.get_actor(MANAGEMENT_ACTOR_NAME)
        except ValueError:
            # the actor does not exist
            actor = WorkflowManagementActor.options(
                name=MANAGEMENT_ACTOR_NAME, lifetime="detached").remote()
        # NOTE: It is important to 'ray.get' the returned output. This
        # ensures caller of 'run()' holds the reference to the workflow
        # result. Otherwise if the actor removes the reference of the
        # workflow output, the caller may fail to resolve the result.
        output = ray.get(actor.run_or_resume.remote(workflow_id, storage_url))
        direct_output = flatten_workflow_output(workflow_id, output)
    finally:
        workflow_context.set_workflow_step_context(None)
    return direct_output


# TODO(suquark): support recovery with ObjectRef inputs.


def resume(workflow_id: str,
           storage: Optional[Union[str, Storage]] = None) -> ray.ObjectRef:
    """Resume a workflow asynchronously. See "api.resume()" for details.
    """
    if isinstance(storage, str):
        store = create_storage(storage)
    elif isinstance(storage, Storage):
        store = storage
    elif storage is None:
        store = get_global_storage()
    else:
        raise TypeError("'storage' should be None, str, or Storage type.")
    logger.info(f"Resuming workflow [id=\"{workflow_id}\", storage_url="
                f"\"{store.storage_url}\"].")
    try:
        actor = ray.get_actor(MANAGEMENT_ACTOR_NAME)
    except ValueError:
        # the actor does not exist
        actor = WorkflowManagementActor.options(
            name=MANAGEMENT_ACTOR_NAME, lifetime="detached").remote()
    # NOTE: It is important to 'ray.get' the returned output. This
    # ensures caller of 'run()' holds the reference to the workflow
    # result. Otherwise if the actor removes the reference of the
    # workflow output, the caller may fail to resolve the result.
    output = ray.get(
        actor.run_or_resume.remote(workflow_id, store.storage_url))
    direct_output = flatten_workflow_output(workflow_id, output)
    logger.info(f"Workflow job {workflow_id} resumed.")
    return direct_output


def get_output(workflow_id: str) -> ray.ObjectRef:
    """Get the output of a running workflow.
    See "api.get_output()" for details.
    """
    try:
        actor = ray.get_actor(MANAGEMENT_ACTOR_NAME)
    except ValueError as e:
        raise ValueError(
            "Failed to connect to the workflow management "
            "actor. The workflow could have already failed. You can use "
            "workflow.resume() to resume the workflow.") from e
    output = ray.get(actor.get_output.remote(workflow_id))
    return flatten_workflow_output(workflow_id, output)
