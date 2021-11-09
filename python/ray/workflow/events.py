import logging
import ray
from ray.workflow import workflow_context
from ray.workflow import serialization_context
from ray.workflow.common import (
    Workflow,
    WorkflowData,
    StepType,
    ensure_ray_initialized,
    WorkflowStepRuntimeOptions,
    WorkflowInputs,
    EVENT_ACTOR_NAME,
    MANAGEMENT_ACTOR_NAMESPACE,
)
from ray.workflow.event_listener import EventListenerType, Event
from ray._private import signature

from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def make_event(
    event_listener_type: EventListenerType, args: List[Any], kwargs: Dict[str, Any]
) -> Workflow[Event]:

    step_options = WorkflowStepRuntimeOptions.make(step_type=StepType.EVENT)

    def make_and_invoke_poll(*args, **kwargs):
        return make_event_listener().poll_for_event(*args, **kwargs)

    print("flattening args ", make_and_invoke_poll, args, kwargs)
    flattened_args = signature.flatten_args(
        signature.extract_signature(event_listener_type), args, kwargs
    )

    def prepare_inputs():
        ensure_ray_initialized()
        return serialization_context.make_workflow_inputs(flattened_args)

    workflow_data = WorkflowData(
        func_body=event_listener_type,
        inputs=None,
        step_options=step_options,
        name="TODO",
        user_metadata={"TODO": "TODO"},
    )

    return Workflow(workflow_data, prepare_inputs)


def init_manager() -> None:
    get_or_create_manager(warn_on_creation=False)


def get_or_create_manager(warn_on_creation=False) -> "ActorHandle":
    try:
        return ray.get_actor(EVENT_ACTOR_NAME, namespace=MANAGEMENT_ACTOR_NAMESPACE)
    except ValueError:
        if warn_on_creation:
            logger.warning(
                "Cannot access workflow event coordinator. It "
                "could be because "
                "the workflow manager exited unexpectedly. A new "
                "coordinator is being created."
            )
        handle = Manager.options(
            name=EVENT_ACTOR_NAME,
            namespace=MANAGEMENT_ACTOR_NAMESPACE,
            lifetime="detached",
        ).remote()
        ray.get(handle.ping.remote())
        return handle


@ray.remote(num_cpus=0)
def _resolve_args(event_data: WorkflowData, context, step_id, baked_inputs):
    # NOTE: This needs to be in a remote function since it invokes
    # `_resolve_step_inputs` which uses ray.get not await.

    # Setup the context.
    workflow_context.update_workflow_step_context(context, step_id)
    context = workflow_context.get_workflow_step_context()
    step_type = event_data.step_options.step_type

    # Resolve the function and args
    # TODO: Fix this circular dependency.
    from ray.workflow.step_executor import _resolve_step_inputs

    args, kwargs = _resolve_step_inputs(baked_inputs)
    return args, kwargs


@ray.remote(num_cpus=0)
class Manager:
    def __init__(self):
        self.events = set()
        self.dependencies = dict()

    async def ping(self):
        logger.info("PONG")
        pass

    async def register_workflow_dependency(
        self, workflow_id: str, event_data: WorkflowData, context, step_id, baked_inputs
    ):
        """Register that `workflow_id` depends on the step `event_data`'s result
        before it can continue to execute.

        Args:
            workflow_id (str): The id of the workflow to be resumed when the event occurs.
            event_data (WorkflowData): The information needed to begin to wait for the event.
        """

        args, kwargs = await _resolve_args.remote(
            event_data, context, step_id, baked_inputs
        )
        func = event_data.func_body

        # Execute function
        logger.info(f"Executing {func} with {args}, {kwargs}")
        func(*args, **kwargs)

        pass
