from ray.workflow import serialization_context
from ray.workflow.common import (Workflow, WorkflowData, StepType,
                                 ensure_ray_initialized,
                                 WorkflowStepRuntimeOptions,
                                 WorkflowInputs)
from ray.workflow.event_listener import EventListenerType, Event
from ray._private import signature

from typing import Any, Dict, List

def make_event(event_listener_type: EventListenerType, args: List[Any],
                   kwargs: Dict[str, Any]) -> Workflow[Event]:

    step_options = WorkflowStepRuntimeOptions.make(step_type=StepType.EVENT)

    def make_and_invoke_poll(*args, **kwargs):
        return make_event_listener().poll_for_event(*args, **kwargs)


    print("flattening args ", make_and_invoke_poll, args, kwargs)
    flattened_args = signature.flatten_args(signature.extract_signature(event_listener_type), args,
                                            kwargs)

    def prepare_inputs():
        ensure_ray_initialized()
        return serialization_context.make_workflow_inputs(
            flattened_args)

    workflow_data = WorkflowData(
        func_body=event_listener_type,
        inputs=None,
        step_options=step_options,
        name="TODO",
        user_metadata={"TODO": "TODO"})

    return Workflow(workflow_data, prepare_inputs)
