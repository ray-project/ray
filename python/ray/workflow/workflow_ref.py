from typing import Union, List, Any
import logging

import ray
from ray.workflow import recovery
from ray.workflow import workflow_context
from ray.workflow import workflow_storage
from ray.workflow.common import WorkflowRef
from ray.workflow.workflow_access import get_or_create_management_actor

logger = logging.getLogger(__name__)


def _resolve_object_ref(ref: ray.ObjectRef) -> Any:
    """
    Resolves the ObjectRef into the object instance.

    This applies to a nested object ref recursively.

    Returns:
        The object instance.
    """
    while isinstance(ref, (WorkflowRef, ray.ObjectRef)):
        if isinstance(ref, ray.ObjectRef):
            ref = ray.get(ref)
            continue
        if ref.is_dynamic:
            raise ValueError(
                "Cannot process dynamic workflow output reference")
        if ref.is_resolved:
            ref = ref.output
        else:
            ref = ray.get(ref.output_ref)
    return ref


def _resolve_dynamic_workflow_refs(workflow_refs: "List[WorkflowRef]"):
    """Get the output of a workflow step with the step ID at runtime.

    We lookup the output by the following order:
    1. Query cached step output in the workflow manager. Fetch the physical
       output object.
    2. If failed to fetch the physical output object, look into the storage
       to see whether the output is checkpointed. Load the checkpoint.
    3. If failed to load the checkpoint, resume the step and get the output.
    """
    workflow_manager = get_or_create_management_actor()
    context = workflow_context.get_workflow_step_context()
    workflow_id = context.workflow_id
    storage_url = context.storage_url
    workflow_ref_mapping = []
    for workflow_ref in workflow_refs:
        step_ref = ray.get(
            workflow_manager.get_cached_step_output.remote(
                workflow_id, workflow_ref.step_id))
        get_cached_step = False
        if step_ref is not None:
            try:
                output = _resolve_object_ref(step_ref)
                get_cached_step = True
            except Exception:
                get_cached_step = False
        if not get_cached_step:
            wf_store = workflow_storage.get_workflow_storage()
            try:
                output = wf_store.load_step_output(workflow_ref.step_id)
            except Exception:
                current_step_id = workflow_context.get_current_step_id()
                logger.warning("Failed to get the output of step "
                               f"{workflow_ref.step_id}. Trying to resume it. "
                               f"Current step: '{current_step_id}'")
                step_ref = recovery.resume_workflow_step(
                    workflow_id, workflow_ref.step_id, storage_url,
                    None).persisted_output
                output = _resolve_object_ref(step_ref.output_ref)
        workflow_ref_mapping.append(output)
    return workflow_ref_mapping


def resolve_workflow_refs(
        refs: Union[WorkflowRef, List[WorkflowRef]]) -> Union[Any, List[Any]]:
    """Resolve workflow ref(s) into result(s).

    This function returns the result(s) in the input order, but it may not
    resolve it in the input order if it contains dynamic workflow refs.

    Args:
        refs: A workflow ref or a list of workflow refs.

    Returns:
        The result(s) of the input workflow ref(s).
    """
    is_individual_ref = isinstance(refs, WorkflowRef)
    if is_individual_ref:
        refs = [refs]
    results = []
    dynamic_refs = []
    dynamic_refs_map = []
    for i, r in enumerate(refs):
        if r.has_output:
            results.append(r.output)
        elif r.output_ref is not None:
            results.append(_resolve_object_ref(r.output_ref))
        else:
            dynamic_refs.append(r)
            dynamic_refs_map.append(i)
            results.append(None)

    if dynamic_refs:
        dynamic_results = _resolve_dynamic_workflow_refs(dynamic_refs)
        assert len(dynamic_results) == len(dynamic_refs_map)
        for i, r in zip(dynamic_refs_map, dynamic_results):
            results[i] = r

    return results[0] if is_individual_ref else results
