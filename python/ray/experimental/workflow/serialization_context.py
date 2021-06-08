import contextlib
from typing import List, Any, Dict

import ray
import ray.cloudpickle
from ray.util.serialization import register_serializer, deregister_serializer

from ray.experimental.workflow.common import Workflow


def _resolve_workflow_outputs(index: int) -> Any:
    raise ValueError("There is no context for resolving workflow outputs.")


def _resolve_objectrefs(index: int) -> ray.ObjectRef:
    raise ValueError("There is no context for resolving object refs.")


@contextlib.contextmanager
def workflow_args_serialization_context(
        workflows: List[Workflow], object_refs: List[ray.ObjectRef]) -> None:
    workflow_deduplicator: Dict[Workflow, int] = {}
    objectref_deduplicator: Dict[ray.ObjectRef, int] = {}

    def workflow_serializer(workflow):
        if workflow in workflow_deduplicator:
            return workflow_deduplicator[workflow]
        i = len(workflows)
        workflows.append(workflow)
        workflow_deduplicator[workflow] = i
        return i

    register_serializer(
        Workflow,
        serializer=workflow_serializer,
        deserializer=_resolve_workflow_outputs)

    def objectref_serializer(rref):
        if rref in objectref_deduplicator:
            return objectref_deduplicator[rref]
        i = len(object_refs)
        object_refs.append(rref)
        objectref_deduplicator[rref] = i
        return i

    # override the default ObjectRef serializer
    ray_objectref_reducer_backup = ray.cloudpickle.CloudPickler.dispatch[
        ray.ObjectRef]
    register_serializer(
        ray.ObjectRef,
        serializer=objectref_serializer,
        deserializer=_resolve_objectrefs)

    try:
        yield
    finally:
        # we do not want to serialize Workflow objects in other places.
        deregister_serializer(Workflow)
        # restore original dispatch
        ray.cloudpickle.CloudPickler.dispatch[
            ray.ObjectRef] = ray_objectref_reducer_backup


@contextlib.contextmanager
def workflow_args_resolving_context(
        workflow_output_mapping: List[Any],
        objectref_mapping: List[ray.ObjectRef]) -> None:
    """
    This context resolves workflows and objectrefs inside workflow
    arguments into correct values.

    Args:
        workflow_output_mapping: List of workflow outputs.
        objectref_mapping: List of object refs.
    """
    global _resolve_workflow_outputs, _resolve_objectrefs
    _resolve_workflow_outputs_bak = _resolve_workflow_outputs
    _resolve_objectrefs_bak = _resolve_objectrefs

    _resolve_workflow_outputs = workflow_output_mapping.__getitem__
    _resolve_objectrefs = objectref_mapping.__getitem__

    try:
        yield
    finally:
        _resolve_workflow_outputs = _resolve_workflow_outputs_bak
        _resolve_objectrefs = _resolve_objectrefs_bak
