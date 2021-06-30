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
    """
    This serialization context reduces workflow input arguments to three
    parts:

    1. A workflow input placeholder. It is an object without 'Workflow' and
       'ObjectRef' object. They are replaced with integer indices. During
       deserialization, we can refill the placeholder with a list of
       'Workflow' and a list of 'ObjectRef'. This provides us great
       flexibility, for example, during recovery we can plug an alternative
       list of 'Workflow' and 'ObjectRef', since we lose the original ones.
    2. A list of 'Workflow'. There is no duplication in it.
    3. A list of 'ObjectRef'. There is no duplication in it.

    We do not allow duplication because in the arguments duplicated workflows
    and object refs are shared by reference. So when deserialized, we also
    want them to be shared by reference. See
    "tests/test_object_deref.py:deref_shared" as an example.

    The deduplication works like this:
        Inputs: [A B A B C C A]
        Output List: [A B C]
        Index in placeholder: [0 1 0 1 2 2 0]

    Args:
        workflows: Workflow list output.
        object_refs: ObjectRef list output.
    """
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
    # TODO(suquark): We are using Ray internal APIs to access serializers.
    # This is only a workaround. We need alternatives later.
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


class _KeepWorkflowOutputs:
    def __init__(self, index: int):
        self._index = index

    def __reduce__(self):
        return _resolve_workflow_outputs, (self._index, )


class _KeepObjectRefs:
    def __init__(self, index: int):
        self._index = index

    def __reduce__(self):
        return _resolve_objectrefs, (self._index, )


@contextlib.contextmanager
def workflow_args_keeping_context() -> None:
    """
    This context only read workflow arguments. Workflows and objectrefs inside
    are untouched and can be serialized again properly.
    """
    global _resolve_workflow_outputs, _resolve_objectrefs
    _resolve_workflow_outputs_bak = _resolve_workflow_outputs
    _resolve_objectrefs_bak = _resolve_objectrefs

    # we must capture the old functions to prevent self-referencing.
    def _keep_workflow_outputs(index: int):
        return _KeepWorkflowOutputs(index)

    def _keep_objectrefs(index: int):
        return _KeepObjectRefs(index)

    _resolve_workflow_outputs = _keep_workflow_outputs
    _resolve_objectrefs = _keep_objectrefs

    try:
        yield
    finally:
        _resolve_workflow_outputs = _resolve_workflow_outputs_bak
        _resolve_objectrefs = _resolve_objectrefs_bak
