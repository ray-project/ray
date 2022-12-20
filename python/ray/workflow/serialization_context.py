import contextlib
from typing import List, Any, Dict

from ray.util.serialization import register_serializer, deregister_serializer
from ray.workflow.common import WorkflowRef


def _resolve_workflow_refs(index: int) -> Any:
    raise ValueError("There is no context for resolving workflow refs.")


@contextlib.contextmanager
def workflow_args_serialization_context(workflow_refs: List[WorkflowRef]) -> None:
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
        workflow_refs: Output list of workflows or references to workflows.
    """
    deduplicator: Dict[WorkflowRef, int] = {}

    def serializer(w):
        if w in deduplicator:
            return deduplicator[w]
        if isinstance(w, WorkflowRef):
            # The ref should be resolved by the workflow management actor
            # when treated as the input of a workflow, so we remove the ref here.
            w.ref = None
        i = len(workflow_refs)
        workflow_refs.append(w)
        deduplicator[w] = i
        return i

    register_serializer(
        WorkflowRef,
        serializer=serializer,
        deserializer=_resolve_workflow_refs,
    )

    try:
        yield
    finally:
        # we do not want to serialize Workflow objects in other places.
        deregister_serializer(WorkflowRef)


@contextlib.contextmanager
def workflow_args_resolving_context(workflow_ref_mapping: List[Any]) -> None:
    """
    This context resolves workflows and object refs inside workflow
    arguments into correct values.

    Args:
        workflow_ref_mapping: List of workflow refs.
    """
    global _resolve_workflow_refs
    _resolve_workflow_refs_bak = _resolve_workflow_refs
    _resolve_workflow_refs = workflow_ref_mapping.__getitem__

    try:
        yield
    finally:
        _resolve_workflow_refs = _resolve_workflow_refs_bak


class _KeepWorkflowRefs:
    def __init__(self, index: int):
        self._index = index

    def __reduce__(self):
        return _resolve_workflow_refs, (self._index,)


@contextlib.contextmanager
def workflow_args_keeping_context() -> None:
    """
    This context only read workflow arguments. Workflows inside
    are untouched and can be serialized again properly.
    """
    global _resolve_workflow_refs
    _resolve_workflow_refs_bak = _resolve_workflow_refs

    # we must capture the old functions to prevent self-referencing.
    def _keep_workflow_refs(index: int):
        return _KeepWorkflowRefs(index)

    _resolve_workflow_refs = _keep_workflow_refs

    try:
        yield
    finally:
        _resolve_workflow_refs = _resolve_workflow_refs_bak
