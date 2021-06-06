import contextlib
from typing import List, Any
import ray


def _resolve_workflow_outputs(index: int) -> Any:
    raise ValueError("There is no context for resolving workflow outputs.")


def _resolve_objectrefs(index: int) -> ray.ObjectRef:
    raise ValueError("There is no context for resolving object refs.")


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
