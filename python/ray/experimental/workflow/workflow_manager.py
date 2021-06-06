import functools
import inspect
from typing import List, Tuple, Union, Any, Dict, Callable, Optional
import uuid

import ray
import ray.serialization
import ray.cloudpickle
from ray.util.serialization import register_serializer, deregister_serializer

from ray.experimental.workflow import workflow_context
from ray.experimental.workflow import serialization_context

RRef = ray.ObjectRef  # Alias ObjectRef because it is too long in type hints.
WorkflowOutputType = RRef  # Alias workflow output type.
StepArgType = Union[RRef, Any]


def resolve_object_ref(ref: RRef) -> Tuple[Any, RRef]:
    """
    Resolves the ObjectRef into the object instance.

    Returns:
        The object instance and the direct ObjectRef to the instance.
    """
    assert ray.is_initialized()
    last_ref = ref
    while True:
        if isinstance(ref, RRef):
            last_ref = ref
        else:
            break
        ref = ray.get(last_ref)
    return ref, last_ref


def _deref_arguments(inputs: RRef) -> Tuple[List, Dict]:
    # deref arguments like ray remote functions
    args, kwargs = ray.get(inputs)
    _args = [ray.get(a) if isinstance(a, RRef) else a for a in args]
    _kwargs = {
        k: ray.get(v) if isinstance(v, RRef) else v
        for k, v in kwargs.items()
    }
    return _args, _kwargs


def _resolve_step_inputs(step_inputs: Tuple[RRef, List[RRef], List[RRef]]
                         ) -> Tuple[List, Dict, List[RRef]]:
    """
    This function resolves the inputs for the code inside
    a workflow step (works on the callee side). For outputs from other
    workflows, we resolve them into object instances inplace.

    For each ObjectRef argument, the function returns both the ObjectRef
    and the object instance. If the ObjectRef is a chain of nested
    ObjectRefs, then we resolve it recursively until we get the
    object instance, and we return the *direct* ObjectRef of the
    instance. This function does not resolve ObjectRef
    inside another object (e.g. list of ObjectRefs) to give users some
    flexibility.

    Args:
        step_inputs: Workflow step inputs.
    Returns:
        Instances of arguments and resolved object refs.
    """

    resolved_object_refs = []
    objects_mapping = []
    inputs, workflow_outputs, object_refs = step_inputs
    for rref in workflow_outputs:
        obj, ref = resolve_object_ref(rref)
        objects_mapping.append(obj)
        resolved_object_refs.append(ref)
    with serialization_context.workflow_args_resolving_context(
            objects_mapping, object_refs):
        _args, _kwargs = _deref_arguments(inputs)
    return _args, _kwargs, resolved_object_refs


class WorkflowStepFunction:
    def __init__(self, func: Callable):
        def _func(context, task_id, step_inputs):
            # NOTE: must use 'set_current_store_dir' to ensure that we are
            # accessing the correct global variable.
            workflow_context.set_workflow_step_context(context)
            scope = workflow_context.get_scope()
            scope.append(task_id)
            args, kwargs, resolved_object_refs = _resolve_step_inputs(
                step_inputs)
            # free references to potentially save memory
            del resolved_object_refs

            output = func(*args, **kwargs)
            if isinstance(output, Workflow):
                output = output.execute()
            return output

        self.func = func
        self._remote_function = ray.remote(_func)
        self._func_signature = list(
            inspect.signature(func).parameters.values())

        # Override signature and docstring
        @functools.wraps(func)
        def _build_workflow(*args, **kwargs) -> Workflow:
            # validate if the input arguments match the signature of the
            # original function.
            reconstructed_signature = inspect.Signature(
                parameters=self._func_signature)
            try:
                reconstructed_signature.bind(*args, **kwargs)
            except TypeError as exc:  # capture a friendlier stacktrace
                raise TypeError(str(exc)) from None
            return Workflow(self._run_step, args, kwargs)

        self.step = _build_workflow

    def _run_step(self, step_id, step_inputs):
        ref = self._remote_function.remote(
            workflow_context.get_workflow_step_context(), step_id, step_inputs)
        return ref

    def __call__(self, *args, **kwargs):
        raise TypeError("Workflow steps cannot be called directly. Instead "
                        f"of running '{self.step.__name__}()', "
                        f"try '{self.step.__name__}.step()'.")


class Workflow:
    def __init__(self, step_func: Callable, args: Tuple,
                 kwargs: Dict[str, Any]):
        # NOTE: we must serialize the inputs here, so they cannot be
        # mutable later.
        self._captured_workflows: List[Workflow] = []
        self._captured_objectrefs: List[RRef] = []
        with _CaptureWorkflowInputs(self._captured_workflows,
                                    self._captured_objectrefs):
            self._inputs: RRef = ray.put((args, kwargs))
        self.step_func = step_func
        self._executed = False
        self._output: Optional[WorkflowOutputType] = None
        self._step_id = uuid.uuid4()

    @property
    def executed(self) -> bool:
        return self._executed

    @property
    def output(self):
        if not self._executed:
            raise Exception("The workflow has not been executed.")
        return self._output

    def execute(self) -> WorkflowOutputType:
        """
        Trigger workflow execution recursively.
        """
        if self.executed:
            return self._output
        workflow_outputs = [w.execute() for w in self._captured_workflows]
        # NOTE: wrap '_inputs' inside a tuple to prevent dereference.
        step_inputs = (self._inputs, workflow_outputs,
                       self._captured_objectrefs)
        output = self.step_func(self._step_id, step_inputs)
        if not isinstance(output, WorkflowOutputType):
            raise TypeError("Unexpected return type of the workflow.")
        self._output = output
        self._executed = True
        return output

    def __reduce__(self):
        raise Exception("Workflow is not supposed to be serialized by pickle."
                        "Maybe you are passing it to a Ray remote function?")


class _CaptureWorkflowInputs:
    def __init__(self, captured_workflows: List[Workflow],
                 captured_object_refs: List[RRef]):
        self._captured_workflows = captured_workflows
        self._workflow_deduplicator: Dict[Workflow, int] = {}
        self._captured_objectrefs = captured_object_refs
        self._objectref_deduplicator: Dict[RRef, int] = {}
        self._objectref_reducer = None

    def __enter__(self):
        def workflow_serializer(workflow):
            if workflow in self._workflow_deduplicator:
                return self._workflow_deduplicator[workflow]
            i = len(self._captured_workflows)
            self._captured_workflows.append(workflow)
            self._workflow_deduplicator[workflow] = i
            return i

        register_serializer(
            Workflow,
            serializer=workflow_serializer,
            deserializer=serialization_context._resolve_workflow_outputs)

        def objectref_serializer(rref):
            if rref in self._objectref_deduplicator:
                return self._objectref_deduplicator[rref]
            i = len(self._captured_objectrefs)
            self._captured_objectrefs.append(rref)
            self._objectref_deduplicator[rref] = i
            return i

        self._objectref_reducer = ray.cloudpickle.CloudPickler.dispatch[RRef]
        # this would override the original serializer
        register_serializer(
            RRef,
            serializer=objectref_serializer,
            deserializer=serialization_context._resolve_objectrefs)

    def __exit__(self, exc_type, exc_val, exc_tb):
        # we do not want to serialize Workflow objects in other places.
        deregister_serializer(Workflow)
        # restore original dispatch
        ray.cloudpickle.CloudPickler.dispatch[RRef] = self._objectref_reducer
