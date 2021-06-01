import functools
from typing import List, Tuple, Union, Any, Dict, Callable, Optional
import uuid

import ray

from ray.experimental.workflow import workflow_context

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


def wrap_step_outputs(
        outputs: WorkflowOutputType) -> Tuple[WorkflowOutputType, List[str]]:
    """
    This function converts returned object refs to workflow
    step results. It also collects hex strings of these refs.

    Args:
        outputs: The outputs of a Ray remote call.

    Returns:
        Corresponding WorkflowStepResult objects and
        related hex string.
    """
    if isinstance(outputs, RRef):
        results = [outputs]
    elif isinstance(outputs, tuple):
        results = outputs
    else:
        raise TypeError("Unknown return type")
    output_ids = [o.hex() for o in results]
    return outputs, output_ids


def resolve_step_inputs(args: List[RRef], kwargs: Dict[str, RRef]
                        ) -> Tuple[List, Dict, List[RRef]]:
    """
    This function resolves the inputs for the code inside
    a workflow step (works on the callee side).
    For each ObjectRef argument, the function returns both the ObjectRef
    and the object instance. If the ObjectRef is a chain of nested
    ObjectRefs, then we resolve it recursively until we get the
    object instance, and we return the *direct* ObjectRef of the
    instance. This function does not resolve ObjectRef
    inside another object (e.g. list of ObjectRefs) to give users some
    flexibility.

    Args:
        args: List of workflow step input arguments.
        kwargs: Dict of workflow step input arguments.

    Returns:
        Instances of arguments and resolved object refs.
    """

    resolved_object_refs = []
    _args = []
    _kwargs = {}
    for a in args:
        if isinstance(a, RRef):
            obj, ref = resolve_object_ref(a)
            _args.append(obj)
            resolved_object_refs.append(ref)
        else:
            _args.append(a)
    for k, v in kwargs:
        if isinstance(v, RRef):
            obj, ref = resolve_object_ref(v)
            _kwargs[k] = obj
            resolved_object_refs.append(ref)
        else:
            _kwargs[k] = v
    return _args, _kwargs, resolved_object_refs


class WorkflowStepFunction:
    def __init__(self, func: Callable):
        def _func(context, task_id, args, kwargs):
            # NOTE: must use 'set_current_store_dir' to ensure that we are
            # accessing the correct global variable.
            workflow_context.set_workflow_step_context(context)
            scope = workflow_context.get_scope()
            scope.append(task_id)
            args, kwargs, resolved_object_refs = resolve_step_inputs(
                args, kwargs)
            # free references to potentially save memory
            del resolved_object_refs

            output = func(*args, **kwargs)
            if isinstance(output, Workflow):
                output = output.execute()
            return output

        self.func = func
        self._remote_function = ray.remote(_func)

        # Override signature and docstring
        @functools.wraps(func)
        def _build_workflow(*args, **kwargs) -> Workflow:
            # TODO(suquark): we can validate if the input arguments match
            # the signature of the original function here.
            return Workflow(self._run_step, args, kwargs)

        self.step = _build_workflow

    def _run_step(self, args, kwargs):
        task_id = uuid.uuid4()
        refs = self._remote_function.remote(
            workflow_context.get_workflow_step_context(), task_id, args,
            kwargs)
        outputs, output_ids = wrap_step_outputs(refs)
        return outputs


class Workflow:
    def __init__(self, step_func: Callable, args: Tuple,
                 kwargs: Dict[str, Any]):
        self.args: List[RRef] = []
        self.kwargs: Dict[str, RRef] = {}
        # NOTE: we must serialize the inputs here, so they cannot be
        # mutable later.
        for arg in args:
            if isinstance(arg, (ray.ObjectRef, Workflow)):
                self.args.append(arg)
            else:
                self.args.append(ray.put(arg))
        for k, v in kwargs:
            if isinstance(v, (ray.ObjectRef, Workflow)):
                self.kwargs[k] = v
            else:
                self.kwargs[k] = ray.put(v)
        self.step_func = step_func
        self._executed = False
        self._output: Optional[WorkflowOutputType] = None

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
        input_args = []
        input_kwargs = {}
        for a in self.args:
            if isinstance(a, Workflow):
                input_args.append(a.execute())
            else:
                input_args.append(a)
        for k, v in self.kwargs.items():
            if isinstance(v, Workflow):
                input_kwargs[k] = v.execute()
            else:
                input_kwargs[k] = v
        output = self.step_func(input_args, input_kwargs)
        if not isinstance(output, WorkflowOutputType):
            raise TypeError("Unexpected return type of the workflow.")
        return output
