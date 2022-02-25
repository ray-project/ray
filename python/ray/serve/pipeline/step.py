from types import FunctionType
from typing import Callable, Optional, Union

from ray.serve.pipeline.node import ExecutorPipelineNode, INPUT, PipelineNode
from ray.serve.pipeline.common import ExecutionMode, str_to_execution_mode, StepConfig


def _validate_step_args(*args, **kwargs):
    """Validate arguments passed into a step.

    Currently, these must only consist of other steps (including INPUT), and
    kwargs are not supported.
    """
    if len(kwargs):
        raise NotImplementedError("No kwargs support yet!")

    # Check arguments for incoming edges.
    for arg in args:
        if isinstance(arg, PipelineStep):
            raise TypeError(
                "PipelineSteps cannot be passed in directly, "
                "you need to call them with an input first. For "
                "example: instead of `my_step_2(my_step_1)`, try "
                "`my_step_2(my_step_1(pipeline.INPUT))`."
            )

        elif arg is INPUT:
            if len(args) > 1:
                raise ValueError("INPUT steps cannnot take argument other than INPUT.")

        elif not isinstance(arg, PipelineNode):
            raise TypeError(
                f"Only PipelineNodes supported as arguments, got {type(arg)}"
            )


class PipelineStep:
    def __init__(self, config: StepConfig):
        self._config = config

    @property
    def num_replicas(self) -> int:
        return self._config.num_replicas

    def options(self, *args, **kwargs):
        raise NotImplementedError(".options() not supported yet.")


class CallablePipelineStep(PipelineStep):
    """A step that is ready to be used in a pipeline.

    Wraps either a function or a class and its constructor args & kwargs.

    This should be used by calling it with a number of other pipeline steps
    as its arguments (or the INPUT step).
    """

    def __init__(self, callable_factory: Callable[[], Callable], config: StepConfig):
        super().__init__(config)
        assert callable(callable_factory)
        self._callable_factory = callable_factory

    def options(self, *args, **kwargs):
        raise NotImplementedError("No options yet!")

    def __call__(self, *args, **kwargs):
        _validate_step_args(*args, **kwargs)
        return ExecutorPipelineNode(
            self._callable_factory, self._config, incoming_edges=args
        )


class UninstantiatedClassPipelineStep(PipelineStep):
    """Represents a class step whose constructor has not been initialized.

    This must be called with constructor args & kwargs to return an
    CallablePipelineStep before it can actually be used.
    """

    def __init__(self, _class: Callable, config: StepConfig):
        super().__init__(config)
        self._class = _class

    def options(self, *args, **kwargs):
        raise NotImplementedError("No options yet!")

    def __call__(self, *args, **kwargs):
        return CallablePipelineStep(lambda: self._class(*args, **kwargs), self._config)


def step(
    _func_or_class: Optional[Callable] = None,
    execution_mode: Union[ExecutionMode, str] = ExecutionMode.LOCAL,
    num_replicas: int = 1,
) -> Callable[[Callable], PipelineStep]:
    """Decorator used to define a pipeline step.

    Args:
        execution_mode(:class:`ExecutionMode`): The execution mode for this
            step. Supported modes:

            - ExecutionMode.LOCAL (default): executes this step inline in
              the calling process.
            - ExecutionMode.TASKS: executes this step in Ray tasks.
            - ExecutionMode.ACTORS: executes this step in Ray actors.
        num_replicas (int): The number of Ray actors to start that
            will run this step (default to 1). Only valid when using
            ExecutionMode.ACTORS.

    Example:

    >>> @pipeline.step(execution_mode="actors", num_replicas=10)
        def my_step(*args):
            pass

    Returns:
        :class:`PipelineStep`
    """

    if isinstance(execution_mode, str):
        execution_mode = str_to_execution_mode(execution_mode)
    elif not isinstance(execution_mode, ExecutionMode):
        raise TypeError("execution_mode must be an ExecutionMode or str.")

    config = StepConfig(execution_mode=execution_mode, num_replicas=num_replicas)

    def decorator(_func_or_class):
        if isinstance(_func_or_class, FunctionType):
            return CallablePipelineStep(lambda: _func_or_class, config)
        else:
            return UninstantiatedClassPipelineStep(_func_or_class, config)

    # This handles both parametrized and non-parametrized usage of the
    # decorator. See the @serve.batch code for more details.
    return decorator(_func_or_class) if callable(_func_or_class) else decorator
