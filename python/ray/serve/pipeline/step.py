from types import FunctionType
from typing import Any, Callable, Dict, Optional, Tuple

from ray.serve.pipeline.node import CallableNodeFactory, INPUT, PipelineNode
from ray.serve.pipeline.common import StepConfig


def validate_step_args(*args, **kwargs):
    """Checks that all args are PipelineNodes."""
    if len(kwargs):
        raise NotImplementedError("No kwargs support yet!")

    # Check arguments for incoming edges.
    for arg in args:
        if isinstance(arg, PipelineStep):
            raise TypeError("PipelineSteps cannot be passed in directly, "
                            "you need to call them with an input first. For "
                            "example: instead of `my_step_2(my_step_1)`, try "
                            "`my_step_2(my_step_1(pipeline.INPUT))`.")

        elif arg is INPUT:
            if len(args) > 1:
                raise ValueError(
                    "INPUT steps can only take a single argument.")

        elif not isinstance(arg, PipelineNode):
            raise TypeError(
                f"Only PipelineNodes supported as arguments, got {type(arg)}")


class PipelineStep:
    def __init__(self, config: StepConfig):
        self._config = config

    @property
    def num_replicas(self) -> int:
        return self._config.num_replicas

    def options(self, *args, **kwargs):
        raise NotImplementedError(".options() not supported yet.")


class FunctionPipelineStep(PipelineStep):
    def __init__(self, func: Callable, config: StepConfig):
        super().__init__(config)
        assert isinstance(func, FunctionType)
        self._func = func

    def options(self, *args, **kwargs):
        raise NotImplementedError("No options yet!")

    def __call__(self, *args, **kwargs):
        validate_step_args(*args, **kwargs)
        return PipelineNode(
            CallableNodeFactory(lambda: self._func, self._config),
            incoming_edges=args)


class InstantiatedClassPipelineStep(PipelineStep):
    def __init__(self, _class: Callable, class_args: Tuple[Any],
                 class_kwargs: Dict[Any, Any], config: StepConfig):
        super().__init__(config)
        self._class = _class
        self._class_args = class_args
        self._class_kwargs = class_kwargs

    def options(self, *args, **kwargs):
        raise NotImplementedError("No options yet!")

    def __call__(self, *args, **kwargs):
        validate_step_args(*args, **kwargs)
        return PipelineNode(
            CallableNodeFactory(
                lambda: self._class(*self._class_args, **self._class_kwargs),
                self._config),
            incoming_edges=args)


class UninstantiatedClassPipelineStep(PipelineStep):
    def __init__(self, _class: Callable, config: StepConfig):
        super().__init__(config)
        self._class = _class

    def options(self, *args, **kwargs):
        raise NotImplementedError("No options yet!")

    def __call__(self, *args, **kwargs):
        return InstantiatedClassPipelineStep(self._class, args, kwargs,
                                             self._config)


def step(_func_or_class: Optional[Callable] = None,
         num_replicas: int = 1,
         inline: bool = True) -> Callable[[Callable], PipelineStep]:
    """Define a pipeline step.

    Args:
        num_replicas (int): The number of Ray actors to start that
            will run this step. Defaults to 1.
        inline (bool): If true, this step will be run inline in the driver
            process as a normal function call.

    Example:

    >>> @pipeline.step(num_replicas=10)
        def my_step(*args):
            pass

    Returns:
        PipelineStep
    """

    config = StepConfig(num_replicas=num_replicas, inline=inline)

    def decorator(_func_or_class):
        if isinstance(_func_or_class, FunctionType):
            return FunctionPipelineStep(_func_or_class, config)
        else:
            return UninstantiatedClassPipelineStep(_func_or_class, config)

    # This handles both parametrized and non-parametrized usage of the
    # decorator. See the @serve.batch code for more details.
    return decorator(_func_or_class) if callable(_func_or_class) else decorator
