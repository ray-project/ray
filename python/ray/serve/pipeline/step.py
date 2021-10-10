from types import FunctionType
from typing import Any, Callable, Dict, Optional, Tuple

from pydantic import BaseModel, PositiveInt, validator

from ray.serve.pipeline.node import PipelineNode, INPUT


class StepConfig(BaseModel):
    num_replicas: Optional[PositiveInt] = None

    class Config:
        validate_assignment = True

    @validator("num_replicas")
    def set_num_replicas(cls, num_replicas: Optional[PositiveInt]):
        return num_replicas if num_replicas is not None else 1


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
        return PipelineNode(lambda: self._func, incoming_edges=args)


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
            lambda: self._class(*self._class_args, **self._class_kwargs),
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
         num_replicas: Optional[int] = None
         ) -> Callable[[Callable], PipelineStep]:
    """Define a pipeline step.

    Args:
        num_replicas (int): The number of Ray actors to start that
            will run this step. Defaults to 1.

    Example:

    >>> @pipeline.step(num_replicas=10)
        def my_step(*args):
            pass

    Returns:
        PipelineStep
    """

    config = StepConfig(num_replicas=num_replicas)

    def decorator(_func_or_class):
        if isinstance(_func_or_class, FunctionType):
            return FunctionPipelineStep(_func_or_class, config)
        else:
            return UninstantiatedClassPipelineStep(_func_or_class, config)

    # This handles both parametrized and non-parametrized usage of the
    # decorator. See the @serve.batch code for more details.
    return decorator(_func_or_class) if callable(_func_or_class) else decorator
