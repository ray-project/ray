from typing import Any, Callable, Dict, Set, Tuple

from ray.serve.pipeline.node import PipelineNode

class PipelineStep:
    def __init__(self, _func_or_class: Callable, num_replicas: int = 1):
        self._func_or_class = _func_or_class
        self._num_replicas = num_replicas

    def options(self, *args, **kwargs):
        raise NotImplementedError("No options yet!")

    def __call__(self, *args, **kwargs):
        return PipelineNode(*args, **kwargs)

def step(_func_or_class: Optional[Callable] = None,
        num_replicas: Optional[int] = None) -> Callable[[Callable], Deployment]:
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


    def decorator(_func_or_class):
        return PipelineStep(_func_or_class, num_replicas=num_replicas)

    # This handles both parametrized and non-parametrized usage of the
    # decorator. See the @serve.batch code for more details.
    return decorator(_func_or_class) if callable(_func_or_class) else decorator
