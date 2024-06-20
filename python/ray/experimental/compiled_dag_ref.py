import asyncio
from typing import Any, List

import ray
from ray.exceptions import RayTaskError
from ray.util.annotations import PublicAPI


def _process_return_vals(return_vals: List[Any], return_single_output: bool):
    # Check for exceptions.
    for val in return_vals:
        if isinstance(val, RayTaskError):
            raise val

    if return_single_output:
        return return_vals[0]

    return return_vals


@PublicAPI(stability="alpha")
class CompiledDAGRef(ray.ObjectRef):
    """
    A reference to a compiled DAG execution result.

    This is a subclass of ObjectRef and resembles ObjectRef in the
    most common way. For example, similar to ObjectRef, ray.get()
    can be called on it to retrieve result. However, there are several
    major differences:
    1. ray.get() can only be called once on CompiledDAGRef.
    2. ray.wait() is not supported.
    3. CompiledDAGRef cannot be copied, deep copied, or pickled.
    4. CompiledDAGRef cannot be passed as an argument to another task.
    """

    def __init__(
        self,
        dag: "ray.experimental.CompiledDAG",
        execution_index: int,
    ):
        """
        Args:
            dag: The compiled DAG that generated this CompiledDAGRef.
            execution_index: The index of the execution for the DAG.
                A DAG can be executed multiple times, and execution index
                indicates which execution this CompiledDAGRef corresponds to.
        """
        self._dag = dag
        self._execution_index = execution_index
        # Whether ray.get() was called on this CompiledDAGRef.
        self._ray_get_called = False
        self._dag_output_channels = dag.dag_output_channels

    def __str__(self):
        return (
            f"CompiledDAGRef({self._dag.get_id()}, "
            f"execution_index={self._execution_index})"
        )

    def __copy__(self):
        raise ValueError("CompiledDAGRef cannot be copied.")

    def __deepcopy__(self, memo):
        raise ValueError("CompiledDAGRef cannot be deep copied.")

    def __reduce__(self):
        raise ValueError("CompiledDAGRef cannot be pickled.")

    def __del__(self):
        # If not yet, get the result and discard to avoid execution result leak.
        if not self._ray_get_called:
            self.get()

    def get(self):
        if self._ray_get_called:
            raise ValueError(
                "ray.get() can only be called once "
                "on a CompiledDAGRef, and it was already called."
            )
        self._ray_get_called = True
        return_vals = self._dag._execute_until(self._execution_index)
        return _process_return_vals(
            return_vals,
            self._dag.has_single_output,
        )


@PublicAPI(stability="alpha")
class CompiledDAGFuture:
    def __init__(
        self,
        dag: "ray.experimental.CompiledDAG",
        execution_index: int,
        fut: "asyncio.Future",
    ):
        self._dag = dag
        self._execution_index = execution_index
        self._fut = fut

    def __str__(self):
        return (
            f"CompiledDAGFuture({self._dag.get_id()}, "
            f"execution_index={self._execution_index})"
        )

    def __copy__(self):
        raise ValueError("CompiledDAGFuture cannot be copied.")

    def __deepcopy__(self, memo):
        raise ValueError("CompiledDAGFuture cannot be deep copied.")

    def __reduce__(self):
        raise ValueError("CompiledDAGFuture cannot be pickled.")

    def __await__(self):
        if self._fut is None:
            raise ValueError(
                "CompiledDAGFuture can only be awaited upon once, and it has "
                "already been awaited upon."
            )

        # NOTE(swang): If the object is zero-copy deserialized, then it will
        # stay in scope as long as this future is in scope. Therefore, we
        # delete self._fut here before we return the result to the user.
        fut = self._fut
        self._fut = None

        return_vals = yield from fut.__await__()

        return _process_return_vals(
            return_vals,
            self._dag.has_single_output,
        )
