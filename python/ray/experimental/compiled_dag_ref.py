import traceback

import ray


class CompiledDAGRef:
    """
    A reference to a compiled DAG execution result.

    A CompiledDAGRef resembles an ObjectRef in the most common way.
    For example, similar to ObjectRef, ray.get() can be called on
    it to retrieve result. However, there are several major differences:
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
                "on a CompiledDAGRef and it was already called."
            )
        self._ray_get_called = True
        return self._dag._execute_until(self._execution_index)


class RayDAGTaskError:
    """
    Wraps an exception that occurred during the execution of a DAG.
    """

    def __init__(self, exc):
        """
        Args:
            exc: The exception that occurred during the execution of the DAG.
        """
        self._cause = exc
        self._backtrace = ray._private.utils.format_error_message(
            "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
            task_exception=True,
        )

    def __str__(self):
        return "Exception occurred during DAG execution:\n" + self._backtrace

    @property
    def cause(self):
        return self._cause
