import ray


class CompiledDAGRef(ray.ObjectRef):
    def __init__(
        self,
        dag: "ray.experimental.CompiledDAG",
        execution_index: int,
    ):
        self._dag = dag
        self._execution_index = execution_index
        self._called = False

    def get(self):
        if self._called:
            raise ValueError("This CompiledDAGRef has already been called.")
        self._called = True
        return self._dag._execute_until(self._execution_index)
