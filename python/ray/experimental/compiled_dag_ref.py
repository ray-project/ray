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
        self._reader_refs = (
            dag.dag_output_channels._reader_ref
            if dag.has_single_output
            else [channel._reader_ref for channel in dag.dag_output_channels]
        )

    def __repr__(self):
        return (
            f"CompiledDAGRef(_dag={self._dag}, "
            f"_execution_index={self._execution_index}, "
            f"_called={self._called}, "
            f"_reader_refs={self._reader_refs})"
        )

    def get(self):
        if self._called:
            raise ValueError("This CompiledDAGRef has already been called.")
        self._called = True
        return self._dag._execute_until(self._execution_index)
