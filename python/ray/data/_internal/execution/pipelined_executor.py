from typing import Dict, List, Iterator, Optional

import ray
from ray.data.block import Block, BlockMetadata
from ray.data._internal.execution.interfaces import (
    Executor,
    RefBundle,
    PhysicalOperator,
    OneToOneOperator,
    AllToAllOperator,
    BufferOperator,
)
from ray.data._internal.execution.bulk_executor import _transform_one
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.stats import DatasetStats
from ray.types import ObjectRef


class _OpState:
    def __init__(self, num_inputs: int):
        self.inqueues: List[List[RefBundle]] = [[] for _ in range(num_inputs)]
        self.outqueue: List[RefBundle] = []


# TODO: reconcile with ComputeStrategy
class _OneToOneTask:
    def __init__(self, op: OneToOneOperator, state: _OpState, inputs: RefBundle):
        self._op: OneToOneOperator = op
        self._state: _OpState = state
        self._inputs: RefBundle = inputs
        self._block_ref: Optional[ObjectRef[Block]] = None
        self._meta_ref: Optional[ObjectRef[BlockMetadata]] = None

    def execute(self) -> ObjectRef:
        if len(self._inputs.blocks) != 1:
            raise NotImplementedError("TODO: multi-block inputs")
        self._block_ref, self._meta_ref = _transform_one.remote(
            self._op, self._inputs.blocks[0][0]
        )
        return self._meta_ref

    def completed(self):
        meta = ray.get(self._meta_ref)
        self._state.outqueue.append(RefBundle([(self._block_ref, meta)]))


class PipelinedExecutor(Executor):
    def execute(self, dag: PhysicalOperator) -> Iterator[RefBundle]:
        """Executes the DAG using a fully pipelined strategy.

        TODO: optimize memory usage by deleting intermediate results and marking
        the `owned` field in the ref bundles correctly.

        TODO: implement order preservation.
        """

        # TODO: implement parallelism control and autoscaling strategies.
        PARALLELISM_LIMIT = 2

        # TODO: make these class members so we can unit test this.
        operator_state: Dict[PhysicalOperator, _OpState] = {}
        candidate_tasks: Dict[PhysicalOperator, _OneToOneTask] = {}
        active_tasks: List[ObjectRef, _OneToOneTask] = {}

        # Setup the streaming topology.
        def setup_state(node) -> _OpState:
            if node in operator_state:
                return operator_state[node]

            # Create state if it doesn't exist.
            state = _OpState(len(node.input_dependencies))
            operator_state[node] = state

            # Wire up the input outqueues to this node's inqueues.
            for i, parent in enumerate(node.input_dependencies):
                parent_state = setup_state(parent)
                state.inqueues[i] = parent_state.outqueue

            return state

        setup_state(dag)
        buffer_state_change = True

        while candidate_tasks or active_tasks or buffer_state_change:
            buffer_state_change = False

            # Process completed tasks.
            if active_tasks:
                [ref], _ = ray.wait(list(active_tasks), num_returns=1, fetch_local=True)
                task = active_tasks.pop(ref)
                task.completed()

            # Generate new tasks.
            for op, state in operator_state.items():
                if isinstance(op, OneToOneOperator):
                    assert len(state.inqueues) == 1, "OneToOne takes exactly 1 input"
                    inqueue = state.inqueues[0]
                    if inqueue and op not in candidate_tasks:
                        candidate_tasks[op] = _OneToOneTask(op, state, inqueue.pop(0))
                elif isinstance(op, AllToAllOperator):
                    assert len(state.inqueues) == 1, "AllToAll takes exactly 1 input"
                    raise NotImplementedError
                elif isinstance(op, BufferOperator):
                    for i, inqueue in enumerate(state.inqueues):
                        while inqueue:
                            op.add_next(state.inqueue.pop(0), input_index=i)
                            buffer_state_change = True
                    while op.has_next():
                        state.outqueue.append(op.get_next())
                        buffer_state_change = True
                else:
                    assert False, "Unknown operator type: {}".format(op)

            # Yield outputs.
            output = operator_state[dag]
            while output.outqueue:
                yield output.outqueue.pop(0)

            # Dispatch new tasks.
            for op, task in list(candidate_tasks.items()):
                if len(active_tasks) < PARALLELISM_LIMIT:
                    active_tasks[task.execute()] = task
                    del candidate_tasks[op]

    def get_stats() -> DatasetStats:
        raise NotImplementedError
