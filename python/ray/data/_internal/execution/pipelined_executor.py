from typing import Dict, List, Iterator, Optional, Any

import ray
from ray.data.block import Block, BlockMetadata, BlockAccessor
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.interfaces import (
    Executor,
    ExecutionOptions,
    RefBundle,
    PhysicalOperator,
    ExchangeOperator,
)
from ray.data._internal.execution.bulk_executor import _transform_one
from ray.data._internal.execution.operators import InputDataBuffer, OneToOneOperator
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.stats import DatasetStats
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.types import ObjectRef


@ray.remote
class _Actor:
    @ray.method(num_returns=2)
    def transform_one(self, op, block):
        [out] = list(op.execute_one([block], {}))
        return out, BlockAccessor.for_block(out).get_metadata([], None)


class _ActorPool:
    def __init__(
        self,
        size: int,
        ray_remote_args: Dict[str, Any] = None,
    ):
        cls = _Actor
        if ray_remote_args:
            cls = cls.options(**ray_remote_args)
        self.actors = {cls.remote(): 0 for _ in range(size)}

    def pick_actor(self):
        actors = sorted(list(self.actors.items()), key=lambda a: a[1])
        least_busy = actors[0][0]
        self.actors[least_busy] += 1
        return least_busy

    def return_actor(self, actor):
        self.actors[actor] -= 1


class _OpState:
    """Execution state for a PhysicalOperator."""

    def __init__(self, op: PhysicalOperator):
        self.inqueues: List[List[RefBundle]] = [
            [] for _ in range(len(op.input_dependencies))
        ]
        self.outqueue: List[RefBundle] = []
        self.op = op
        self.progress_bar = None
        self.num_active_tasks = 0
        self.num_completed_tasks = 0
        if isinstance(op, OneToOneOperator):
            self.compute_strategy = op.compute_strategy()
        else:
            self.compute_strategy = None
        if isinstance(self.compute_strategy, ActorPoolStrategy):
            self.actor_pool = _ActorPool(
                self.compute_strategy.max_size, self.op.ray_remote_args()
            )
        else:
            self.actor_pool = None

    def initialize_progress_bar(self, index: int) -> None:
        self.progress_bar = ProgressBar(
            self.op.name, self.op.num_outputs_total(), index
        )

    def num_queued(self) -> int:
        return sum(len(q) for q in self.inqueues)

    def add_output(self, ref: RefBundle) -> None:
        self.outqueue.append(ref)
        self.num_completed_tasks += 1
        if self.progress_bar:
            self.progress_bar.update(1)

    def refresh_progress_bar(self) -> None:
        if self.progress_bar:
            queued = self.num_queued()
            self.progress_bar.set_description(
                f"{self.op.name}: {self.num_active_tasks} active, {queued} queued"
            )


# TODO: reconcile with ComputeStrategy
class _OneToOneTask:
    """Execution state for OneToOneOperator task."""

    def __init__(
        self,
        op: OneToOneOperator,
        state: _OpState,
        inputs: RefBundle,
        options: ExecutionOptions,
    ):
        self._op: OneToOneOperator = op
        self._state: _OpState = state
        self._inputs: RefBundle = inputs
        self._block_ref: Optional[ObjectRef[Block]] = None
        self._meta_ref: Optional[ObjectRef[BlockMetadata]] = None
        self._options = options
        self._actor = None

    def execute(self) -> ObjectRef:
        if len(self._inputs.blocks) != 1:
            raise NotImplementedError("TODO: multi-block inputs")
        if self._state.actor_pool:
            return self._execute_actor()
        else:
            return self._execute_task()

    def _execute_task(self):
        transform_fn = _transform_one
        if self._options.locality_with_output:
            transform_fn = transform_fn.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    ray.get_runtime_context().get_node_id(),
                    soft=True,
                )
            )
        else:
            transform_fn = transform_fn.options(scheduling_strategy="SPREAD")
        self._block_ref, self._meta_ref = transform_fn.remote(
            self._op, self._inputs.blocks[0][0]
        )
        self._state.num_active_tasks += 1
        return self._meta_ref

    def _execute_actor(self):
        actor = self._state.actor_pool.pick_actor()
        self._actor = actor
        self._block_ref, self._meta_ref = actor.transform_one.remote(
            self._op, self._inputs.blocks[0][0]
        )
        self._state.num_active_tasks += 1
        return self._meta_ref

    def completed(self):
        meta = ray.get(self._meta_ref)
        self._state.num_active_tasks -= 1
        self._state.add_output(RefBundle([(self._block_ref, meta)]))
        if self._actor:
            self._state.actor_pool.return_actor(self._actor)


# TODO: optimize memory usage by deleting intermediate results.
# TODO: implement order preservation.
class PipelinedExecutor(Executor):
    def __init__(self, options: ExecutionOptions):
        # Operator state for the executing pipeline, populated on execution start.
        self._operator_state: Dict[PhysicalOperator, _OpState] = {}
        self._output_node: Optional[PhysicalOperator] = None
        self._active_tasks: List[ObjectRef, _OneToOneTask] = {}
        super().__init__(options)

    def execute(self, dag: PhysicalOperator) -> Iterator[RefBundle]:
        """Executes the DAG using a pipelined execution strategy.

        We take an event-loop approach to scheduling. We block on the next scheduling
        event using `ray.wait`, updating operator state and dispatching new tasks.
        """
        self._init_operator_state(dag)
        i = 0
        while self._active_tasks or i == 0:
            self._scheduling_loop_step()
            i += 1
            output = self._operator_state[self._output_node]
            while output.outqueue:
                yield output.outqueue.pop(0)

    def get_stats() -> DatasetStats:
        raise NotImplementedError

    def _scheduling_loop_step(self) -> None:
        """Run one step of the pipeline scheduling loop.

        This runs a few general phases:
            1. Waiting for the next task completion using `ray.wait()`.
            2. Pushing updates through operator inqueues / outqueues.
            3. Selecting and dispatching new operator tasks.
        """
        self._process_completed_tasks()
        op = self._select_operator_to_run()
        while op is not None:
            self._dispatch_next_task(op)
            op = self._select_operator_to_run()

    def _init_operator_state(self, dag: PhysicalOperator) -> None:
        """Initialize operator state for the given DAG.

        This involves creating the operator state for each operator in the DAG,
        registering it with this class, and wiring up the inqueues/outqueues of
        dependent operator states.
        """
        if self._operator_state:
            raise ValueError("Cannot init operator state twice.")

        def setup_state(node) -> _OpState:
            if node in self._operator_state:
                return self._operator_state[node]

            # Create state if it doesn't exist.
            state = _OpState(node)
            self._operator_state[node] = state

            # Wire up the input outqueues to this node's inqueues.
            for i, parent in enumerate(node.input_dependencies):
                parent_state = setup_state(parent)
                state.inqueues[i] = parent_state.outqueue

            return state

        setup_state(dag)
        self._output_node = dag

        i = 0
        for state in list(self._operator_state.values())[::-1]:
            if not isinstance(state.op, InputDataBuffer):
                state.initialize_progress_bar(i)
                i += 1

    def _process_completed_tasks(self) -> None:
        """Process any newly completed tasks and update operator state.

        This does not dispatch any new tasks, but pushes RefBundles through the
        DAG topology (i.e., operator state inqueues/outqueues).
        """
        for state in self._operator_state.values():
            state.refresh_progress_bar()

        if self._active_tasks:
            [ref], _ = ray.wait(
                list(self._active_tasks), num_returns=1, fetch_local=False
            )
            task = self._active_tasks.pop(ref)
            task.completed()

        for op, state in self._operator_state.items():
            if isinstance(op, ExchangeOperator):
                for i, inqueue in enumerate(state.inqueues):
                    while inqueue:
                        op.add_next(state.inqueue.pop(0), input_index=i)
                while op.has_next():
                    state.add_output(op.get_next())
            elif isinstance(op, OneToOneOperator):
                pass
            else:
                assert False, "Unknown operator type: {}".format(op)

    def _select_operator_to_run(self) -> Optional[PhysicalOperator]:
        """Select an operator to run, if possible.

        The objective of this function is to maximize the throughput of the overall
        pipeline, subject to defined memory and parallelism limits.
        """
        PARALLELISM_LIMIT = self._options.parallelism_limit or 8
        if len(self._active_tasks) >= PARALLELISM_LIMIT:
            return None

        # TODO: improve the prioritization.
        pairs = list(self._operator_state.items())
        pairs.sort(key=lambda p: len(p[1].outqueue) + p[1].num_active_tasks)

        for op, state in pairs:
            if isinstance(op, OneToOneOperator):
                assert len(state.inqueues) == 1, "OneToOne takes exactly 1 input"
                if state.inqueues[0]:
                    return op
            elif isinstance(op, ExchangeOperator):
                pass
            else:
                assert False, "Unknown operator type: {}".format(op)

    def _dispatch_next_task(self, op: PhysicalOperator) -> None:
        """Schedule the next task for the given operator.

        It is an error to call this if the given operator has no next tasks.

        Args:
            op: The operator to schedule a task for.
        """
        if isinstance(op, OneToOneOperator):
            state = self._operator_state[op]
            task = _OneToOneTask(op, state, state.inqueues[0].pop(0), self._options)
            self._active_tasks[task.execute()] = task
        else:
            raise NotImplementedError
