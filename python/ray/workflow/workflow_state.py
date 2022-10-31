import asyncio

from collections import deque, defaultdict
import dataclasses
from dataclasses import field
import logging
from typing import List, Dict, Optional, Set, Deque, Callable

import ray
from ray.workflow.common import (
    TaskID,
    WorkflowRef,
    WorkflowTaskRuntimeOptions,
)
from ray.workflow.workflow_context import WorkflowTaskContext

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class TaskExecutionMetadata:
    submit_time: Optional[float] = None
    finish_time: Optional[float] = None
    output_size: Optional[int] = None

    @property
    def duration(self):
        return self.finish_time - self.submit_time


@dataclasses.dataclass
class Task:
    """Data class for a workflow task."""

    task_id: str
    options: WorkflowTaskRuntimeOptions
    user_metadata: Dict
    func_body: Optional[Callable]

    def to_dict(self) -> Dict:
        return {
            "task_id": self.task_id,
            "task_options": self.options.to_dict(),
            "user_metadata": self.user_metadata,
        }


@dataclasses.dataclass
class WorkflowExecutionState:
    """The execution state of a workflow. This dataclass helps with observation
    and debugging."""

    # -------------------------------- dependencies -------------------------------- #

    # The mapping from all tasks to immediately upstream tasks.
    upstream_dependencies: Dict[TaskID, List[TaskID]] = field(default_factory=dict)
    # A reverse mapping of the above. The dependency mapping from tasks to
    # immediately downstream tasks.
    downstream_dependencies: Dict[TaskID, List[TaskID]] = field(
        default_factory=lambda: defaultdict(list)
    )
    # The mapping from a task to its immediate continuation.
    next_continuation: Dict[TaskID, TaskID] = field(default_factory=dict)
    # The reversed mapping from continuation to its immediate task.
    prev_continuation: Dict[TaskID, TaskID] = field(default_factory=dict)
    # The mapping from a task to its latest continuation. The latest continuation is
    # a task that returns a value instead of a continuation.
    latest_continuation: Dict[TaskID, TaskID] = field(default_factory=dict)
    # The mapping from a task to the root of the continuation, i.e. the initial task
    # that generates the lineage of continuation.
    continuation_root: Dict[TaskID, TaskID] = field(default_factory=dict)

    # ------------------------------- task properties ------------------------------- #

    # Workflow tasks.
    tasks: Dict[TaskID, Task] = field(default_factory=dict)

    # The arguments for the task.
    task_input_args: Dict[TaskID, ray.ObjectRef] = field(default_factory=dict)
    # The context of the task.
    task_context: Dict[TaskID, WorkflowTaskContext] = field(default_factory=dict)
    # The execution metadata of a task.
    task_execution_metadata: Dict[TaskID, TaskExecutionMetadata] = field(
        default_factory=dict
    )
    task_retries: Dict[TaskID, int] = field(default_factory=lambda: defaultdict(int))

    # ------------------------------ object management ------------------------------ #

    # Set of references to upstream outputs.
    reference_set: Dict[TaskID, Set[TaskID]] = field(
        default_factory=lambda: defaultdict(set)
    )
    # The set of pending inputs of a task. We are able to run the task
    # when it becomes empty.
    pending_input_set: Dict[TaskID, Set[TaskID]] = field(default_factory=dict)
    # The map from a task to its in-memory outputs. Normally it is the ObjectRef
    # returned by the underlying Ray task. Things are different for continuation:
    # because the true output of a continuation is created by the last task in
    # the continuation lineage, so all other tasks in the continuation points
    # to the output of the last task instead of the output of themselves.
    output_map: Dict[TaskID, WorkflowRef] = field(default_factory=dict)
    # The map from a task to its in-storage checkpoints. Normally it is the checkpoint
    # created by the underlying Ray task. For continuations, the semantics is similar
    # to 'output_map'.
    checkpoint_map: Dict[TaskID, WorkflowRef] = field(default_factory=dict)
    # Outputs that are free (no reference to this output in the workflow) and
    # can be garbage collected.
    free_outputs: Set[TaskID] = field(default_factory=set)

    # -------------------------------- scheduling -------------------------------- #

    # The frontier that is ready to run.
    frontier_to_run: Deque[TaskID] = field(default_factory=deque)
    # The set of frontier tasks to run. This field helps deduplicate tasks or
    # look up task quickly. It contains the same elements as 'frontier_to_run',
    # they act like a 'DequeSet' when combined.
    frontier_to_run_set: Set[TaskID] = field(default_factory=set)
    # The frontier that is running.
    running_frontier: Dict[asyncio.Future, WorkflowRef] = field(default_factory=dict)
    # The set of running frontier. This field helps deduplicate tasks or
    # look up task quickly. It contains the same elements as 'running_frontier',
    # they act like a dict but its values are in a set when combined.
    running_frontier_set: Set[TaskID] = field(default_factory=set)
    # The set of completed tasks. They are tasks are actually executed with the state,
    # so inspected during recovery does not count.
    #
    # Normally, a task will be added in 'done_tasks' immediately after its completion.
    # However, a task that is the root of continuations (i.e. it returns a continuation
    # but itself is not a continuation) is only added to 'done_tasks' when all its
    # continuation completes. We do not add its continuations in 'done_tasks' because
    # we indicate their completion from the continuation structure - if a continuation
    # is appended to a previous continuation, then the previous continuation must
    # already complete; if the task that is the root of all continuation completes,
    # then all its continuations would complete.
    done_tasks: Set[TaskID] = field(default_factory=set)

    # -------------------------------- external -------------------------------- #

    # The ID of the output task.
    output_task_id: Optional[TaskID] = None

    def get_input(self, task_id: TaskID) -> Optional[WorkflowRef]:
        """Get the input. It checks memory first and storage later. It returns None if
        the input does not exist.
        """
        return self.output_map.get(task_id, self.checkpoint_map.get(task_id))

    def pop_frontier_to_run(self) -> Optional[TaskID]:
        """Pop one task to run from the frontier queue."""
        try:
            t = self.frontier_to_run.popleft()
            self.frontier_to_run_set.remove(t)
            return t
        except IndexError:
            return None

    def append_frontier_to_run(self, task_id: TaskID) -> None:
        """Insert one task to the frontier queue."""
        if (
            task_id not in self.frontier_to_run_set
            and task_id not in self.running_frontier_set
        ):
            self.frontier_to_run.append(task_id)
            self.frontier_to_run_set.add(task_id)

    def add_dependencies(self, task_id: TaskID, in_dependencies: List[TaskID]) -> None:
        """Add dependencies between a task and it input dependencies."""
        self.upstream_dependencies[task_id] = in_dependencies
        for in_task_id in in_dependencies:
            self.downstream_dependencies[in_task_id].append(task_id)

    def pop_running_frontier(self, fut: asyncio.Future) -> WorkflowRef:
        """Pop a task from the running frontier."""
        ref = self.running_frontier.pop(fut)
        self.running_frontier_set.remove(ref.task_id)
        return ref

    def insert_running_frontier(self, fut: asyncio.Future, ref: WorkflowRef) -> None:
        """Insert a task to the running frontier."""
        self.running_frontier[fut] = ref
        self.running_frontier_set.add(ref.task_id)

    def append_continuation(
        self, task_id: TaskID, continuation_task_id: TaskID
    ) -> None:
        """Append continuation to a task."""
        continuation_root = self.continuation_root.get(task_id, task_id)
        self.prev_continuation[continuation_task_id] = task_id
        self.next_continuation[task_id] = continuation_task_id
        self.continuation_root[continuation_task_id] = continuation_root
        self.latest_continuation[continuation_root] = continuation_task_id

    def merge_state(self, state: "WorkflowExecutionState") -> None:
        """Merge with another execution state."""
        self.upstream_dependencies.update(state.upstream_dependencies)
        self.downstream_dependencies.update(state.downstream_dependencies)
        self.task_input_args.update(state.task_input_args)
        self.tasks.update(state.tasks)
        self.task_context.update(state.task_context)
        self.output_map.update(state.output_map)
        self.checkpoint_map.update(state.checkpoint_map)

    def construct_scheduling_plan(self, task_id: TaskID) -> None:
        """Analyze upstream dependencies of a task to construct the scheduling plan."""
        if self.get_input(task_id) is not None:
            # This case corresponds to the scenario that the task is a
            # checkpoint or ref.
            return

        visited_nodes = set()
        dag_visit_queue = deque([task_id])
        while dag_visit_queue:
            tid = dag_visit_queue.popleft()
            if tid in visited_nodes:
                continue
            visited_nodes.add(tid)
            self.pending_input_set[tid] = set()
            for in_task_id in self.upstream_dependencies[tid]:
                self.reference_set[in_task_id].add(tid)
                # All upstream deps should already complete here,
                # so we just check their checkpoints.
                task_input = self.get_input(in_task_id)
                if task_input is None:
                    self.pending_input_set[tid].add(in_task_id)
                    dag_visit_queue.append(in_task_id)
            if tid in self.latest_continuation:
                if self.pending_input_set[tid]:
                    raise ValueError(
                        "A task that already returns a continuation cannot be pending."
                    )
                # construct continuations, as they are not directly connected to
                # the DAG dependency
                self.construct_scheduling_plan(self.latest_continuation[tid])
            elif not self.pending_input_set[tid]:
                self.append_frontier_to_run(tid)

    def init_context(self, context: WorkflowTaskContext) -> None:
        """Initialize the context of all tasks."""
        for task_id, task in self.tasks.items():
            options = task.options
            self.task_context.setdefault(
                task_id,
                dataclasses.replace(
                    context,
                    task_id=task_id,
                    creator_task_id=context.task_id,
                    checkpoint=options.checkpoint,
                    catch_exceptions=options.catch_exceptions,
                ),
            )
