"""Contains classes that encapsulate streaming executor state.

This is split out from streaming_executor.py to facilitate better unit testing.
"""

import math
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import ray
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.execution.autoscaling_requester import (
    get_or_create_autoscaling_requester_actor,
)
from ray.data._internal.execution.backpressure_policy import BackpressurePolicy
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    MetadataOpTask,
    OpTask,
    Waitable,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.util import memory_string
from ray.data._internal.progress_bar import ProgressBar
from ray.data.context import DataContext

logger = DatasetLogger(__name__)

# Holds the full execution state of the streaming topology. It's a dict mapping each
# operator to tracked streaming exec state.
Topology = Dict[PhysicalOperator, "OpState"]

# The fraction of the object store capacity that will be used as the default object
# store memory limit for the streaming executor.
DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION = 0.25

# Min number of seconds between two autoscaling requests.
MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS = 20


@dataclass
class AutoscalingState:
    """State of the interaction between an executor and Ray autoscaler."""

    # The timestamp of the latest resource request made to Ray autoscaler
    # by an executor.
    last_request_ts: int = 0


@dataclass
class TopologyResourceUsage:
    """Snapshot of resource usage in a `Topology` object.

    The stats here can be computed on the fly from any `Topology`; this class
    serves only a convenience wrapper to access the current usage snapshot.
    """

    # The current usage of the topology (summed across all operators and queues).
    overall: ExecutionResources

    # The downstream resource usage by operator.
    downstream_memory_usage: Dict[PhysicalOperator, "DownstreamMemoryInfo"]

    @staticmethod
    def of(topology: Topology) -> "TopologyResourceUsage":
        """Calculate the resource usage of the given topology."""
        downstream_usage = {}
        cur_usage = ExecutionResources(0, 0, 0)
        # Iterate from last to first operator.
        for op, state in reversed(topology.items()):
            cur_usage = cur_usage.add(op.current_resource_usage())
            # Don't count input refs towards dynamic memory usage, as they have been
            # pre-created already outside this execution.
            if not isinstance(op, InputDataBuffer):
                cur_usage.object_store_memory += state.outqueue_memory_usage()
            # Subtract one from denom to account for input buffer.
            f = (1.0 + len(downstream_usage)) / max(1.0, len(topology) - 1.0)
            downstream_usage[op] = DownstreamMemoryInfo(
                topology_fraction=min(1.0, f),
                object_store_memory=cur_usage.object_store_memory,
            )
        return TopologyResourceUsage(cur_usage, downstream_usage)


@dataclass
class DownstreamMemoryInfo:
    """Mem stats of an operator and its downstream operators in a topology."""

    # The fraction of the topology this covers, e.g., the last operator of a 4-op
    # graph would have fraction `0.25`.
    topology_fraction: float

    # The resources used by this operator and operators downstream of this operator.
    object_store_memory: float


class RefBundleDeque(deque):
    """Thread-safe wrapper around collections.deque that stores current stats."""

    def __init__(self):
        self.memory_usage = 0
        self._lock = threading.Lock()
        super().__init__()

    def append(self, ref: RefBundle):
        with self._lock:
            self.memory_usage += ref.size_bytes()
        super().append(ref)

    def appendleft(self, ref: RefBundle):
        with self._lock:
            self.memory_usage += ref.size_bytes()
        super().appendleft(ref)

    def pop(self) -> RefBundle:
        ref = super().pop()
        with self._lock:
            self.memory_usage -= ref.size_bytes()
        return ref

    def popleft(self) -> RefBundle:
        ref = super().popleft()
        with self._lock:
            self.memory_usage -= ref.size_bytes()
        return ref

    def remove(self, ref: RefBundle):
        super().remove(ref)
        with self._lock:
            self.memory_usage -= ref.size_bytes()

    def clear(self):
        super().clear()
        with self._lock:
            self.memory_usage = 0


class OpState:
    """The execution state tracked for each PhysicalOperator.

    This tracks state to manage input and output buffering for StreamingExecutor and
    progress bars, which is separate from execution state internal to the operators.

    Note: we use the `deque` data structure here because it is thread-safe, enabling
    operator queues to be shared across threads.
    """

    def __init__(self, op: PhysicalOperator, inqueues: List[RefBundleDeque]):
        # Each inqueue is connected to another operator's outqueue.
        assert len(inqueues) == len(op.input_dependencies), (op, inqueues)
        self.inqueues: List[RefBundleDeque] = inqueues
        # The outqueue is connected to another operator's inqueue (they physically
        # share the same Python list reference).
        #
        # Note: this queue is also accessed concurrently from the consumer thread.
        # (in addition to the streaming executor thread). Hence, it must be a
        # thread-safe type such as `deque`.
        self.outqueue: RefBundleDeque = RefBundleDeque()
        self.op = op
        self.progress_bar = None
        self.num_completed_tasks = 0
        self.inputs_done_called = False
        # Tracks whether `input_done` is called for each input op.
        self.input_done_called = [False] * len(op.input_dependencies)
        self.dependents_completed_called = False
        # Used for StreamingExecutor to signal exception or end of execution
        self._finished: bool = False
        self._exception: Optional[Exception] = None

    def __repr__(self):
        return f"OpState({self.op.name})"

    def initialize_progress_bars(self, index: int, verbose_progress: bool) -> int:
        """Create progress bars at the given index (line offset in console).

        For AllToAllOperator, zero or more sub progress bar would be created.
        Return the number of progress bars created for this operator.
        """
        is_all_to_all = isinstance(self.op, AllToAllOperator)
        # Only show 1:1 ops when in verbose progress mode.
        enabled = verbose_progress or is_all_to_all
        self.progress_bar = ProgressBar(
            "- " + self.op.name,
            self.op.num_outputs_total(),
            index,
            enabled=enabled,
        )
        if enabled:
            num_bars = 1
            if is_all_to_all:
                num_bars += self.op.initialize_sub_progress_bars(index + 1)
        else:
            num_bars = 0
        return num_bars

    def close_progress_bars(self):
        """Close all progress bars for this operator."""
        if self.progress_bar:
            self.progress_bar.close()
            if isinstance(self.op, AllToAllOperator):
                self.op.close_sub_progress_bars()

    def num_queued(self) -> int:
        """Return the number of queued bundles across all inqueues."""
        return sum(len(q) for q in self.inqueues)

    def num_processing(self):
        """Return the number of bundles currently in processing for this operator."""
        return self.op.num_active_tasks() + self.op.internal_queue_size()

    def add_output(self, ref: RefBundle) -> None:
        """Move a bundle produced by the operator to its outqueue."""
        self.outqueue.append(ref)
        self.num_completed_tasks += 1
        if self.progress_bar:
            self.progress_bar.update(1, self.op._estimated_output_blocks)

    def refresh_progress_bar(self) -> None:
        """Update the console with the latest operator progress."""
        if self.progress_bar:
            self.progress_bar.set_description(self.summary_str())

    def summary_str(self) -> str:
        queued = self.num_queued() + self.op.internal_queue_size()
        active = self.op.num_active_tasks()
        desc = f"- {self.op.name}: {active} active, {queued} queued"
        mem = memory_string(
            (self.op.current_resource_usage().object_store_memory or 0)
            + self.inqueue_memory_usage()
        )
        desc += f", {mem} objects"
        suffix = self.op.progress_str()
        if suffix:
            desc += f", {suffix}"
        return desc

    def dispatch_next_task(self) -> None:
        """Move a bundle from the operator inqueue to the operator itself."""
        for i, inqueue in enumerate(self.inqueues):
            if inqueue:
                self.op.add_input(inqueue.popleft(), input_index=i)
                return
        assert False, "Nothing to dispatch"

    def get_output_blocking(self, output_split_idx: Optional[int]) -> RefBundle:
        """Get an item from this node's output queue, blocking as needed.

        Returns:
            The RefBundle from the output queue, or an error / end of stream indicator.

        Raises:
            StopIteration: If all outputs are already consumed.
            Exception: If there was an exception raised during execution.
        """
        while True:
            # Check if StreamingExecutor has caught an exception or is done execution.
            if self._exception is not None:
                raise self._exception
            elif self._finished and len(self.outqueue) == 0:
                raise StopIteration()
            try:
                # Non-split output case.
                if output_split_idx is None:
                    return self.outqueue.popleft()

                # Scan the queue and look for outputs tagged for the given index.
                for i in range(len(self.outqueue)):
                    bundle = self.outqueue[i]
                    if bundle.output_split_idx == output_split_idx:
                        self.outqueue.remove(bundle)
                        return bundle

                # Didn't find any outputs matching this index, repeat the loop until
                # we find one or hit a None.
            except IndexError:
                pass
            time.sleep(0.01)

    def inqueue_memory_usage(self) -> int:
        """Return the object store memory of this operator's inqueue."""
        total = 0
        for op, inq in zip(self.op.input_dependencies, self.inqueues):
            # Exclude existing input data items from dynamic memory usage.
            if not isinstance(op, InputDataBuffer):
                total += inq.memory_usage
        return total

    def outqueue_memory_usage(self) -> int:
        """Return the object store memory of this operator's outqueue."""
        return self.outqueue.memory_usage

    def outqueue_num_blocks(self) -> int:
        """Return the number of blocks in this operator's outqueue."""
        num_blocks = 0
        for i in range(len(self.outqueue)):
            try:
                bundle = self.outqueue[i]
                if isinstance(bundle, RefBundle):
                    num_blocks += len(bundle.blocks)
            except IndexError:
                break
        return len(self.outqueue)

    def mark_finished(self, exception: Optional[Exception] = None):
        """Marks this operator as finished. Used for exiting get_output_blocking."""
        if exception is None:
            self._finished = True
        else:
            self._exception = exception


def build_streaming_topology(
    dag: PhysicalOperator, options: ExecutionOptions
) -> Tuple[Topology, int]:
    """Instantiate the streaming operator state topology for the given DAG.

    This involves creating the operator state for each operator in the DAG,
    registering it with this class, and wiring up the inqueues/outqueues of
    dependent operator states.

    Args:
        dag: The operator DAG to instantiate.
        options: The execution options to use to start operators.

    Returns:
        The topology dict holding the streaming execution state.
        The number of progress bars initialized so far.
    """

    topology: Topology = {}

    # DFS walk to wire up operator states.
    def setup_state(op: PhysicalOperator) -> OpState:
        if op in topology:
            raise ValueError("An operator can only be present in a topology once.")

        # Wire up the input outqueues to this op's inqueues.
        inqueues = []
        for i, parent in enumerate(op.input_dependencies):
            parent_state = setup_state(parent)
            inqueues.append(parent_state.outqueue)

        # Create state.
        op_state = OpState(op, inqueues)
        topology[op] = op_state
        op.start(options)
        return op_state

    setup_state(dag)

    # Create the progress bars starting from the first operator to run.
    # Note that the topology dict is in topological sort order. Index zero is reserved
    # for global progress information.
    i = 1
    for op_state in list(topology.values()):
        if not isinstance(op_state.op, InputDataBuffer):
            i += op_state.initialize_progress_bars(i, options.verbose_progress)

    return (topology, i)


def process_completed_tasks(
    topology: Topology,
    backpressure_policies: List[BackpressurePolicy],
    max_errored_blocks: int,
) -> int:
    """Process any newly completed tasks. To update operator
    states, call `update_operator_states()` afterwards.

    Args:
        topology: The toplogy of operators.
        backpressure_policies: The backpressure policies to use.
        max_errored_blocks: Max number of errored blocks to allow,
            unlimited if negative.
    Returns:
        The number of errored blocks.
    """

    # All active tasks, keyed by their waitables.
    active_tasks: Dict[Waitable, Tuple[OpState, OpTask]] = {}
    for op, state in topology.items():
        for task in op.get_active_tasks():
            active_tasks[task.get_waitable()] = (state, task)

    max_blocks_to_read_per_op: Dict[OpState, int] = {}
    for policy in backpressure_policies:
        res = policy.calculate_max_blocks_to_read_per_op(topology)
        if len(res) > 0:
            if len(max_blocks_to_read_per_op) > 0:
                raise ValueError(
                    "At most one backpressure policy that implements "
                    "calculate_max_blocks_to_read_per_op() can be used at a time."
                )
            else:
                max_blocks_to_read_per_op = res

    # Process completed Ray tasks and notify operators.
    num_errored_blocks = 0
    if active_tasks:
        ready, _ = ray.wait(
            list(active_tasks.keys()),
            num_returns=len(active_tasks),
            fetch_local=False,
            timeout=0.1,
        )

        # Organize tasks by the operator they belong to, and sort them by task index.
        # So that we'll process them in a deterministic order.
        # This is because some backpressure policies (e.g.,
        # StreamingOutputBackpressurePolicy) may limit the number of blocks to read
        # per operator. In this case, we want to have fewer tasks finish quickly and
        # yield resources, instead of having all tasks output blocks together.
        ready_tasks_by_op = defaultdict(list)
        for ref in ready:
            state, task = active_tasks[ref]
            ready_tasks_by_op[state].append(task)

        for state, ready_tasks in ready_tasks_by_op.items():
            ready_tasks = sorted(ready_tasks, key=lambda t: t.task_index())
            for task in ready_tasks:
                if isinstance(task, DataOpTask):
                    try:
                        num_blocks_read = task.on_data_ready(
                            max_blocks_to_read_per_op.get(state, None)
                        )
                        if state in max_blocks_to_read_per_op:
                            max_blocks_to_read_per_op[state] -= num_blocks_read
                    except Exception as e:
                        num_errored_blocks += 1
                        should_ignore = (
                            max_errored_blocks < 0
                            or max_errored_blocks >= num_errored_blocks
                        )
                        error_message = (
                            "An exception was raised from a task of "
                            f'operator "{state.op.name}".'
                        )
                        if should_ignore:
                            remaining = (
                                max_errored_blocks - num_errored_blocks
                                if max_errored_blocks >= 0
                                else "unlimited"
                            )
                            error_message += (
                                " Ignoring this exception with remaining"
                                f" max_errored_blocks={remaining}."
                            )
                            logger.get_logger().warning(error_message, exc_info=e)
                        else:
                            error_message += (
                                " Dataset execution will now abort."
                                " To ignore this exception and continue, set"
                                " DataContext.max_errored_blocks."
                            )
                            logger.get_logger().error(error_message)
                            raise e from None
                else:
                    assert isinstance(task, MetadataOpTask)
                    task.on_task_finished()

    # Pull any operator outputs into the streaming op state.
    for op, op_state in topology.items():
        while op.has_next():
            op_state.add_output(op.get_next())

    return num_errored_blocks


def update_operator_states(topology: Topology) -> None:
    """Update operator states accordingly for newly completed tasks.
    Should be called after `process_completed_tasks()`."""

    # Call inputs_done() on ops where no more inputs are coming.
    for op, op_state in topology.items():
        if op_state.inputs_done_called:
            continue
        all_inputs_done = True
        for idx, dep in enumerate(op.input_dependencies):
            if dep.completed() and not topology[dep].outqueue:
                if not op_state.input_done_called[idx]:
                    op.input_done(idx)
                    op_state.input_done_called[idx] = True
            else:
                all_inputs_done = False

        if all_inputs_done:
            op.all_inputs_done()
            op_state.inputs_done_called = True

    # Traverse the topology in reverse topological order.
    # For each op, if all of its downstream operators don't need any more inputs,
    # call all_dependents_complete() to also complete this op.
    for op, op_state in reversed(list(topology.items())):
        if op_state.dependents_completed_called:
            continue
        dependents_completed = len(op.output_dependencies) > 0 and all(
            not dep.need_more_inputs() for dep in op.output_dependencies
        )
        if dependents_completed:
            op.all_dependents_complete()
            op_state.dependents_completed_called = True


def select_operator_to_run(
    topology: Topology,
    cur_usage: TopologyResourceUsage,
    limits: ExecutionResources,
    backpressure_policies: List[BackpressurePolicy],
    ensure_at_least_one_running: bool,
    execution_id: str,
    autoscaling_state: AutoscalingState,
) -> Optional[PhysicalOperator]:
    """Select an operator to run, if possible.

    The objective of this function is to maximize the throughput of the overall
    pipeline, subject to defined memory and parallelism limits.

    This is currently implemented by applying backpressure on operators that are
    producing outputs faster than they are consuming them `len(outqueue)`, as well as
    operators with a large number of running tasks `num_processing()`.

    Note that memory limits also apply to the outqueue of the output operator. This
    provides backpressure if the consumer is slow. However, once a bundle is returned
    to the user, it is no longer tracked.
    """
    assert isinstance(cur_usage, TopologyResourceUsage), cur_usage

    # Filter to ops that are eligible for execution.
    ops = []
    for op, state in topology.items():
        under_resource_limits = _execution_allowed(op, cur_usage, limits)
        if (
            op.need_more_inputs()
            and state.num_queued() > 0
            and op.should_add_input()
            and under_resource_limits
            and not op.completed()
            and all(p.can_add_input(op) for p in backpressure_policies)
        ):
            ops.append(op)
        # Update the op in all cases to enable internal autoscaling, etc.
        op.notify_resource_usage(state.num_queued(), under_resource_limits)

    # If no ops are allowed to execute due to resource constraints, try to trigger
    # cluster scale-up.
    if not ops and any(state.num_queued() > 0 for state in topology.values()):
        now = time.time()
        if (
            now
            > autoscaling_state.last_request_ts + MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS
        ):
            autoscaling_state.last_request_ts = now
            _try_to_scale_up_cluster(topology, execution_id)

    # To ensure liveness, allow at least 1 op to run regardless of limits. This is
    # gated on `ensure_at_least_one_running`, which is set if the consumer is blocked.
    if (
        ensure_at_least_one_running
        and not ops
        and all(op.num_active_tasks() == 0 for op in topology)
    ):
        # The topology is entirely idle, so choose from all ready ops ignoring limits.
        ops = [
            op
            for op, state in topology.items()
            if op.need_more_inputs() and state.num_queued() > 0 and not op.completed()
        ]

    # Nothing to run.
    if not ops:
        return None

    # Run metadata-only operators first. After that, equally penalize outqueue length
    # and num bundles processing for backpressure.
    return min(
        ops,
        key=lambda op: (
            not op.throttling_disabled(),
            len(topology[op].outqueue) + topology[op].num_processing(),
        ),
    )


def _try_to_scale_up_cluster(topology: Topology, execution_id: str):
    """Try to scale up the cluster to accomodate the provided in-progress workload.

    This makes a resource request to Ray's autoscaler consisting of the current,
    aggregate usage of all operators in the DAG + the incremental usage of all operators
    that are ready for dispatch (i.e. that have inputs queued). If the autoscaler were
    to grant this resource request, it would allow us to dispatch one task for every
    ready operator.

    Note that this resource request does not take the global resource limits or the
    liveness policy into account; it only tries to make the existing resource usage +
    one more task per ready operator feasible in the cluster.

    Args:
        topology: The execution state of the in-progress workload for which we wish to
            request more resources.
    """
    # Get resource usage for all ops + additional resources needed to launch one more
    # task for each ready op.
    resource_request = []

    def to_bundle(resource: ExecutionResources) -> Dict:
        req = {}
        if resource.cpu:
            req["CPU"] = math.ceil(resource.cpu)
        if resource.gpu:
            req["GPU"] = math.ceil(resource.gpu)
        return req

    for op, state in topology.items():
        per_task_resource = op.incremental_resource_usage()
        task_bundle = to_bundle(per_task_resource)
        resource_request.extend([task_bundle] * op.num_active_tasks())
        # Only include incremental resource usage for ops that are ready for
        # dispatch.
        if state.num_queued() > 0:
            # TODO(Clark): Scale up more aggressively by adding incremental resource
            # usage for more than one bundle in the queue for this op?
            resource_request.append(task_bundle)

    # Make autoscaler resource request.
    actor = get_or_create_autoscaling_requester_actor()
    actor.request_resources.remote(resource_request, execution_id)


def _execution_allowed(
    op: PhysicalOperator,
    global_usage: TopologyResourceUsage,
    global_limits: ExecutionResources,
) -> bool:
    """Return whether an operator is allowed to execute given resource usage.

    Operators are throttled globally based on CPU and GPU limits for the stream.

    For an N operator DAG, we only throttle the kth operator (in the source-to-sink
    ordering) on object store utilization if the cumulative object store utilization
    for the kth operator and every operator downstream from it is greater than
    k/N * global_limit; i.e., the N - k operator sub-DAG is using more object store
    memory than it's share.

    Args:
        op: The operator to check.
        global_usage: Resource usage across the entire topology.
        global_limits: Execution resource limits.

    Returns:
        Whether the op is allowed to run.
    """

    if op.throttling_disabled():
        return True

    assert isinstance(global_usage, TopologyResourceUsage), global_usage
    # To avoid starvation problems when dealing with fractional resource types,
    # convert all quantities to integer (0 or 1) for deciding admissibility. This
    # allows operators with non-integral requests to slightly overshoot the limit.
    global_floored = ExecutionResources(
        cpu=math.floor(global_usage.overall.cpu or 0),
        gpu=math.floor(global_usage.overall.gpu or 0),
        object_store_memory=global_usage.overall.object_store_memory,
    )
    inc = op.incremental_resource_usage()
    if inc.cpu and inc.gpu:
        raise NotImplementedError(
            "Operator incremental resource usage cannot specify both CPU "
            "and GPU at the same time, since it may cause deadlock."
        )

    # Ignore the scale of CPU and GPU requests, i.e., treating them as either 1 or 0.
    # This ensures operators don't get starved due to the shape of their resource
    # requests.
    inc_indicator = ExecutionResources(
        cpu=1 if inc.cpu else 0,
        gpu=1 if inc.gpu else 0,
        object_store_memory=inc.object_store_memory
        if DataContext.get_current().use_runtime_metrics_scheduling
        else None,
    )

    # Under global limits; always allow.
    new_usage = global_floored.add(inc_indicator)
    if new_usage.satisfies_limit(global_limits):
        return True

    # We're over global limits, but execution may still be allowed if memory is the
    # only bottleneck and this wouldn't impact downstream memory limits. This avoids
    # stalling the execution for memory bottlenecks that occur upstream.
    # See for more context: https://github.com/ray-project/ray/pull/32673
    global_limits_sans_memory = ExecutionResources(
        cpu=global_limits.cpu, gpu=global_limits.gpu
    )
    global_ok_sans_memory = new_usage.satisfies_limit(global_limits_sans_memory)
    downstream_usage = global_usage.downstream_memory_usage[op]
    downstream_limit = global_limits.scale(downstream_usage.topology_fraction)
    downstream_memory_ok = ExecutionResources(
        object_store_memory=downstream_usage.object_store_memory
    ).satisfies_limit(downstream_limit)

    # If completing a task decreases the overall object store memory usage, allow it
    # even if we're over the global limit.
    if (
        DataContext.get_current().use_runtime_metrics_scheduling
        and global_ok_sans_memory
        and op.metrics.average_bytes_change_per_task is not None
        and op.metrics.average_bytes_change_per_task <= 0
    ):
        return True

    return global_ok_sans_memory and downstream_memory_ok
