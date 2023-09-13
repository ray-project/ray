"""Contains classes that encapsulate streaming executor state.

This is split out from streaming_executor.py to facilitate better unit testing.
"""

import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Tuple, Union

import ray
from ray.data._internal.execution.autoscaling_requester import (
    get_or_create_autoscaling_requester_actor,
)
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import OpTask, Waitable
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.util import memory_string
from ray.data._internal.progress_bar import ProgressBar

# Holds the full execution state of the streaming topology. It's a dict mapping each
# operator to tracked streaming exec state.
Topology = Dict[PhysicalOperator, "OpState"]

# A RefBundle or an exception / end of stream indicator.
MaybeRefBundle = Union[RefBundle, Exception, None]

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
        for op, state in list(topology.items())[::-1]:
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


class OpState:
    """The execution state tracked for each PhysicalOperator.

    This tracks state to manage input and output buffering for StreamingExecutor and
    progress bars, which is separate from execution state internal to the operators.

    Note: we use the `deque` data structure here because it is thread-safe, enabling
    operator queues to be shared across threads.
    """

    def __init__(self, op: PhysicalOperator, inqueues: List[Deque[MaybeRefBundle]]):
        # Each inqueue is connected to another operator's outqueue.
        assert len(inqueues) == len(op.input_dependencies), (op, inqueues)
        self.inqueues: List[Deque[MaybeRefBundle]] = inqueues
        # The outqueue is connected to another operator's inqueue (they physically
        # share the same Python list reference).
        #
        # Note: this queue is also accessed concurrently from the consumer thread.
        # (in addition to the streaming executor thread). Hence, it must be a
        # thread-safe type such as `deque`.
        self.outqueue: Deque[MaybeRefBundle] = deque()
        self.op = op
        self.progress_bar = None
        self.num_completed_tasks = 0
        self.inputs_done_called = False
        # Tracks whether `input_done` is called for each input op.
        self.input_done_called = [False] * len(op.input_dependencies)
        self.dependents_completed_called = False

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
            self.op.num_outputs_total() or 1,
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
            self.progress_bar.update(1)

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

    def get_output_blocking(self, output_split_idx: Optional[int]) -> MaybeRefBundle:
        """Get an item from this node's output queue, blocking as needed.

        Returns:
            The RefBundle from the output queue, or an error / end of stream indicator.
        """
        while True:
            try:
                # Non-split output case.
                if output_split_idx is None:
                    return self.outqueue.popleft()

                # Scan the queue and look for outputs tagged for the given index.
                for i in range(len(self.outqueue)):
                    bundle = self.outqueue[i]
                    if bundle is None or isinstance(bundle, Exception):
                        # End of stream for this index! Note that we
                        # do not remove the None, so that it can act
                        # as the termination signal for all indices.
                        return bundle
                    elif bundle.output_split_idx == output_split_idx:
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
                total += self._queue_memory_usage(inq)
        return total

    def outqueue_memory_usage(self) -> int:
        """Return the object store memory of this operator's outqueue."""
        return self._queue_memory_usage(self.outqueue)

    def _queue_memory_usage(self, queue: Deque[RefBundle]) -> int:
        """Sum the object store memory usage in this queue.

        Note: Python's deque isn't truly thread-safe since it raises RuntimeError
        if it detects concurrent iteration. Hence we don't use its iterator but
        manually index into it.
        """

        object_store_memory = 0
        for i in range(len(queue)):
            try:
                bundle = queue[i]
                object_store_memory += bundle.size_bytes()
            except IndexError:
                break  # Concurrent pop from the outqueue by the consumer thread.
        return object_store_memory


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


def process_completed_tasks(topology: Topology) -> None:
    """Process any newly completed tasks. To update operator
    states, call `update_operator_states()` afterwards."""

    # Update active tasks.
    active_tasks: Dict[Waitable, OpTask] = {}

    for op in topology.keys():
        for task in op.get_active_tasks():
            active_tasks[task.get_waitable()] = task

    # Process completed Ray tasks and notify operators.
    if active_tasks:
        ready, _ = ray.wait(
            list(active_tasks.keys()),
            num_returns=len(active_tasks),
            fetch_local=False,
            timeout=0.1,
        )
        for ref in ready:
            active_tasks[ref].on_waitable_ready()

    # Pull any operator outputs into the streaming op state.
    for op, op_state in topology.items():
        while op.has_next():
            op_state.add_output(op.get_next())


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
    elif inc.object_store_memory:
        raise NotImplementedError(
            "Operator incremental resource usage must not include memory."
        )
    inc_indicator = ExecutionResources(
        cpu=1 if inc.cpu else 0,
        gpu=1 if inc.gpu else 0,
        object_store_memory=1 if inc.object_store_memory else 0,
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

    return global_ok_sans_memory and downstream_memory_ok
