from typing import List, Optional, Any
import queue

from ray.util.iter import LocalIterator, _NextValueNotReady
from ray.util.iter_metrics import SharedMetrics
from ray.rllib.utils.typing import SampleBatchType


def Concurrently(
    ops: List[LocalIterator],
    *,
    mode: str = "round_robin",
    output_indexes: Optional[List[int]] = None,
    round_robin_weights: Optional[List[int]] = None
) -> LocalIterator[SampleBatchType]:
    """Operator that runs the given parent iterators concurrently.

    Args:
        mode: One of 'round_robin', 'async'. In 'round_robin' mode,
            we alternate between pulling items from each parent iterator in
            order deterministically. In 'async' mode, we pull from each parent
            iterator as fast as they are produced. This is non-deterministic.
        output_indexes: If specified, only output results from the
            given ops. For example, if ``output_indexes=[0]``, only results
            from the first op in ops will be returned.
        round_robin_weights: List of weights to use for round-robin
            mode. For example, ``[2, 1]`` will cause the iterator to pull twice
            as many items from the first iterator as the second. ``[2, 1, *]``
            will cause as many items to be pulled as possible from the third
            iterator without blocking. This is only allowed in round-robin
            mode.

    Examples:
        >>> from ray.rllib.execution import ParallelRollouts
        >>> sim_op = ParallelRollouts(...).for_each(...) # doctest: +SKIP
        >>> replay_op = LocalReplay(...).for_each(...) # doctest: +SKIP
        >>> combined_op = Concurrently( # doctest: +SKIP
        ...     [sim_op, replay_op], mode="async")
    """

    if len(ops) < 2:
        raise ValueError("Should specify at least 2 ops.")
    if mode == "round_robin":
        deterministic = True
    elif mode == "async":
        deterministic = False
        if round_robin_weights:
            raise ValueError("round_robin_weights cannot be specified in async mode")
    else:
        raise ValueError("Unknown mode {}".format(mode))
    if round_robin_weights and all(r == "*" for r in round_robin_weights):
        raise ValueError("Cannot specify all round robin weights = *")

    if output_indexes:
        for i in output_indexes:
            assert i in range(len(ops)), ("Index out of range", i)

        def tag(op, i):
            return op.for_each(lambda x: (i, x))

        ops = [tag(op, i) for i, op in enumerate(ops)]

    output = ops[0].union(
        *ops[1:], deterministic=deterministic, round_robin_weights=round_robin_weights
    )

    if output_indexes:
        output = output.filter(lambda tup: tup[0] in output_indexes).for_each(
            lambda tup: tup[1]
        )

    return output


class Enqueue:
    """Enqueue data items into a queue.Queue instance.

    Returns the input item as output.

    The enqueue is non-blocking, so Enqueue operations can executed with
    Dequeue via the Concurrently() operator.

    Examples:
        >>> import queue
        >>> from ray.rllib.execution import ParallelRollouts
        >>> queue = queue.Queue(100) # doctest: +SKIP
        >>> write_op = ParallelRollouts(...).for_each(Enqueue(queue)) # doctest: +SKIP
        >>> read_op = Dequeue(queue) # doctest: +SKIP
        >>> combined_op = Concurrently( # doctest: +SKIP
        ...     [write_op, read_op], mode="async")
        >>> next(combined_op) # doctest: +SKIP
        SampleBatch(...)
    """

    def __init__(self, output_queue: queue.Queue):
        if not isinstance(output_queue, queue.Queue):
            raise ValueError("Expected queue.Queue, got {}".format(type(output_queue)))
        self.queue = output_queue

    def __call__(self, x: Any) -> Any:
        try:
            self.queue.put(x, timeout=0.001)
        except queue.Full:
            return _NextValueNotReady()
        return x


def Dequeue(
    input_queue: queue.Queue, check=lambda: True
) -> LocalIterator[SampleBatchType]:
    """Dequeue data items from a queue.Queue instance.

    The dequeue is non-blocking, so Dequeue operations can execute with
    Enqueue via the Concurrently() operator.

    Args:
        input_queue: queue to pull items from.
        check: liveness check. When this function returns false,
            Dequeue() will raise an error to halt execution.

    Examples:
        >>> import queue
        >>> from ray.rllib.execution import ParallelRollouts
        >>> queue = queue.Queue(100) # doctest: +SKIP
        >>> write_op = ParallelRollouts(...) # doctest: +SKIP
        ...     .for_each(Enqueue(queue))
        >>> read_op = Dequeue(queue) # doctest: +SKIP
        >>> combined_op = Concurrently( # doctest: +SKIP
        ...     [write_op, read_op], mode="async")
        >>> next(combined_op) # doctest: +SKIP
        SampleBatch(...)
    """
    if not isinstance(input_queue, queue.Queue):
        raise ValueError("Expected queue.Queue, got {}".format(type(input_queue)))

    def base_iterator(timeout=None):
        while check():
            try:
                item = input_queue.get(timeout=0.001)
                yield item
            except queue.Empty:
                yield _NextValueNotReady()
        raise RuntimeError(
            "Dequeue `check()` returned False! "
            "Exiting with Exception from Dequeue iterator."
        )

    return LocalIterator(base_iterator, SharedMetrics())
