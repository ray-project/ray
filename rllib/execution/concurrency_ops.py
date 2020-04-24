from typing import List
import queue

from ray.util.iter import LocalIterator, _NextValueNotReady
from ray.util.iter_metrics import SharedMetrics


def Concurrently(ops: List[LocalIterator],
                 *,
                 mode="round_robin",
                 output_indexes=None):
    """Operator that runs the given parent iterators concurrently.

    Arguments:
        mode (str): One of {'round_robin', 'async'}.
            - In 'round_robin' mode, we alternate between pulling items from
              each parent iterator in order deterministically.
            - In 'async' mode, we pull from each parent iterator as fast as
              they are produced. This is non-deterministic.
        output_indexes (list): If specified, only output results from the
            given ops. For example, if output_indexes=[0], only results from
            the first op in ops will be returned.

        >>> sim_op = ParallelRollouts(...).for_each(...)
        >>> replay_op = LocalReplay(...).for_each(...)
        >>> combined_op = Concurrently([sim_op, replay_op], mode="async")
    """

    if len(ops) < 2:
        raise ValueError("Should specify at least 2 ops.")
    if mode == "round_robin":
        deterministic = True
    elif mode == "async":
        deterministic = False
    else:
        raise ValueError("Unknown mode {}".format(mode))

    if output_indexes:
        for i in output_indexes:
            assert i in range(len(ops)), ("Index out of range", i)

        def tag(op, i):
            return op.for_each(lambda x: (i, x))

        ops = [tag(op, i) for i, op in enumerate(ops)]

    output = ops[0].union(*ops[1:], deterministic=deterministic)

    if output_indexes:
        output = (output.filter(lambda tup: tup[0] in output_indexes)
                  .for_each(lambda tup: tup[1]))

    return output


class Enqueue:
    """Enqueue data items into a queue.Queue instance.

    The enqueue is non-blocking, so Enqueue operations can executed with
    Dequeue via the Concurrently() operator.

    Examples:
        >>> queue = queue.Queue(100)
        >>> write_op = ParallelRollouts(...).for_each(Enqueue(queue))
        >>> read_op = Dequeue(queue)
        >>> combined_op = Concurrently([write_op, read_op], mode="async")
        >>> next(combined_op)
        SampleBatch(...)
    """

    def __init__(self, output_queue: queue.Queue):
        if not isinstance(output_queue, queue.Queue):
            raise ValueError("Expected queue.Queue, got {}".format(
                type(output_queue)))
        self.queue = output_queue

    def __call__(self, x):
        try:
            self.queue.put_nowait(x)
        except queue.Full:
            return _NextValueNotReady()


def Dequeue(input_queue: queue.Queue, check=lambda: True):
    """Dequeue data items from a queue.Queue instance.

    The dequeue is non-blocking, so Dequeue operations can executed with
    Enqueue via the Concurrently() operator.

    Arguments:
        input_queue (Queue): queue to pull items from.
        check (fn): liveness check. When this function returns false,
            Dequeue() will raise an error to halt execution.

    Examples:
        >>> queue = queue.Queue(100)
        >>> write_op = ParallelRollouts(...).for_each(Enqueue(queue))
        >>> read_op = Dequeue(queue)
        >>> combined_op = Concurrently([write_op, read_op], mode="async")
        >>> next(combined_op)
        SampleBatch(...)
    """
    if not isinstance(input_queue, queue.Queue):
        raise ValueError("Expected queue.Queue, got {}".format(
            type(input_queue)))

    def base_iterator(timeout=None):
        while check():
            try:
                item = input_queue.get_nowait()
                yield item
            except queue.Empty:
                yield _NextValueNotReady()
        raise RuntimeError("Error raised reading from queue")

    return LocalIterator(base_iterator, SharedMetrics())
