from typing import Any, Tuple
import queue


class MinibatchBuffer:
    """Ring buffer of recent data batches for minibatch SGD.

    This is for use with AsyncSamplesOptimizer.
    """

    def __init__(self,
                 inqueue: queue.Queue,
                 size: int,
                 timeout: float,
                 num_passes: int,
                 init_num_passes: int = 1):
        """Initialize a minibatch buffer.

        Args:
           inqueue: Queue to populate the internal ring buffer from.
           size: Max number of data items to buffer.
           timeout: Queue timeout
           num_passes: Max num times each data item should be emitted.
           init_num_passes: Initial max passes for each data item
       """
        self.inqueue = inqueue
        self.size = size
        self.timeout = timeout
        self.max_ttl = num_passes
        self.cur_max_ttl = init_num_passes
        self.buffers = [None] * size
        self.ttl = [0] * size
        self.idx = 0

    def get(self) -> Tuple[Any, bool]:
        """Get a new batch from the internal ring buffer.

        Returns:
           buf: Data item saved from inqueue.
           released: True if the item is now removed from the ring buffer.
        """
        if self.ttl[self.idx] <= 0:
            self.buffers[self.idx] = self.inqueue.get(timeout=self.timeout)
            self.ttl[self.idx] = self.cur_max_ttl
            if self.cur_max_ttl < self.max_ttl:
                self.cur_max_ttl += 1
        buf = self.buffers[self.idx]
        self.ttl[self.idx] -= 1
        released = self.ttl[self.idx] <= 0
        if released:
            self.buffers[self.idx] = None
        self.idx = (self.idx + 1) % len(self.buffers)
        return buf, released
