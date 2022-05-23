from typing import Any, Tuple
import queue


class MinibatchBuffer:
    """Ring buffer of recent data batches for minibatch SGD.

    This is for use with AsyncSamplesOptimizer.
    """

    def __init__(
        self,
        inqueue: queue.Queue,
        size: int,
        timeout: float,
        num_passes: int,
        init_num_passes: int = 1,
    ):
        """Initialize a minibatch buffer.

        Args:
           inqueue (queue.Queue): Queue to populate the internal ring buffer
           from.
           size (int): Max number of data items to buffer.
           timeout (float): Queue timeout
           num_passes (int): Max num times each data item should be emitted.
           init_num_passes (int): Initial passes for each data item.
           Maxiumum number of passes per item are increased to num_passes over
           time.
        """
        self.inqueue = inqueue
        self.size = size
        self.timeout = timeout
        self.max_initial_ttl = num_passes
        self.cur_initial_ttl = init_num_passes
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
            self.ttl[self.idx] = self.cur_initial_ttl
            if self.cur_initial_ttl < self.max_initial_ttl:
                self.cur_initial_ttl += 1
        buf = self.buffers[self.idx]
        self.ttl[self.idx] -= 1
        released = self.ttl[self.idx] <= 0
        if released:
            self.buffers[self.idx] = None
        self.idx = (self.idx + 1) % len(self.buffers)
        return buf, released
