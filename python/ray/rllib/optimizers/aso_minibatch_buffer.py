"""Helper class for AsyncSamplesOptimizer."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class MinibatchBuffer(object):
    """Ring buffer of recent data batches for minibatch SGD.

    This is for use with AsyncSamplesOptimizer.
    """

    def __init__(self, inqueue, size, timeout, num_passes):
        """Initialize a minibatch buffer.

        Arguments:
           inqueue: Queue to populate the internal ring buffer from.
           size: Max number of data items to buffer.
           timeout: Queue timeout
           num_passes: Max num times each data item should be emitted.
        """
        self.inqueue = inqueue
        self.size = size
        self.timeout = timeout
        self.max_ttl = num_passes
        self.cur_max_ttl = 1  # ramp up slowly to better mix the input data
        self.buffers = [None] * size
        self.ttl = [0] * size
        self.idx = 0

    def get(self):
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
