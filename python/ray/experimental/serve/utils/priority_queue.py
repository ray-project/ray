from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import heapq


class BaseQueue:
    """Abstract base class for queue abstraction"""

    def push(self, item):
        raise NotImplementedError()

    def pop(self):
        raise NotImplementedError()

    def try_pop(self):
        raise NotImplementedError()

    def __len__(self):
        raise NotImplementedError()


class PriorityQueue(BaseQueue):
    """A min-heap class wrapping heapq module."""

    def __init__(self):
        self.q = []

    def push(self, item):
        heapq.heappush(self.q, item)

    def pop(self):
        return heapq.heappop(self.q)

    def try_pop(self):
        if len(self.q) == 0:
            return None
        else:
            return self.pop()

    def __len__(self):
        return len(self.q)


class FIFOQueue(BaseQueue):
    """A min-heap class wrapping heapq module."""

    def __init__(self):
        self.q = []

    def push(self, item):
        self.q.append(item)

    def pop(self):
        return self.q.pop(0)

    def try_pop(self):
        if len(self.q) == 0:
            return None
        else:
            return self.pop()

    def __len__(self):
        return len(self.q)
