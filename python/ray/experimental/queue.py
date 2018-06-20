from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import deque
import time

import ray


class Empty(Exception):
    pass


class Full(Exception):
    pass


class Queue(object):
    """Queue implementation on Ray.

    Todo:
        - not_empty
        - not_full
        - join
        - mutex
        - task_done
        - unfinished_taskss
    """
    def __init__(self, maxsize=0):
        self.maxsize = maxsize
        self.actor = _QueueActor.remote(maxsize)

    def qsize(self):
        return ray.get(self.actor.qsize.remote())

    def empty(self):
        return ray.get(self.actor.qsize.remote())

    def full(self):
        return ray.get(self.actor.full.remote())

    def put(self, item, block=True, timeout=None):
        if self.maxsize <= 0:
            self.actor.put.remote(item)
        elif not block:
            if not ray.get(self.actor.put.remote(item)):
                raise Full
        elif timeout is None:
            # Polling
            # TODO: implement with not_full condition variable
            while not ray.get(self.actor.put.remote(item)):
                # Consider adding time.sleep here
                pass
        elif timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        else:
            endtime = time.time() + timeout
            # Polling
            # TODO: implement with not_full condition variable
            success = False
            while not success and time.time() < endtime:
                success = ray.get(self.actor.put.remote(item))
            if not success:
                raise Full

    def get(self, block=True, timeout=None):
        if not block:
            success, item = ray.get(self.actor.get.remote())
            if not success:
                raise Empty
        elif timeout is None:
            # Polling
            # TODO: implement with not_empty condition variable
            success, item = ray.get(self.actor.get.remote())
            while not success:
                # Consider adding time.sleep here
                success, item = ray.get(self.actor.get.remote())
        elif timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        else:
            endtime = time.time() + timeout
            # Polling
            # TODO: implement with not_full condition variable
            success = False
            while not success and time.time() < endtime:
                success, item = ray.get(self.actor.get.remote())
            if not success:
                raise Empty
        return item

    def put_nowait(self, item):
        return self.put(block=False)

    def get_nowait(self):
        return self.get(block=False)


@ray.remote
class _QueueActor(object):
    def __init__(self, maxsize):
        self.maxsize = maxsize
        self._init(maxsize)

    def qsize(self):
        return self._qsize()

    def empty(self):
        return not self._qsize()

    def full(self):
        return 0 < self.maxsize <= self._qsize()

    def put(self, item):
        if self.maxsize > 0 and self._qsize() >= self.maxsize:
            return False
        self._put(item)
        return True

    def get(self):
        if not self._qsize():
            return False, None
        return True, self._get()

    # Override these for different queue implementations
    def _init(self, maxsize):
        self.queue = deque()

    def _qsize(self):
        return len(self.queue)

    def _put(self, item):
        self.queue.append(item)

    def _get(self):
        return self.queue.popleft()
