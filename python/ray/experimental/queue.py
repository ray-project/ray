from collections import deque
import time

import ray


class Empty(Exception):
    pass


class Full(Exception):
    pass


class Queue:
    """Queue implementation on Ray.

    Args:
        maxsize (int): maximum size of the queue. If zero, size is unboundend.
    """

    def __init__(self, maxsize=0):
        self.maxsize = maxsize
        self.actor = _QueueActor.remote(maxsize)

    def __len__(self):
        return self.size()

    def size(self):
        """The size of the queue."""
        return ray.get(self.actor.qsize.remote())

    def qsize(self):
        """The size of the queue."""
        return self.size()

    def empty(self):
        """Whether the queue is empty."""
        return ray.get(self.actor.qsize.remote())

    def full(self):
        """Whether the queue is full."""
        return ray.get(self.actor.full.remote())

    def put(self, item, block=True, timeout=None):
        """Adds an item to the queue.

        Uses polling if block=True, so there is no guarantee of order if
        multiple producers put to the same full queue.

        Raises:
            Full if the queue is full and blocking is False.
        """
        if self.maxsize <= 0:
            self.actor.put.remote(item)
        elif not block:
            if not ray.get(self.actor.put.remote(item)):
                raise Full
        elif timeout is None:
            # Polling
            # Use a not_full condition variable or promise?
            while not ray.get(self.actor.put.remote(item)):
                # Consider adding time.sleep here
                pass
        elif timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        else:
            endtime = time.time() + timeout
            # Polling
            # Use a condition variable or switch to promise?
            success = False
            while not success and time.time() < endtime:
                success = ray.get(self.actor.put.remote(item))
            if not success:
                raise Full

    def get(self, block=True, timeout=None):
        """Gets an item from the queue.

        Uses polling if block=True, so there is no guarantee of order if
        multiple consumers get from the same empty queue.

        Returns:
            The next item in the queue.

        Raises:
            Empty if the queue is empty and blocking is False.
        """
        if not block:
            success, item = ray.get(self.actor.get.remote())
            if not success:
                raise Empty
        elif timeout is None:
            # Polling
            # Use a not_empty condition variable or return a promise?
            success, item = ray.get(self.actor.get.remote())
            while not success:
                # Consider adding time.sleep here
                success, item = ray.get(self.actor.get.remote())
        elif timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        else:
            endtime = time.time() + timeout
            # Polling
            # Use a not_full condition variable or return a promise?
            success = False
            while not success and time.time() < endtime:
                success, item = ray.get(self.actor.get.remote())
            if not success:
                raise Empty
        return item

    def put_nowait(self, item):
        """Equivalent to put(item, block=False).

        Raises:
            Full if the queue is full.
        """
        return self.put(item, block=False)

    def get_nowait(self):
        """Equivalent to get(item, block=False).

        Raises:
            Empty if the queue is empty.
        """
        return self.get(block=False)


@ray.remote
class _QueueActor:
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
