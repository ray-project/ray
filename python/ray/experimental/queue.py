import asyncio

import ray


class Empty(Exception):
    pass


class Full(Exception):
    pass


class Queue:
    """Queue implementation on Ray.

    Args:
        maxsize (int): maximum size of the queue. If zero, size is unbounded.
    """

    def __init__(self, maxsize=0):
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
        return ray.get(self.actor.empty.remote())

    def full(self):
        """Whether the queue is full."""
        return ray.get(self.actor.full.remote())

    def put(self, item, block=True, timeout=None):
        """Adds an item to the queue.

        There is no guarantee of order if multiple producers put to the same
        full queue.

        Raises:
            Full if the queue is full and blocking is False.
            Full if the queue is full, blocking is True, and it timed out.
            ValueError if timeout is negative.
        """
        if not block:
            try:
                ray.get(self.actor.put_nowait.remote(item))
            except asyncio.QueueFull:
                raise Full
        else:
            if timeout is not None and timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                ray.get(self.actor.put.remote(item, timeout))

    def get(self, block=True, timeout=None):
        """Gets an item from the queue.

        There is no guarantee of order if multiple consumers get from the
        same empty queue.

        Returns:
            The next item in the queue.

        Raises:
            Empty if the queue is empty and blocking is False.
            Empty if the queue is empty, blocking is True, and it timed out.
            ValueError if timeout is negative.
        """
        if not block:
            try:
                return ray.get(self.actor.get_nowait.remote())
            except asyncio.QueueEmpty:
                raise Empty
        else:
            if timeout is not None and timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                return ray.get(self.actor.get.remote(timeout))

    def put_nowait(self, item):
        """Equivalent to put(item, block=False).

        Raises:
            Full if the queue is full.
        """
        return self.put(item, block=False)

    def get_nowait(self):
        """Equivalent to get(block=False).

        Raises:
            Empty if the queue is empty.
        """
        return self.get(block=False)


@ray.remote
class _QueueActor:
    def __init__(self, maxsize):
        self.queue = asyncio.Queue(maxsize)

    def qsize(self):
        return self.queue.qsize()

    def empty(self):
        return self.queue.empty()

    def full(self):
        return self.queue.full()

    async def put(self, item, timeout=None):
        try:
            await asyncio.wait_for(self.queue.put(item), timeout)
        except asyncio.TimeoutError:
            raise Full

    async def get(self, timeout=None):
        try:
            return await asyncio.wait_for(self.queue.get(), timeout)
        except asyncio.TimeoutError:
            raise Empty

    def put_nowait(self, item):
        self.queue.put_nowait(item)

    def get_nowait(self):
        return self.queue.get_nowait()
