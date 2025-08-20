from collections import deque
import threading
import queue


class BoundedQueue:
    """
    A light weight thread-safe bounded queue that automatically drops the oldest item when full.

    This implementation uses a deque with maxlen for automatic oldest-item eviction
    and provides non-blocking put/get operations.
    """

    def __init__(self, max_size):
        if not isinstance(max_size, int) or max_size <= 0:
            raise ValueError("max_size must be a positive integer")

        self._queue = deque(maxlen=max_size)
        self._lock = threading.Lock()
        self.max_size = max_size

    def put(self, item) -> int:
        """
        Add an item to the queue, dropping the oldest item if the queue is full.

        This operation is thread-safe and non-blocking. If the queue is at capacity,
        the oldest item is automatically removed to make space for the new item.

        Returns:
            bool: whether an item was dropped to make space for the new item or not
        """
        dropped = False
        with self._lock:
            if len(self._queue) >= self.max_size:
                dropped = True
            # deque with maxlen automatically handles drop-oldest
            self._queue.append(item)
        return dropped

    def get(self):
        """
        Remove and return the oldest item from the queue.

        This operation is thread-safe and non-blocking. If the queue is empty,
        raises queue.Empty exception.

        Returns:
            The oldest item in the queue.
        """
        with self._lock:
            if len(self._queue) == 0:
                raise queue.Empty("Queue is empty")
            return self._queue.popleft()
