from typing import Callable


def with_lock(func: Callable):
    """Use as decorator (@withlock) around object methods that need locking.

    Note: The object must have a self._lock = threading.Lock() property.
    Locking thus works on the object level (no two locked methods of the same
    object can be called asynchronously).

    Args:
        func (Callable): The function to decorate/wrap.

    Returns:
        Callable: The wrapped (object-level locked) function.
    """

    def wrapper(self, *a, **k):
        try:
            with self._lock:
                return func(self, *a, **k)
        except AttributeError:
            raise AttributeError(
                "Object {} must have a `self._lock` property (assigned to a "
                "threading.Lock() object in its constructor)!".format(self))

    return wrapper
