from collections import Counter, defaultdict
from typing import Dict, Generator, List, Optional, TypeVar

# Grouping key - must be hashable
T = TypeVar("T")
# Objects to cache
U = TypeVar("U")


class _ObjectCache:
    """Cache up to some maximum count given a grouping key.

    This object cache can e.g. be used to cache Ray Tune trainable actors
    given their resource requirements (reuse_actors=True).

    If the max number of cached objects for a grouping key is reached,
    no more objects for this group will be cached.

    However, if `may_keep_one=True`, one object (globally across all grouping
    keys) may be cached, even if the max number of objects is 0. This is to
    allow to cache an object if the max number of objects of this key
    will increase shortly after (as is the case e.g. in the Ray Tune control
    loop).

    Args:
        may_keep_one: If True, one object (globally) may be cached if no desired
            maximum objects are defined.

    """

    def __init__(self, may_keep_one: bool = True):
        self._num_cached_objects: int = 0
        self._cached_objects: Dict[T, List[U]] = defaultdict(list)
        self._max_num_objects: Counter[T] = Counter()

        self._may_keep_one = may_keep_one

    @property
    def num_cached_objects(self):
        return self._num_cached_objects

    @property
    def total_max_objects(self):
        # Counter.total() is only available for python 3.10+
        return sum(self._max_num_objects.values())

    def increase_max(self, key: T, by: int = 1) -> None:
        """Increase number of max objects for this key.

        Args:
            key: Group key.
            by: Decrease by this amount.
        """
        self._max_num_objects[key] += by

    def decrease_max(self, key: T, by: int = 1) -> None:
        """Decrease number of max objects for this key.

        Args:
            key: Group key.
            by: Decrease by this amount.
        """
        self._max_num_objects[key] -= by

    def has_cached_object(self, key: T) -> bool:
        """Return True if at least one cached object exists for this key.

        Args:
            key: Group key.

        Returns:
            True if at least one cached object exists for this key.
        """
        return bool(self._cached_objects[key])

    def cache_object(self, key: T, obj: U) -> bool:
        """Cache object for a given key.

        This will put the object into a cache, assuming the number
        of cached objects for this key is less than the number of
        max objects for this key.

        An exception is made if `max_keep_one=True` and no other
        objects are cached globally. In that case, the object can
        still be cached.

        Args:
            key: Group key.
            obj: Object to cache.

        Returns:
            True if the object has been cached. False otherwise.

        """
        # If we have more objects cached already than we desire
        if len(self._cached_objects[key]) >= self._max_num_objects[key]:
            # If may_keep_one is False, never cache
            if not self._may_keep_one:
                return False

            # If we have more than one other cached object, don't cache
            if self._num_cached_objects > 0:
                return False

            # If any other objects are expected to be cached, don't cache
            if any(v for v in self._max_num_objects.values()):
                return False

        # Otherwise, cache (for now).

        self._cached_objects[key].append(obj)
        self._num_cached_objects += 1
        return True

    def pop_cached_object(self, key: T) -> Optional[U]:
        """Get one cached object for a key.

        This will remove the object from the cache.

        Args:
            key: Group key.

        Returns:
            Cached object.
        """
        if not self.has_cached_object(key):
            return None

        self._num_cached_objects -= 1
        return self._cached_objects[key].pop(0)

    def flush_cached_objects(self, force_all: bool = False) -> Generator[U, None, None]:
        """Return a generator over cached objects evicted from the cache.

        This method yields all cached objects that should be evicted from the
        cache for cleanup by the caller.

        If the number of max objects is lower than the number of
        cached objects for a given key, objects are evicted until
        the numbers are equal.

        If `max_keep_one=True` (and ``force_all=False``), one cached object
        may be retained.

        Objects are evicted FIFO.

        If ``force_all=True``, all objects are evicted.

        Args:
            force_all: If True, all objects are flushed. This takes precedence
                over ``keep_one``.

        Yields:
            Evicted objects to be cleaned up by caller.

        """
        # If force_all=True, don't keep one.
        keep_one = self._may_keep_one and not force_all

        for key, objs in self._cached_objects.items():
            max_cached = self._max_num_objects[key] if not force_all else 0

            if (
                self._num_cached_objects == 1
                and keep_one
                # Only keep this object if we don't expect a different one
                and not any(v for v in self._max_num_objects.values())
            ):
                break

            while len(objs) > max_cached:
                self._num_cached_objects -= 1
                yield objs.pop(0)
