from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from typing import TypeVar, Generic, Iterable, List, Callable, Any

import ray

# The type of an iterator element.
T = TypeVar("T")


def from_items(items: List[T], num_shards: int = 2) -> "ParIterator[T]":
    """Create a parallel iterator from an existing set of objects.

    Arguments:
        items (list): The list of items to iterate over.
        num_shards (int): The number of worker actors to create.

    The objects will be divided round-robin among the number of shards."""
    shards = [[] for _ in range(num_shards)]
    for i, item in enumerate(items):
        shards[i % num_shards].append(item)
    return from_generators(shards)


def from_range(n: int, num_shards: int = 2) -> "ParIterator[int]":
    """Create a parallel iterator over the range 0..n.

    Arguments:
        n (int): The max end of the range of numbers.
        num_shards (int): The number of worker actors to create.

    The range will be split sequentially among the number of shards."""
    generators = []
    for i in range(num_shards):
        start = i * (n // num_shards)
        if i == num_shards - 1:
            end = n
        else:
            end = (i + 1) * (n // num_shards)
        generators.append(range(start, end))
    return from_generators(generators)


def from_generators(generators: List[Iterable[T]]) -> "ParIterator[T]":
    """Create a parallel iterator from a set of generators.

    Arguments:
        generators (list): A list of Python generator objects.

    An actor will be created for each generator."""
    worker_cls = ray.remote(_ParIteratorWorker)
    actors = [worker_cls.remote(g) for g in generators]
    return from_actors(actors)


def from_actors(actors: List["ray.actor.ActorHandle"]) -> "ParIterator[T]":
    """Create a parallel iterator from an existing set of actors.

    Arguments:
        actors (list): List of actors that each implement _ParIteratorWorker.

    Each actor must implement the par_iter_init() and par_iter_next() methods
    from the _ParIteratorWorker interface."""
    return ParIterator(actors)


class ParIterator(Generic[T]):
    """A parallel iterator over a set of remote actors.

    This can be used to iterate over a fixed set of task results
    (like an actor pool), or a stream of data (e.g., a fixed range of numbers,
    an infinite stream of RLlib rollout results).

    Examples:
        # Applying a function over items in parallel.
        >>> it = ray.experimental.iter.from_items([1, 2, 3], num_shards=2)
        ... <__main__.ParIterator object>
        >>> it = it.for_each(lambda x: x * 2).sync_iterator()
        ... <__main__.LocalIterator object>
        >>> print(list(it))
        ... [2, 4, 6]

        # Creating from generators.
        >>> it = ray.experimental.iter.from_generators([range(3), range(3)])
        ... <__main__.ParIterator object>
        >>> print(list(it.sync_iterator()))
        ... [0, 0, 1, 1, 2, 2]

        # Accessing the individual shards of an iterator.
        >>> it = ray.experimental.iter.from_range(10, num_shards=2)
        ... <__main__.ParIterator object>
        >>> it0 = it.get_shard(0)
        ... <__main__.LocalIterator object>
        >>> print(list(it0))
        ... [0, 1, 2, 3, 4]
        >>> it1 = it.get_shard(1)
        ... <__main__.LocalIterator object>
        >>> print(list(it1))
        ... [5, 6, 7, 8, 9]

        # Gathering results from actors synchronously in parallel.
        >>> it = ray.experimental.iter.from_actors(workers)
        ... <__main__.ParIterator object>
        >>> it = it.sync_iterator_across_shards()
        ... <__main__.LocalIterator object>
        >>> print(next(it))
        ... [worker_1_result_1, worker_2_result_1]
        >>> print(next(it))
        ... [worker_1_result_2, worker_2_result_2]

    This class is serializable and can be passed to other remote
    tasks and actors. However, each shard should be read from at most one
    process at a time.
    """

    def __init__(self,
                 actors: List["ray.actor.ActorHandle"],
                 transforms: List[Callable[["LocalIterator"],
                                           "LocalIterator"]] = None):
        self.actors = actors
        self.transforms = transforms or []

    def __iter__(self):
        raise TypeError(
            "You must use it.sync_iterator() or it.async_iterator() to "
            "iterate over the results of a ParIterator.")

    def for_each(self, fn: Callable[[T], T]) -> "ParIterator[T]":
        """Remotely apply fn to each item in this iterator.

        Arguments:
            fn (func): function to apply to each item.

        Examples:
            >>> next(from_range(4).filter(lambda x: x * 2).sync_iterator())
            ... [0, 2, 4, 8]
        """
        return ParIterator(
            self.actors,
            self.transforms + [lambda local_it: local_it.for_each(fn)])

    def filter(self, fn: Callable[[T], bool]) -> "ParIterator[T]":
        """Remotely filter items from this iterator.

        Arguments:
            fn (func): returns False for items to drop from the iterator.

        Examples:
            >>> next(from_items([0, 1, 2]).filter(lambda x: x).sync_iterator())
            ... [1, 2]
        """
        return ParIterator(
            self.actors,
            self.transforms + [lambda local_it: local_it.filter(fn)])

    def batch(self, n: int) -> "ParIterator[List[T]]":
        """Remotely batch together items in this iterator.

        Arguments:
            n (int): Number of items to batch together.

        Examples:
            >>> next(from_range(10, 1).batch(4).sync_iterator())
            ... [0, 1, 2, 3]
        """
        return ParIterator(
            self.actors,
            self.transforms + [lambda local_it: local_it.batch(n)])

    def flatten(self) -> "ParIterator[T[0]]":
        """Flatten batches of items into individual items.

        Examples:
            >>> next(from_range(10, 1).batch(4).flatten())
            ... 0
        """
        return ParIterator(
            self.actors,
            self.transforms + [lambda local_it: local_it.flatten()])

    def sync_iterator(self) -> "LocalIterator[T]":
        """Returns a local iterable for synchronous iteration.

        New items will be fetched from the shards on-demand as the iterator
        is stepped through.

        This is the equivalent of sync_iterator_across_shards().flatten().

        Examples:
            >>> it = from_range(100, 1).sync_iterator()
            >>> next(it)
            ... 0
            >>> next(it)
            ... 1
            >>> next(it)
            ... 2
        """
        return self.sync_iterator_across_shards().flatten()

    def sync_iterator_across_shards(self) -> "LocalIterator[List[T]]":
        """Iterate over the results of multiple shards in parallel.

        Examples:
            >>> it = from_generators([range(3), range(3)])
            >>> next(it.sync_iterator_across_shards())
            ... [0, 0]
        """

        def base_iterator():
            ray.get(
                [a.par_iter_init.remote(self.transforms) for a in self.actors])
            while True:
                try:
                    yield ray.get(
                        [a.par_iter_next.remote() for a in self.actors])
                except StopIteration:
                    break

        return LocalIterator(base_iterator)

    def async_iterator(self) -> "LocalIterator[T]":
        """Returns a local iterable for asynchronous iteration.

        New items will be fetched from the shards asynchronously as soon as
        the previous one is computed. Items arrive in non-deterministic order.

        Examples:
            >>> it = from_range(100, 1).sync_iterator()
            >>> next(it)
            ... 3
            >>> next(it)
            ... 0
            >>> next(it)
            ... 1
        """

        def base_iterator():
            ray.get(
                [a.par_iter_init.remote(self.transforms) for a in self.actors])
            futures = {}
            for a in self.actors:
                futures[a.par_iter_next.remote()] = a
            while futures:
                [obj_id], _ = ray.wait(list(futures), num_returns=1)
                actor = futures.pop(obj_id)
                try:
                    yield ray.get(obj_id)
                    futures[actor.par_iter_next.remote()] = actor
                except StopIteration:
                    pass

        return LocalIterator(base_iterator)

    def num_shards(self) -> int:
        """Return the number of worker actors backing this iterator."""
        return len(self.actors)

    def get_shard(self, shard_index: int) -> "LocalIterator[T]":
        """Return a local iterator for the given shard.

        The iterator is guaranteed to be serializable and can be passed to
        remote tasks or actors.
        """
        a = self.actors[shard_index]
        return _SingleActorIterator(a, self.transforms)


class _ParIteratorWorker(object):
    """Worker actor for a ParIterator."""

    def __init__(self, items):
        self.items = items
        self.transforms = []
        self.local_it = None

    def par_iter_init(self, transforms):
        it = LocalIterator(lambda: self.items)
        for fn in transforms:
            it = fn(it)
            assert it is not None, fn
        self.local_it = it.__iter__()

    def par_iter_next(self):
        assert self.local_it is not None, "must call par_iterator_init()"
        return next(self.local_it)


class LocalIterator(Generic[T]):
    """An iterator over a single shard of data.

    It implements similar transformations as ParIterator[T], but the transforms
    will be applied locally and not remotely in parallel.

    This class is serializable and can be passed to other remote
    tasks and actors. However, it should be read from at most one process at
    a time."""

    def __init__(self,
                 base_iterator: Callable[[], Iterable[T]],
                 local_transforms: List[Callable[[Iterable], Any]] = None):
        self.base_iterator = base_iterator
        self.built_iterator = None
        self.local_transforms = local_transforms or []

    def _build_once(self):
        if self.built_iterator is None:
            it = iter(self.base_iterator())
            for fn in self.local_transforms:
                it = fn(it)
            self.built_iterator = it

    def __iter__(self):
        self._build_once()
        return self.built_iterator

    def __next__(self):
        self._build_once()
        return next(self.built_iterator)

    def for_each(self, fn: Callable[[T], T]) -> "LocalIterator[T]":
        def apply_foreach(it):
            for item in it:
                yield fn(item)

        return LocalIterator(self.base_iterator,
                             self.local_transforms + [apply_foreach])

    def filter(self, fn: Callable[[T], bool]) -> "LocalIterator[T]":
        def apply_filter(it):
            for item in it:
                if fn(item):
                    yield item

        return LocalIterator(self.base_iterator,
                             self.local_transforms + [apply_filter])

    def batch(self, n: int) -> "LocalIterator[List[T]]":
        def apply_batch(it):
            batch = []
            for item in it:
                batch.append(item)
                if len(batch) >= n:
                    yield batch
                    batch = []
            if batch:
                yield batch

        return LocalIterator(self.base_iterator,
                             self.local_transforms + [apply_batch])

    def flatten(self) -> "LocalIterator[T[0]]":
        def apply_flatten(it):
            for item in it:
                for subitem in item:
                    yield subitem

        return LocalIterator(self.base_iterator,
                             self.local_transforms + [apply_flatten])


class _SingleActorIterator(LocalIterator):
    """Helper class for iterating over a single worker actor's results."""

    def __init__(self, actor, transforms):
        self.actor = actor
        self.transforms = transforms

    def __iter__(self):
        ray.get(self.actor.par_iter_init.remote(self.transforms))
        while True:
            try:
                yield ray.get(self.actor.par_iter_next.remote())
            except StopIteration:
                break
