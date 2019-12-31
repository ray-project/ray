from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from typing import TypeVar, Generic, Iterable, List, Callable, Any

import ray

# The type of an iterator element.
T = TypeVar("T")


class YieldIterator(Exception):
    """Indicates that a local iterator has no value available.

    This is used internally to implement the union() of multiple blocking
    local generators."""
    pass


def from_items(items: List[T], num_shards: int = 2) -> "ParIterator[T]":
    """Create a parallel iterator from an existing set of objects.

    The objects will be divided round-robin among the number of shards.

    Arguments:
        items (list): The list of items to iterate over.
        num_shards (int): The number of worker actors to create.
    """
    shards = [[] for _ in range(num_shards)]
    for i, item in enumerate(items):
        shards[i % num_shards].append(item)
    name = "from_items[{}, {}, shards={}]".format(
        items and type(items[0]).__name__ or "None", len(items), num_shards)
    return from_iterators(shards, name=name)


def from_range(n: int, num_shards: int = 2) -> "ParIterator[int]":
    """Create a parallel iterator over the range 0..n.

    The range will be partitioned sequentially among the number of shards.

    Arguments:
        n (int): The max end of the range of numbers.
        num_shards (int): The number of worker actors to create.
    """
    generators = []
    for i in range(num_shards):
        start = i * (n // num_shards)
        if i == num_shards - 1:
            end = n
        else:
            end = (i + 1) * (n // num_shards)
        generators.append(range(start, end))
    name = "from_range[{}, shards={}]".format(n, num_shards)
    return from_iterators(generators, name=name)


def from_iterators(generators: List[Iterable[T]],
                   name=None) -> "ParIterator[T]":
    """Create a parallel iterator from a set of iterators.

    An actor will be created for each iterator.

    Arguments:
        generators (list): A list of Python generator objects or lambda
            functions that produced a generator when called.
        name (str): Optional name to give the iterator.
    """
    worker_cls = ray.remote(_ParIteratorWorker)
    actors = [worker_cls.remote(g) for g in generators]
    if not name:
        name = "from_iterators[shards={}]".format(len(generators))
    return from_actors(actors, name=name)


def from_actors(actors: List["ray.actor.ActorHandle"],
                name=None) -> "ParIterator[T]":
    """Create a parallel iterator from an existing set of actors.

    Each actor must implement the par_iter_init() and par_iter_next() methods
    from the _ParIteratorWorker interface.

    Arguments:
        actors (list): List of actors that each implement _ParIteratorWorker.
        name (str): Optional name to give the iterator.
    """
    if not name:
        name = "from_actors[shards={}]".format(len(actors))
    return ParIterator([_ActorSet(actors, [])], name)


class ParIterator(Generic[T]):
    """A parallel iterator over a set of remote actors.

    This can be used to iterate over a fixed set of task results
    (like an actor pool), or a stream of data (e.g., a fixed range of numbers,
    an infinite stream of RLlib rollout results).

    This class is **serializable** and can be passed to other remote
    tasks and actors. However, each shard should be read from at most one
    process at a time.

    Examples:
        # Applying a function over items in parallel.
        >>> it = ray.experimental.iter.from_items([1, 2, 3], num_shards=2)
        ... <__main__.ParIterator object>
        >>> it = it.for_each(lambda x: x * 2).sync_iterator()
        ... <__main__.LocalIterator object>
        >>> print(list(it))
        ... [2, 4, 6]

        # Creating from generators.
        >>> it = ray.experimental.iter.from_iterators([range(3), range(3)])
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
        >>> it = it.batch_across_shards()
        ... <__main__.LocalIterator object>
        >>> print(next(it))
        ... [worker_1_result_1, worker_2_result_1]
        >>> print(next(it))
        ... [worker_1_result_2, worker_2_result_2]
    """

    def __init__(self, actor_sets: List["_ActorSet"], name: str):
        # We track multiple sets of actors to support parallel .union().
        self.actor_sets = actor_sets
        self.name = name

    def __iter__(self):
        raise TypeError(
            "You must use it.sync_iterator() or it.async_iterator() to "
            "iterate over the results of a ParIterator.")

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def for_each(self, fn: Callable[[T], T]) -> "ParIterator[T]":
        """Remotely apply fn to each item in this iterator.

        Arguments:
            fn (func): function to apply to each item.

        Examples:
            >>> next(from_range(4).filter(lambda x: x * 2).sync_iterator())
            ... [0, 2, 4, 8]
        """
        return ParIterator(
            [
                a.with_transform(lambda local_it: local_it.for_each(fn))
                for a in self.actor_sets
            ],
            name=self.name + ".for_each()")

    def filter(self, fn: Callable[[T], bool]) -> "ParIterator[T]":
        """Remotely filter items from this iterator.

        Arguments:
            fn (func): returns False for items to drop from the iterator.

        Examples:
            >>> next(from_items([0, 1, 2]).filter(lambda x: x).sync_iterator())
            ... [1, 2]
        """
        return ParIterator(
            [
                a.with_transform(lambda local_it: local_it.filter(fn))
                for a in self.actor_sets
            ],
            name=self.name + ".filter()")

    def batch(self, n: int) -> "ParIterator[List[T]]":
        """Remotely batch together items in this iterator.

        Arguments:
            n (int): Number of items to batch together.

        Examples:
            >>> next(from_range(10, 1).batch(4).sync_iterator())
            ... [0, 1, 2, 3]
        """
        return ParIterator(
            [
                a.with_transform(lambda local_it: local_it.batch(n))
                for a in self.actor_sets
            ],
            name=self.name + ".batch({})".format(n))

    def flatten(self) -> "ParIterator[T[0]]":
        """Flatten batches of items into individual items.

        Examples:
            >>> next(from_range(10, 1).batch(4).flatten())
            ... 0
        """
        return ParIterator(
            [
                a.with_transform(lambda local_it: local_it.flatten())
                for a in self.actor_sets
            ],
            name=self.name + ".flatten()")

    def sync_iterator(self) -> "LocalIterator[T]":
        """Returns a local iterable for synchronous iteration.

        New items will be fetched from the shards on-demand as the iterator
        is stepped through.

        This is the equivalent of batch_across_shards().flatten().

        Examples:
            >>> it = from_range(100, 1).sync_iterator()
            >>> next(it)
            ... 0
            >>> next(it)
            ... 1
            >>> next(it)
            ... 2
        """
        it = self.batch_across_shards().flatten()
        it.name = "{}.sync_iterator()".format(self)
        return it

    def batch_across_shards(self) -> "LocalIterator[List[T]]":
        """Iterate over the results of multiple shards in parallel.

        Examples:
            >>> it = from_iterators([range(3), range(3)])
            >>> next(it.batch_across_shards())
            ... [0, 0]
        """

        def base_iterator(timeout=None):
            active = []
            for actor_set in self.actor_sets:
                actor_set.init_actors()
                active.extend(actor_set.actors)
            futures = [a.par_iter_next.remote() for a in active]
            while active:
                try:
                    yield ray.get(futures, timeout=timeout)
                    futures = [a.par_iter_next.remote() for a in active]
                    # Always yield after each round of gets with timeout.
                    if timeout is not None:
                        yield YieldIterator()
                except TimeoutError:
                    yield YieldIterator()
                except StopIteration:
                    # Find and remove the actor that produced StopIteration.
                    results = []
                    for a, f in zip(list(active), futures):
                        try:
                            results.append(ray.get(f))
                        except StopIteration:
                            active.remove(a)
                    if results:
                        yield results
                    futures = [a.par_iter_next.remote() for a in active]

        name = "{}.batch_across_shards()".format(self)
        return LocalIterator(base_iterator, name=name)

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

        def base_iterator(timeout=None):
            all_actors = []
            for actor_set in self.actor_sets:
                actor_set.init_actors()
                all_actors.extend(actor_set.actors)
            futures = {}
            for a in all_actors:
                futures[a.par_iter_next.remote()] = a
            while futures:
                pending = list(futures)
                if timeout is None:
                    # First try to do a batch wait for efficiency.
                    ready, _ = ray.wait(
                        pending, num_returns=len(pending), timeout=0)
                    # Fall back to a blocking wait.
                    if not ready:
                        ready, _ = ray.wait(pending, num_returns=1)
                else:
                    ready, _ = ray.wait(
                        pending, num_returns=len(pending), timeout=timeout)
                for obj_id in ready:
                    actor = futures.pop(obj_id)
                    try:
                        yield ray.get(obj_id)
                        futures[actor.par_iter_next.remote()] = actor
                    except StopIteration:
                        pass
                # Always yield after each round of wait with timeout.
                if timeout is not None:
                    yield YieldIterator()

        name = "{}.async_iterator()".format(self)
        return LocalIterator(base_iterator, name=name)

    def union(self, other: "ParIterator[T]") -> "ParIterator[T]":
        """Return an iterator that is the union of this and the other."""
        actor_sets = []
        actor_sets.extend(self.actor_sets)
        actor_sets.extend(other.actor_sets)
        return ParIterator(actor_sets, "ParUnion[{}, {}]".format(self, other))

    def num_shards(self) -> int:
        """Return the number of worker actors backing this iterator."""
        return sum(len(a.actors) for a in self.actor_sets)

    def get_shard(self, shard_index: int) -> "LocalIterator[T]":
        """Return a local iterator for the given shard.

        The iterator is guaranteed to be serializable and can be passed to
        remote tasks or actors.
        """
        a, t = None, None
        i = shard_index
        for actor_set in self.actor_sets:
            if i < len(actor_set.actors):
                a = actor_set.actors[i]
                t = actor_set.transforms
                break
            else:
                i -= len(actor_set.actors)
        if a is None:
            raise ValueError("Shard index out of range", shard_index,
                             self.num_shards())
        return _SingleActorIterator(a, t)


class LocalIterator(Generic[T]):
    """An iterator over a single shard of data.

    It implements similar transformations as ParIterator[T], but the transforms
    will be applied locally and not remotely in parallel.

    This class is **serializable** and can be passed to other remote
    tasks and actors. However, it should be read from at most one process at
    a time."""

    def __init__(self,
                 base_iterator: Callable[[], Iterable[T]],
                 local_transforms: List[Callable[[Iterable], Any]] = None,
                 timeout: int = None,
                 name=None):
        self.base_iterator = base_iterator
        self.built_iterator = None
        self.local_transforms = local_transforms or []
        self.timeout = timeout
        if name is None:
            name = "LocalIterator[?]"
        self.name = name

    def _build_once(self):
        if self.built_iterator is None:
            it = iter(self.base_iterator(self.timeout))
            for fn in self.local_transforms:
                it = fn(it)
            self.built_iterator = it

    def __iter__(self):
        self._build_once()
        return self.built_iterator

    def __next__(self):
        self._build_once()
        return next(self.built_iterator)

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def for_each(self, fn: Callable[[T], T]) -> "LocalIterator[T]":
        def apply_foreach(it):
            for item in it:
                if isinstance(item, YieldIterator):
                    yield item
                else:
                    yield fn(item)

        return LocalIterator(
            self.base_iterator,
            self.local_transforms + [apply_foreach],
            name=self.name + ".for_each()")

    def filter(self, fn: Callable[[T], bool]) -> "LocalIterator[T]":
        def apply_filter(it):
            for item in it:
                if isinstance(item, YieldIterator):
                    yield item
                elif fn(item):
                    yield item

        return LocalIterator(
            self.base_iterator,
            self.local_transforms + [apply_filter],
            name=self.name + ".filter()")

    def batch(self, n: int) -> "LocalIterator[List[T]]":
        def apply_batch(it):
            batch = []
            for item in it:
                if isinstance(item, YieldIterator):
                    yield item
                else:
                    batch.append(item)
                    if len(batch) >= n:
                        yield batch
                        batch = []
            if batch:
                yield batch

        return LocalIterator(
            self.base_iterator,
            self.local_transforms + [apply_batch],
            name=self.name + ".batch({})".format(n))

    def flatten(self) -> "LocalIterator[T[0]]":
        def apply_flatten(it):
            for item in it:
                if isinstance(item, YieldIterator):
                    yield item
                else:
                    for subitem in item:
                        yield subitem

        return LocalIterator(
            self.base_iterator,
            self.local_transforms + [apply_flatten],
            name=self.name + ".flatten()")

    def union(self, other: "LocalIterator[T]") -> "LocalIterator[T]":
        """Return an iterator that is the union of this and the other.

        This works by alternating waits/gets between the two iterators with
        timeout=0. This may be less efficient that calling union on the
        underlying ParIterators, but is more flexible in that local
        transformations can be made at the local iterator level prior to the
        union call."""

        it1 = LocalIterator(
            self.base_iterator, self.local_transforms, timeout=0)
        it2 = LocalIterator(
            other.base_iterator, other.local_transforms, timeout=0)
        active = [it1, it2]

        def build_union(timeout=None):
            while True:
                for it in list(active):
                    # Yield items from the iterator until YieldIterator is
                    # found, then switch to the next iterator.
                    try:
                        while True:
                            item = next(it)
                            if isinstance(item, YieldIterator):
                                break
                            else:
                                yield item
                    except StopIteration:
                        active.remove(it)
                if not active:
                    break

        return LocalIterator(
            build_union, [], name="LocalUnion[{}, {}]".format(self, other))


class _ParIteratorWorker(object):
    """Worker actor for a ParIterator."""

    def __init__(self, item_generator):
        if callable(item_generator):
            self.item_generator = item_generator()
        else:
            self.item_generator = item_generator
        self.transforms = []
        self.local_it = None

    def par_iter_init(self, transforms):
        it = LocalIterator(lambda timeout: self.item_generator)
        for fn in transforms:
            it = fn(it)
            assert it is not None, fn
        self.local_it = it.__iter__()

    def par_iter_next(self):
        assert self.local_it is not None, "must call par_iterator_init()"
        return next(self.local_it)


class _ActorSet(object):
    """Helper class that represents a set of actors and transforms."""

    def __init__(
            self, actors: List["ray.actor.ActorHandle"],
            transforms: List[Callable[["LocalIterator"], "LocalIterator"]]):
        self.actors = actors
        self.transforms = transforms

    def init_actors(self):
        ray.get([a.par_iter_init.remote(self.transforms) for a in self.actors])

    def with_transform(self, fn):
        return _ActorSet(self.actors, self.transforms + [fn])


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
