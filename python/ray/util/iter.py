from typing import TypeVar, Generic, Iterable, List, Callable, Any
import random

import ray

# The type of an iterator element.
T = TypeVar("T")
U = TypeVar("U")


def from_items(items: List[T], num_shards: int = 2,
               repeat: bool = False) -> "ParallelIterator[T]":
    """Create a parallel iterator from an existing set of objects.

    The objects will be divided round-robin among the number of shards.

    Args:
        items (list): The list of items to iterate over.
        num_shards (int): The number of worker actors to create.
        repeat (bool): Whether to cycle over the items forever.
    """
    shards = [[] for _ in range(num_shards)]
    for i, item in enumerate(items):
        shards[i % num_shards].append(item)
    name = "from_items[{}, {}, shards={}{}]".format(
        items and type(items[0]).__name__ or "None", len(items), num_shards,
        ", repeat=True" if repeat else "")
    return from_iterators(shards, repeat=repeat, name=name)


def from_range(n: int, num_shards: int = 2,
               repeat: bool = False) -> "ParallelIterator[int]":
    """Create a parallel iterator over the range 0..n.

    The range will be partitioned sequentially among the number of shards.

    Args:
        n (int): The max end of the range of numbers.
        num_shards (int): The number of worker actors to create.
        repeat (bool): Whether to cycle over the range forever.
    """
    generators = []
    shard_size = n // num_shards
    for i in range(num_shards):
        start = i * shard_size
        if i == num_shards - 1:
            end = n
        else:
            end = (i + 1) * shard_size
        generators.append(range(start, end))
    name = "from_range[{}, shards={}{}]".format(
        n, num_shards, ", repeat=True" if repeat else "")
    return from_iterators(generators, repeat=repeat, name=name)


def from_iterators(generators: List[Iterable[T]],
                   repeat: bool = False,
                   name=None) -> "ParallelIterator[T]":
    """Create a parallel iterator from a set of iterators.

    An actor will be created for each iterator.

    Examples:
        >>> # Create using a list of generators.
        >>> from_iterators([range(100), range(100)])

        >>> # Equivalent to the above.
        >>> from_iterators([lambda: range(100), lambda: range(100)])

    Args:
        generators (list): A list of Python generator objects or lambda
            functions that produced a generator when called. We allow lambda
            functions since the generator itself might not be serializable,
            but a lambda that returns it can be.
        repeat (bool): Whether to cycle over the iterators forever.
        name (str): Optional name to give the iterator.
    """
    worker_cls = ray.remote(ParallelIteratorWorker)
    actors = [worker_cls.remote(g, repeat) for g in generators]
    if not name:
        name = "from_iterators[shards={}{}]".format(
            len(generators), ", repeat=True" if repeat else "")
    return from_actors(actors, name=name)


def from_actors(actors: List["ray.actor.ActorHandle"],
                name=None) -> "ParallelIterator[T]":
    """Create a parallel iterator from an existing set of actors.

    Each actor must subclass the ParallelIteratorWorker interface.

    Args:
        actors (list): List of actors that each implement
            ParallelIteratorWorker.
        name (str): Optional name to give the iterator.
    """
    if not name:
        name = "from_actors[shards={}]".format(len(actors))
    return ParallelIterator([_ActorSet(actors, [])], name)


class ParallelIterator(Generic[T]):
    """A parallel iterator over a set of remote actors.

    This can be used to iterate over a fixed set of task results
    (like an actor pool), or a stream of data (e.g., a fixed range of numbers,
    an infinite stream of RLlib rollout results).

    This class is **serializable** and can be passed to other remote
    tasks and actors. However, each shard should be read from at most one
    process at a time.

    Examples:
        >>> # Applying a function over items in parallel.
        >>> it = ray.util.iter.from_items([1, 2, 3], num_shards=2)
        ... <__main__.ParallelIterator object>
        >>> it = it.for_each(lambda x: x * 2).gather_sync()
        ... <__main__.LocalIterator object>
        >>> print(list(it))
        ... [2, 4, 6]

        >>> # Creating from generators.
        >>> it = ray.util.iter.from_iterators([range(3), range(3)])
        ... <__main__.ParallelIterator object>
        >>> print(list(it.gather_sync()))
        ... [0, 0, 1, 1, 2, 2]

        >>> # Accessing the individual shards of an iterator.
        >>> it = ray.util.iter.from_range(10, num_shards=2)
        ... <__main__.ParallelIterator object>
        >>> it0 = it.get_shard(0)
        ... <__main__.LocalIterator object>
        >>> print(list(it0))
        ... [0, 1, 2, 3, 4]
        >>> it1 = it.get_shard(1)
        ... <__main__.LocalIterator object>
        >>> print(list(it1))
        ... [5, 6, 7, 8, 9]

        >>> # Gathering results from actors synchronously in parallel.
        >>> it = ray.util.iter.from_actors(workers)
        ... <__main__.ParallelIterator object>
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
            "You must use it.gather_sync() or it.gather_async() to "
            "iterate over the results of a ParallelIterator.")

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "ParallelIterator[{}]".format(self.name)

    def for_each(self, fn: Callable[[T], U]) -> "ParallelIterator[U]":
        """Remotely apply fn to each item in this iterator.

        Args:
            fn (func): function to apply to each item.

        Examples:
            >>> next(from_range(4).for_each(lambda x: x * 2).gather_sync())
            ... [0, 2, 4, 8]
        """
        return ParallelIterator(
            [
                a.with_transform(lambda local_it: local_it.for_each(fn))
                for a in self.actor_sets
            ],
            name=self.name + ".for_each()")

    def filter(self, fn: Callable[[T], bool]) -> "ParallelIterator[T]":
        """Remotely filter items from this iterator.

        Args:
            fn (func): returns False for items to drop from the iterator.

        Examples:
            >>> it = from_items([0, 1, 2]).filter(lambda x: x > 0)
            >>> next(it.gather_sync())
            ... [1, 2]
        """
        return ParallelIterator(
            [
                a.with_transform(lambda local_it: local_it.filter(fn))
                for a in self.actor_sets
            ],
            name=self.name + ".filter()")

    def batch(self, n: int) -> "ParallelIterator[List[T]]":
        """Remotely batch together items in this iterator.

        Args:
            n (int): Number of items to batch together.

        Examples:
            >>> next(from_range(10, 1).batch(4).gather_sync())
            ... [0, 1, 2, 3]
        """
        return ParallelIterator(
            [
                a.with_transform(lambda local_it: local_it.batch(n))
                for a in self.actor_sets
            ],
            name=self.name + ".batch({})".format(n))

    def flatten(self) -> "ParallelIterator[T[0]]":
        """Flatten batches of items into individual items.

        Examples:
            >>> next(from_range(10, 1).batch(4).flatten())
            ... 0
        """
        return ParallelIterator(
            [
                a.with_transform(lambda local_it: local_it.flatten())
                for a in self.actor_sets
            ],
            name=self.name + ".flatten()")

    def combine(self, fn: Callable[[T], List[U]]) -> "ParallelIterator[U]":
        """Transform and then combine items horizontally.

        This is the equivalent of for_each(fn).flatten() (flat map).
        """
        it = self.for_each(fn).flatten()
        it.name = self.name + ".combine()"
        return it

    def local_shuffle(self, shuffle_buffer_size: int,
                      seed: int = None) -> "ParallelIterator[T]":
        """Remotely shuffle items of each shard independently

        Args:
            shuffle_buffer_size (int): The algorithm fills a buffer with
                shuffle_buffer_size elements and randomly samples elements from
                this buffer, replacing the selected elements with new elements.
                For perfect shuffling, this argument should be greater than or
                equal to the largest iterator size.
            seed (int): Seed to use for
                randomness. Default value is None.

        Returns:
            Returns a ParallelIterator with a local shuffle applied on the
                base iterator

        Examples:
            >>> it = from_range(10, 1).local_shuffle(shuffle_buffer_size=2)
            >>> it = it.gather_sync()
            >>> next(it)
            0
            >>> next(it)
            2
            >>> next(it)
            3
            >>> next(it)
            1
        """
        return ParallelIterator(
            [
                a.with_transform(
                    lambda localit: localit.shuffle(shuffle_buffer_size, seed))
                for a in self.actor_sets
            ],
            name=self.name +
            ".local_shuffle(shuffle_buffer_size={}, seed={})".format(
                shuffle_buffer_size,
                str(seed) if seed is not None else "None"))

    def gather_sync(self) -> "LocalIterator[T]":
        """Returns a local iterable for synchronous iteration.

        New items will be fetched from the shards on-demand as the iterator
        is stepped through.

        This is the equivalent of batch_across_shards().flatten().

        Examples:
            >>> it = from_range(100, 1).gather_sync()
            >>> next(it)
            ... 0
            >>> next(it)
            ... 1
            >>> next(it)
            ... 2
        """
        it = self.batch_across_shards().flatten()
        it.name = "{}.gather_sync()".format(self)
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
                        yield _NextValueNotReady()
                except TimeoutError:
                    yield _NextValueNotReady()
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

    def gather_async(self) -> "LocalIterator[T]":
        """Returns a local iterable for asynchronous iteration.

        New items will be fetched from the shards asynchronously as soon as
        the previous one is computed. Items arrive in non-deterministic order.

        Examples:
            >>> it = from_range(100, 1).gather_async()
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
                    yield _NextValueNotReady()

        name = "{}.gather_async()".format(self)
        return LocalIterator(base_iterator, name=name)

    def take(self, n: int) -> List[T]:
        """Return up to the first n items from this iterator."""
        return self.gather_sync().take(n)

    def show(self, n: int = 20):
        """Print up to the first n items from this iterator."""
        return self.gather_sync().show(n)

    def union(self, other: "ParallelIterator[T]") -> "ParallelIterator[T]":
        """Return an iterator that is the union of this and the other."""
        if not isinstance(other, ParallelIterator):
            raise ValueError(
                "other must be of type ParallelIterator, got {}".format(
                    type(other)))
        actor_sets = []
        actor_sets.extend(self.actor_sets)
        actor_sets.extend(other.actor_sets)
        return ParallelIterator(actor_sets, "ParallelUnion[{}, {}]".format(
            self, other))

    def num_shards(self) -> int:
        """Return the number of worker actors backing this iterator."""
        return sum(len(a.actors) for a in self.actor_sets)

    def shards(self) -> List["LocalIterator[T]"]:
        """Return the list of all shards."""
        return [self.get_shard(i) for i in range(self.num_shards())]

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

        def base_iterator(timeout=None):
            ray.get(a.par_iter_init.remote(t))
            while True:
                try:
                    yield ray.get(a.par_iter_next.remote(), timeout=timeout)
                    # Always yield after each round of gets with timeout.
                    if timeout is not None:
                        yield _NextValueNotReady()
                except TimeoutError:
                    yield _NextValueNotReady()
                except StopIteration:
                    break

        name = self.name + ".shard[{}]".format(shard_index)
        return LocalIterator(base_iterator, name=name)


class LocalIterator(Generic[T]):
    """An iterator over a single shard of data.

    It implements similar transformations as ParallelIterator[T], but the
    transforms will be applied locally and not remotely in parallel.

    This class is **serializable** and can be passed to other remote
    tasks and actors. However, it should be read from at most one process at
    a time."""

    def __init__(self,
                 base_iterator: Callable[[], Iterable[T]],
                 local_transforms: List[Callable[[Iterable], Any]] = None,
                 timeout: int = None,
                 name=None):
        """Create a local iterator (this is an internal function).

        Args:
            base_iterator (func): A function that produces the base iterator.
                This is a function so that we can ensure LocalIterator is
                serializable.
            local_transforms (list): A list of transformation functions to be
                applied on top of the base iterator. When iteration begins, we
                create the base iterator and apply these functions. This lazy
                creation ensures LocalIterator is serializable until you start
                iterating over it.
            timeout (int): Optional timeout in seconds for this iterator, after
                which _NextValueNotReady will be returned. This avoids
                blocking.
            name (str): Optional name for this iterator.
        """
        self.base_iterator = base_iterator
        self.built_iterator = None
        self.local_transforms = local_transforms or []
        self.timeout = timeout
        self.name = name or "unknown"

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
        return repr(self)

    def __repr__(self):
        return "LocalIterator[{}]".format(self.name)

    def for_each(self, fn: Callable[[T], U]) -> "LocalIterator[U]":
        def apply_foreach(it):
            for item in it:
                if isinstance(item, _NextValueNotReady):
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
                if isinstance(item, _NextValueNotReady) or fn(item):
                    yield item

        return LocalIterator(
            self.base_iterator,
            self.local_transforms + [apply_filter],
            name=self.name + ".filter()")

    def batch(self, n: int) -> "LocalIterator[List[T]]":
        def apply_batch(it):
            batch = []
            for item in it:
                if isinstance(item, _NextValueNotReady):
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
                if isinstance(item, _NextValueNotReady):
                    yield item
                else:
                    for subitem in item:
                        yield subitem

        return LocalIterator(
            self.base_iterator,
            self.local_transforms + [apply_flatten],
            name=self.name + ".flatten()")

    def shuffle(self, shuffle_buffer_size: int,
                seed: int = None) -> "LocalIterator[T]":
        """Shuffle items of this iterator

        Args:
            shuffle_buffer_size (int): The algorithm fills a buffer with
                shuffle_buffer_size elements and randomly samples elements from
                this buffer, replacing the selected elements with new elements.
                For perfect shuffling, this argument should be greater than or
                equal to the largest iterator size.
            seed (int): Seed to use for
                randomness. Default value is None.

        Returns:
            A new LocalIterator with shuffling applied
        """
        shuffle_random = random.Random(seed)

        def apply_shuffle(it):
            buffer = []
            for item in it:
                if isinstance(item, _NextValueNotReady):
                    yield item
                else:
                    buffer.append(item)
                    if len(buffer) >= shuffle_buffer_size:
                        yield buffer.pop(
                            shuffle_random.randint(0,
                                                   len(buffer) - 1))
            while len(buffer) > 0:
                yield buffer.pop(shuffle_random.randint(0, len(buffer) - 1))

        return LocalIterator(
            self.base_iterator,
            self.local_transforms + [apply_shuffle],
            name=self.name +
            ".shuffle(shuffle_buffer_size={}, seed={})".format(
                shuffle_buffer_size,
                str(seed) if seed is not None else "None"))

    def combine(self, fn: Callable[[T], List[U]]) -> "LocalIterator[U]":
        it = self.for_each(fn).flatten()
        it.name = self.name + ".combine()"
        return it

    def take(self, n: int) -> List[T]:
        """Return up to the first n items from this iterator."""
        out = []
        for item in self:
            out.append(item)
            if len(out) >= n:
                break
        return out

    def show(self, n: int = 20):
        """Print up to the first n items from this iterator."""
        i = 0
        for item in self:
            print(item)
            i += 1
            if i >= n:
                break

    def union(self, other: "LocalIterator[T]") -> "LocalIterator[T]":
        """Return an iterator that is the union of this and the other.

        There are no ordering guarantees between the two iterators. We make a
        best-effort attempt to return items from both as they become ready,
        preventing starvation of any particular iterator.
        """

        if not isinstance(other, LocalIterator):
            raise ValueError(
                "other must be of type LocalIterator, got {}".format(
                    type(other)))

        it1 = LocalIterator(
            self.base_iterator, self.local_transforms, timeout=0)
        it2 = LocalIterator(
            other.base_iterator, other.local_transforms, timeout=0)
        active = [it1, it2]

        def build_union(timeout=None):
            while True:
                for it in list(active):
                    # Yield items from the iterator until _NextValueNotReady is
                    # found, then switch to the next iterator.
                    try:
                        while True:
                            item = next(it)
                            if isinstance(item, _NextValueNotReady):
                                break
                            else:
                                yield item
                    except StopIteration:
                        active.remove(it)
                if not active:
                    break

        return LocalIterator(
            build_union, [], name="LocalUnion[{}, {}]".format(self, other))


class ParallelIteratorWorker(object):
    """Worker actor for a ParallelIterator.

    Actors that are passed to iter.from_actors() must subclass this interface.
    """

    def __init__(self, item_generator: Any, repeat: bool):
        """Create an iterator worker.

        Subclasses must call this init function.

        Args:
            item_generator (obj): A Python generator objects or lambda function
                that produces a generator when called. We allow lambda
                functions since the generator itself might not be serializable,
                but a lambda that returns it can be.
            repeat (bool): Whether to loop over the iterator forever.
        """

        def make_iterator():
            if callable(item_generator):
                return item_generator()
            else:
                return item_generator

        if repeat:

            def cycle():
                while True:
                    it = make_iterator()
                    for item in it:
                        yield item

            self.item_generator = cycle()
        else:
            self.item_generator = make_iterator()

        self.transforms = []
        self.local_it = None

    def par_iter_init(self, transforms):
        """Implements ParallelIterator worker init."""
        it = LocalIterator(lambda timeout: self.item_generator)
        for fn in transforms:
            it = fn(it)
            assert it is not None, fn
        self.local_it = iter(it)

    def par_iter_next(self):
        """Implements ParallelIterator worker item fetch."""
        assert self.local_it is not None, "must call par_iter_init()"
        return next(self.local_it)


class _NextValueNotReady(Exception):
    """Indicates that a local iterator has no value currently available.

    This is used internally to implement the union() of multiple blocking
    local generators."""
    pass


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
