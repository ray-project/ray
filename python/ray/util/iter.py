from contextlib import contextmanager
import collections
import random
import threading
from typing import TypeVar, Generic, Iterable, List, Callable, Any

import ray
from ray.util.iter_metrics import MetricsContext, SharedMetrics

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
    return from_iterators(
        generators,
        repeat=repeat,
        name=name,
    )


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
    return ParallelIterator([_ActorSet(actors, [])], name, parent_iterators=[])


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

    def __init__(self, actor_sets: List["_ActorSet"], name: str,
                 parent_iterators: List["ParallelIterator[Any]"]):
        """Create a parallel iterator (this is an internal function)."""

        # We track multiple sets of actors to support parallel .union().
        self.actor_sets = actor_sets
        self.name = name

        # keep explicit reference to parent iterator for repartition
        self.parent_iterators = parent_iterators

    def __iter__(self):
        raise TypeError(
            "You must use it.gather_sync() or it.gather_async() to "
            "iterate over the results of a ParallelIterator.")

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "ParallelIterator[{}]".format(self.name)

    def _with_transform(self, local_it_fn, name):
        """Helper function to create new Parallel Iterator"""
        return ParallelIterator(
            [a.with_transform(local_it_fn) for a in self.actor_sets],
            name=self.name + name,
            parent_iterators=self.parent_iterators)

    def for_each(self, fn: Callable[[T], U]) -> "ParallelIterator[U]":
        """Remotely apply fn to each item in this iterator.

        Args:
            fn (func): function to apply to each item.

        Examples:
            >>> next(from_range(4).for_each(lambda x: x * 2).gather_sync())
            ... [0, 2, 4, 8]
        """
        return self._with_transform(lambda local_it: local_it.for_each(fn),
                                    ".for_each()")

    def filter(self, fn: Callable[[T], bool]) -> "ParallelIterator[T]":
        """Remotely filter items from this iterator.

        Args:
            fn (func): returns False for items to drop from the iterator.

        Examples:
            >>> it = from_items([0, 1, 2]).filter(lambda x: x > 0)
            >>> next(it.gather_sync())
            ... [1, 2]
        """
        return self._with_transform(lambda local_it: local_it.filter(fn),
                                    ".filter()")

    def batch(self, n: int) -> "ParallelIterator[List[T]]":
        """Remotely batch together items in this iterator.

        Args:
            n (int): Number of items to batch together.

        Examples:
            >>> next(from_range(10, 1).batch(4).gather_sync())
            ... [0, 1, 2, 3]
        """
        return self._with_transform(lambda local_it: local_it.batch(n),
                                    ".batch({})".format(n))

    def flatten(self) -> "ParallelIterator[T[0]]":
        """Flatten batches of items into individual items.

        Examples:
            >>> next(from_range(10, 1).batch(4).flatten())
            ... 0
        """
        return self._with_transform(lambda local_it: local_it.flatten(),
                                    ".flatten()")

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
            A ParallelIterator with a local shuffle applied on the base
            iterator

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
        return self._with_transform(
            lambda local_it: local_it.shuffle(shuffle_buffer_size, seed),
            ".local_shuffle(shuffle_buffer_size={}, seed={})".format(
                shuffle_buffer_size,
                str(seed) if seed is not None else "None"))

    def repartition(self, num_partitions: int) -> "ParallelIterator[T]":
        """Returns a new ParallelIterator instance with num_partitions shards.

        The new iterator contains the same data in this instance except with
        num_partitions shards. The data is split in round-robin fashion for
        the new ParallelIterator.

        Args:
            num_partitions (int): The number of shards to use for the new
                ParallelIterator

        Returns:
            A ParallelIterator with num_partitions number of shards and the
            data of this ParallelIterator split round-robin among the new
            number of shards.

        Examples:
            >>> it = from_range(8, 2)
            >>> it = it.repartition(3)
            >>> list(it.get_shard(0))
            [0, 4, 3, 7]
            >>> list(it.get_shard(1))
            [1, 5]
            >>> list(it.get_shard(2))
            [2, 6]
        """

        # initialize the local iterators for all the actors
        all_actors = []
        for actor_set in self.actor_sets:
            actor_set.init_actors()
            all_actors.extend(actor_set.actors)

        def base_iterator(num_partitions, partition_index, timeout=None):
            futures = {}
            for a in all_actors:
                futures[a.par_iter_slice.remote(
                    step=num_partitions, start=partition_index)] = a
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
                        futures[actor.par_iter_slice.remote(
                            step=num_partitions,
                            start=partition_index)] = actor
                    except StopIteration:
                        pass
                # Always yield after each round of wait with timeout.
                if timeout is not None:
                    yield _NextValueNotReady()

        def make_gen_i(i):
            return lambda: base_iterator(num_partitions, i)

        name = self.name + ".repartition[num_partitions={}]".format(
            num_partitions)

        generators = [make_gen_i(s) for s in range(num_partitions)]
        worker_cls = ray.remote(ParallelIteratorWorker)
        actors = [worker_cls.remote(g, repeat=False) for g in generators]
        # need explicit reference to self so actors in this instance do not die
        return ParallelIterator(
            [_ActorSet(actors, [])], name, parent_iterators=[self])

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
        return LocalIterator(base_iterator, SharedMetrics(), name=name)

    def gather_async(self, async_queue_depth=1) -> "LocalIterator[T]":
        """Returns a local iterable for asynchronous iteration.

        New items will be fetched from the shards asynchronously as soon as
        the previous one is computed. Items arrive in non-deterministic order.

        Arguments:
            async_queue_depth (int): The max number of async requests in flight
                per actor. Increasing this improves the amount of pipeline
                parallelism in the iterator.

        Examples:
            >>> it = from_range(100, 1).gather_async()
            >>> next(it)
            ... 3
            >>> next(it)
            ... 0
            >>> next(it)
            ... 1
        """

        if async_queue_depth < 1:
            raise ValueError("queue depth must be positive")

        # Forward reference to the returned iterator.
        local_iter = None

        def base_iterator(timeout=None):
            all_actors = []
            for actor_set in self.actor_sets:
                actor_set.init_actors()
                all_actors.extend(actor_set.actors)
            futures = {}
            for _ in range(async_queue_depth):
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
                        local_iter.shared_metrics.get().current_actor = actor
                        yield ray.get(obj_id)
                        futures[actor.par_iter_next.remote()] = actor
                    except StopIteration:
                        pass
                # Always yield after each round of wait with timeout.
                if timeout is not None:
                    yield _NextValueNotReady()

        name = "{}.gather_async()".format(self)
        local_iter = LocalIterator(base_iterator, SharedMetrics(), name=name)
        return local_iter

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
        # if one of these iterators is a result of a repartition, we need to
        # keep an explicit reference to its parent iterator
        return ParallelIterator(
            actor_sets,
            "ParallelUnion[{}, {}]".format(self, other),
            parent_iterators=self.parent_iterators + other.parent_iterators)

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
        return LocalIterator(base_iterator, SharedMetrics(), name=name)


class LocalIterator(Generic[T]):
    """An iterator over a single shard of data.

    It implements similar transformations as ParallelIterator[T], but the
    transforms will be applied locally and not remotely in parallel.

    This class is **serializable** and can be passed to other remote
    tasks and actors. However, it should be read from at most one process at
    a time."""

    # If a function passed to LocalIterator.for_each() has this method,
    # we will call it at the beginning of each data fetch call. This can be
    # used to measure the underlying wait latency for measurement purposes.
    ON_FETCH_START_HOOK_NAME = "_on_fetch_start"

    thread_local = threading.local()

    def __init__(self,
                 base_iterator: Callable[[], Iterable[T]],
                 shared_metrics: SharedMetrics,
                 local_transforms: List[Callable[[Iterable], Any]] = None,
                 timeout: int = None,
                 name=None):
        """Create a local iterator (this is an internal function).

        Args:
            base_iterator (func): A function that produces the base iterator.
                This is a function so that we can ensure LocalIterator is
                serializable.
            shared_metrics (SharedMetrics): Existing metrics context or a new
                context. Should be the same for each chained iterator.
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
        assert isinstance(shared_metrics, SharedMetrics)
        self.base_iterator = base_iterator
        self.built_iterator = None
        self.local_transforms = local_transforms or []
        self.shared_metrics = shared_metrics
        self.timeout = timeout
        self.name = name or "unknown"

    @staticmethod
    def get_metrics() -> MetricsContext:
        """Return the current metrics context.

        This can only be called within an iterator function."""
        if (not hasattr(LocalIterator.thread_local, "metrics")
                or LocalIterator.thread_local.metrics is None):
            raise ValueError("Cannot access context outside an iterator.")
        return LocalIterator.thread_local.metrics

    def _build_once(self):
        if self.built_iterator is None:
            it = iter(self.base_iterator(self.timeout))
            for fn in self.local_transforms:
                it = fn(it)
            self.built_iterator = it

    @contextmanager
    def _metrics_context(self):
        if hasattr(self.thread_local, "metrics"):
            prev_metrics = self.thread_local.metrics
        else:
            prev_metrics = None
        try:
            self.thread_local.metrics = self.shared_metrics.get()
            yield
        finally:
            self.thread_local.metrics = prev_metrics

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
                    # Keep retrying the function until it returns a valid
                    # value. This allows for non-blocking functions.
                    while True:
                        with self._metrics_context():
                            result = fn(item)
                        yield result
                        if not isinstance(result, _NextValueNotReady):
                            break

        if hasattr(fn, LocalIterator.ON_FETCH_START_HOOK_NAME):
            unwrapped = apply_foreach

            def add_wait_hooks(it):
                it = unwrapped(it)
                new_item = True
                while True:
                    # Avoids calling on_fetch_start repeatedly if we are
                    # yielding _NextValueNotReady.
                    if new_item:
                        with self._metrics_context():
                            fn._on_fetch_start()
                        new_item = False
                    item = next(it)
                    if not isinstance(item, _NextValueNotReady):
                        new_item = True
                    yield item

            apply_foreach = add_wait_hooks

        return LocalIterator(
            self.base_iterator,
            self.shared_metrics,
            self.local_transforms + [apply_foreach],
            name=self.name + ".for_each()")

    def filter(self, fn: Callable[[T], bool]) -> "LocalIterator[T]":
        def apply_filter(it):
            for item in it:
                with self._metrics_context():
                    if isinstance(item, _NextValueNotReady) or fn(item):
                        yield item

        return LocalIterator(
            self.base_iterator,
            self.shared_metrics,
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
            self.shared_metrics,
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
            self.shared_metrics,
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
            self.shared_metrics,
            self.local_transforms + [apply_shuffle],
            name=self.name +
            ".shuffle(shuffle_buffer_size={}, seed={})".format(
                shuffle_buffer_size,
                str(seed) if seed is not None else "None"))

    def combine(self, fn: Callable[[T], List[U]]) -> "LocalIterator[U]":
        it = self.for_each(fn).flatten()
        it.name = self.name + ".combine()"
        return it

    def zip_with_source_actor(self):
        def zip_with_source(item):
            metrics = LocalIterator.get_metrics()
            if metrics.current_actor is None:
                raise ValueError("Could not identify source actor of item")
            return metrics.current_actor, item

        it = self.for_each(zip_with_source)
        it.name = self.name + ".zip_with_source_actor()"
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

    def duplicate(self, n) -> List["LocalIterator[T]"]:
        """Copy this iterator `n` times, duplicating the data.

        Returns:
            List[LocalIterator[T]]: multiple iterators that each have a copy
                of the data of this iterator.
        """

        if n < 2:
            raise ValueError("Number of copies must be >= 2")

        queues = []
        for _ in range(n):
            queues.append(collections.deque())

        def fill_next(timeout):
            self.timeout = timeout
            item = next(self)
            for q in queues:
                q.append(item)

        def make_next(i):
            def gen(timeout):
                while True:
                    if len(queues[i]) == 0:
                        fill_next(timeout)
                    yield queues[i].popleft()

            return gen

        iterators = []
        for i in range(n):
            iterators.append(
                LocalIterator(
                    make_next(i),
                    self.shared_metrics, [],
                    name=self.name + ".duplicate[{}]".format(i)))

        return iterators

    def union(self, *others: "LocalIterator[T]",
              deterministic: bool = False) -> "LocalIterator[T]":
        """Return an iterator that is the union of this and the others.

        If deterministic=True, we alternate between reading from one iterator
        and the others. Otherwise we return items from iterators as they
        become ready.
        """

        for it in others:
            if not isinstance(it, LocalIterator):
                raise ValueError(
                    "other must be of type LocalIterator, got {}".format(
                        type(it)))

        timeout = None if deterministic else 0

        active = []
        parent_iters = [self] + list(others)
        shared_metrics = SharedMetrics(
            parents=[p.shared_metrics for p in parent_iters])
        for it in parent_iters:
            active.append(
                LocalIterator(
                    it.base_iterator,
                    shared_metrics,
                    it.local_transforms,
                    timeout=timeout))

        def build_union(timeout=None):
            while True:
                for it in list(active):
                    # Yield items from the iterator until _NextValueNotReady is
                    # found, then switch to the next iterator.
                    # To avoid starvation, we yield at most max_yield items per
                    # iterator before switching.
                    if deterministic:
                        max_yield = 1  # Forces round robin.
                    else:
                        max_yield = 20
                    try:
                        for _ in range(max_yield):
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
            build_union,
            shared_metrics, [],
            name="LocalUnion[{}, {}]".format(self, ", ".join(map(str,
                                                                 others))))


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
        self.next_ith_buffer = None

    def par_iter_init(self, transforms):
        """Implements ParallelIterator worker init."""
        it = LocalIterator(lambda timeout: self.item_generator,
                           SharedMetrics())
        for fn in transforms:
            it = fn(it)
            assert it is not None, fn
        self.local_it = iter(it)

    def par_iter_next(self):
        """Implements ParallelIterator worker item fetch."""
        assert self.local_it is not None, "must call par_iter_init()"
        return next(self.local_it)

    def par_iter_slice(self, step: int, start: int):
        """Iterates in increments of step starting from start."""
        assert self.local_it is not None, "must call par_iter_init()"

        if self.next_ith_buffer is None:
            self.next_ith_buffer = collections.defaultdict(list)

        index_buffer = self.next_ith_buffer[start]
        if len(index_buffer) > 0:
            return index_buffer.pop(0)
        else:
            for j in range(step):
                try:
                    val = next(self.local_it)
                    self.next_ith_buffer[j].append(val)
                except StopIteration:
                    pass

            if not self.next_ith_buffer[start]:
                raise StopIteration

        return self.next_ith_buffer[start].pop(0)


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
