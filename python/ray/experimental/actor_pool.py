import ray
import time


class ActorPool(object):
    def __init__(self, actors):
        """Create an Actor pool from a list of existing actors.

        An actor pool is a utility class similar to multiprocessing.Pool that
        lets you schedule Ray tasks over a fixed pool of actors.

        Arguments:
            actors (list): List of Ray actor handles to use in this pool.

        Examples:
            >>> a1, a2 = Actor.remote(), Actor.remote()
            >>> pool = ActorPool([a1, a2])
            >>> print(pool.map(lambda a, v: a.double.remote(v), [1, 2, 3, 4]))
            [2, 4, 6, 8]
        """
        self._idle_actors = list(actors)
        self._future_to_actor = {}
        self._index_to_future = {}
        self._next_task_index = 0
        self._next_return_index = 0
        self._pending_submits = []

    def map(self, fn, values):
        """Apply the given function in parallel over the actors and values.

        This returns an ordered iterator that will return results of the map
        as they finish. Note that you must iterate over the iterator to force
        the computation to finish.

        Arguments:
            fn (func): Function that takes (actor, value) as argument and
                returns an ObjectID computing the result over the value. The
                actor will be considered busy until the ObjectID completes.
            values (list): List of values that fn(actor, value) should be
                applied to.

        Returns:
            Iterator over results from applying fn to the actors and values.

        Examples:
            >>> pool = ActorPool(...)
            >>> print(pool.map(lambda a, v: a.double.remote(v), [1, 2, 3, 4]))
            [2, 4, 6, 8]
        """
        for v in values:
            self.submit(lambda a, v: fn(a, v), v)
        while self.has_next():
            yield self.get_next()

    def map_unordered(self, fn, values):
        """Similar to map(), but returning an unordered iterator.

        This returns an unordered iterator that will return results of the map
        as they finish. This can be more efficient that map() if some results
        take longer to compute than others.

        Arguments:
            fn (func): Function that takes (actor, value) as argument and
                returns an ObjectID computing the result over the value. The
                actor will be considered busy until the ObjectID completes.
            values (list): List of values that fn(actor, value) should be
                applied to.

        Returns:
            Iterator over results from applying fn to the actors and values.

        Examples:
            >>> pool = ActorPool(...)
            >>> print(pool.map(lambda a, v: a.double.remote(v), [1, 2, 3, 4]))
            [6, 2, 4, 8]
        """
        for v in values:
            self.submit(lambda a, v: fn(a, v), v)
        while self.has_next():
            yield self.get_next_unordered()

    def submit(self, fn, value):
        """Schedule a single task to run in the pool.

        This has the same argument semantics as map(), but takes on a single
        value instead of a list of values. The result can be retrieved using
        get_next() / get_next_unordered().

        Arguments:
            fn (func): Function that takes (actor, value) as argument and
                returns an ObjectID computing the result over the value. The
                actor will be considered busy until the ObjectID completes.
            value (object): Value to compute a result for.

        Examples:
            >>> pool = ActorPool(...)
            >>> pool.submit(lambda a, v: a.double.remote(v), 1)
            >>> pool.submit(lambda a, v: a.double.remote(v), 2)
            >>> print(pool.get_next(), pool.get_next())
            2, 4
        """
        if self._idle_actors:
            actor = self._idle_actors.pop()
            future = fn(actor, value)
            self._future_to_actor[future] = (self._next_task_index, actor)
            self._index_to_future[self._next_task_index] = future
            self._next_task_index += 1
        else:
            self._pending_submits.append((fn, value))

    def has_next(self):
        """Returns whether there are any pending results to return.

        Returns:
            True if there are any pending results not yet returned.

        Examples:
            >>> pool = ActorPool(...)
            >>> pool.submit(lambda a, v: a.double.remote(v), 1)
            >>> print(pool.has_next())
            True
            >>> print(pool.get_next())
            2
            >>> print(pool.has_next())
            False
        """
        return bool(self._future_to_actor)

    def get_next(self, timeout=None):
        """Returns the next pending result in order.

        This returns the next result produced by submit(), blocking for up to
        the specified timeout until it is available.

        Returns:
            The next result.

        Raises:
            TimeoutError if the timeout is reached.

        Examples:
            >>> pool = ActorPool(...)
            >>> pool.submit(lambda a, v: a.double.remote(v), 1)
            >>> print(pool.get_next())
            2
        """
        if not self.has_next():
            raise StopIteration("No more results to get")
        if self._next_return_index >= self._next_task_index:
            raise ValueError("It is not allowed to call get_next() after "
                             "get_next_unordered().")
        future = self._index_to_future.pop(self._next_return_index)
        self._next_return_index += 1
        i, a = self._future_to_actor.pop(future)
        self._return_actor(a)
        if timeout is not None:
            res, _ = ray.wait([future], timeout=timeout)
            if not res:
                raise TimeoutError("Timed out waiting for result")
        return ray.get(future)

    def get_next_unordered(self, timeout=None):
        """Returns any of the next pending results.

        This returns some result produced by submit(), blocking for up to
        the specified timeout until it is available. Unlike get_next(), the
        results are not always returned in same order as submitted, which can
        improve performance.

        Returns:
            The next result.

        Raises:
            TimeoutError if the timeout is reached.

        Examples:
            >>> pool = ActorPool(...)
            >>> pool.submit(lambda a, v: a.double.remote(v), 1)
            >>> pool.submit(lambda a, v: a.double.remote(v), 2)
            >>> print(pool.get_next_unordered())
            4
            >>> print(pool.get_next_unordered())
            2
        """
        if not self.has_next():
            raise StopIteration("No more results to get")
        # TODO(ekl) bulk wait for performance
        res, _ = ray.wait(
            list(self._future_to_actor), num_returns=1, timeout=timeout)
        if res:
            [future] = res
        else:
            raise TimeoutError("Timed out waiting for result")
        i, a = self._future_to_actor.pop(future)
        self._return_actor(a)
        del self._index_to_future[i]
        self._next_return_index = max(self._next_return_index, i + 1)
        return ray.get(future)

    def _return_actor(self, actor):
        self._idle_actors.append(actor)
        if self._pending_submits:
            self.submit(*self._pending_submits.pop(0))


if __name__ == "__main__":

    @ray.remote
    class MyActor(object):
        def __init__(self):
            pass

        def f(self, x):
            time.sleep(0.1)
            return x + 1

    ray.init()
    actors = [MyActor.remote() for _ in range(4)]
    pool = ActorPool(actors)

    print("-- testing get_next --")
    for i in range(5):
        pool.submit(lambda a, v: a.f.remote(v), i)
        print(pool.get_next())

    print("-- testing get_next_unordered --")
    for i in range(5):
        pool.submit(lambda a, v: a.f.remote(v), i)
    while pool.has_next():
        print(pool.get_next_unordered())

    print("-- testing map --")
    for v in pool.map(lambda a, v: a.f.remote(v), range(5)):
        print(v)

    print("-- testing map unordered --")
    for v in pool.map_unordered(lambda a, v: a.f.remote(v), range(5)):
        print(v)
