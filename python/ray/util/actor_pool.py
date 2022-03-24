import ray
from ray.util.annotations import PublicAPI


@PublicAPI(stability="beta")
class ActorPool:
    """Utility class to operate on a fixed pool of actors.

    Arguments:
        actors (list): List of Ray actor handles to use in this pool.

    Examples:
        >>> import ray
        >>> from ray.util.actor_pool import ActorPool
        >>> @ray.remote # doctest: +SKIP
        >>> class Actor: # doctest: +SKIP
        ...     ... # doctest: +SKIP
        >>> a1, a2 = Actor.remote(), Actor.remote() # doctest: +SKIP
        >>> pool = ActorPool([a1, a2]) # doctest: +SKIP
        >>> print(list(pool.map(lambda a, v: a.double.remote(v), # doctest: +SKIP
        ...                     [1, 2, 3, 4]))) # doctest: +SKIP
        [2, 4, 6, 8]
    """

    def __init__(self, actors):
        # actors to be used
        self._idle_actors = list(actors)

        # get actor from future
        self._future_to_actor = {}

        # get future from index
        self._index_to_future = {}

        # next task to do
        self._next_task_index = 0

        # next task to return
        self._next_return_index = 0

        # next work depending when actors free
        self._pending_submits = []

    def map(self, fn, values):
        """Apply the given function in parallel over the actors and values.

        This returns an ordered iterator that will return results of the map
        as they finish. Note that you must iterate over the iterator to force
        the computation to finish.

        Arguments:
            fn (func): Function that takes (actor, value) as argument and
                returns an ObjectRef computing the result over the value. The
                actor will be considered busy until the ObjectRef completes.
            values (list): List of values that fn(actor, value) should be
                applied to.

        Returns:
            Iterator over results from applying fn to the actors and values.

        Examples:
            >>> from ray.util.actor_pool import ActorPool
            >>> pool = ActorPool(...) # doctest: +SKIP
            >>> print(list(pool.map(lambda a, v: a.double.remote(v),
            ...                     [1, 2, 3, 4]))) # doctest: +SKIP
            [2, 4, 6, 8]
        """
        # Ignore/Cancel all the previous submissions
        # by calling `has_next` and `gen_next` repeteadly.
        while self.has_next():
            try:
                self.get_next(timeout=0)
            except TimeoutError:
                pass

        for v in values:
            self.submit(fn, v)
        while self.has_next():
            yield self.get_next()

    def map_unordered(self, fn, values):
        """Similar to map(), but returning an unordered iterator.

        This returns an unordered iterator that will return results of the map
        as they finish. This can be more efficient that map() if some results
        take longer to compute than others.

        Arguments:
            fn (func): Function that takes (actor, value) as argument and
                returns an ObjectRef computing the result over the value. The
                actor will be considered busy until the ObjectRef completes.
            values (list): List of values that fn(actor, value) should be
                applied to.

        Returns:
            Iterator over results from applying fn to the actors and values.

        Examples:
            >>> from ray.util.actor_pool import ActorPool
            >>> pool = ActorPool(...) # doctest: +SKIP
            >>> print(list(pool.map_unordered(lambda a, v: a.double.remote(v),
            ...                               [1, 2, 3, 4]))) # doctest: +SKIP
            [6, 2, 4, 8]
        """
        # Ignore/Cancel all the previous submissions
        # by calling `has_next` and `gen_next_unordered` repeteadly.
        while self.has_next():
            try:
                self.get_next_unordered(timeout=0)
            except TimeoutError:
                pass

        for v in values:
            self.submit(fn, v)
        while self.has_next():
            yield self.get_next_unordered()

    def submit(self, fn, value):
        """Schedule a single task to run in the pool.

        This has the same argument semantics as map(), but takes on a single
        value instead of a list of values. The result can be retrieved using
        get_next() / get_next_unordered().

        Arguments:
            fn (func): Function that takes (actor, value) as argument and
                returns an ObjectRef computing the result over the value. The
                actor will be considered busy until the ObjectRef completes.
            value (object): Value to compute a result for.

        Examples:
            >>> from ray.util.actor_pool import ActorPool
            >>> pool = ActorPool(...) # doctest: +SKIP
            >>> pool.submit(lambda a, v: a.double.remote(v), 1) # doctest: +SKIP
            >>> pool.submit(lambda a, v: a.double.remote(v), 2) # doctest: +SKIP
            >>> print(pool.get_next(), pool.get_next()) # doctest: +SKIP
            2, 4
        """
        if self._idle_actors:
            actor = self._idle_actors.pop()
            future = fn(actor, value)
            future_key = tuple(future) if isinstance(future, list) else future
            self._future_to_actor[future_key] = (self._next_task_index, actor)
            self._index_to_future[self._next_task_index] = future
            self._next_task_index += 1
        else:
            self._pending_submits.append((fn, value))

    def has_next(self):
        """Returns whether there are any pending results to return.

        Returns:
            True if there are any pending results not yet returned.

        Examples:
            >>> from ray.util.actor_pool import ActorPool
            >>> pool = ActorPool(...) # doctest: +SKIP
            >>> pool.submit(lambda a, v: a.double.remote(v), 1) # doctest: +SKIP
            >>> print(pool.has_next()) # doctest: +SKIP
            True
            >>> print(pool.get_next()) # doctest: +SKIP
            2
            >>> print(pool.has_next()) # doctest: +SKIP
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
            >>> from ray.util.actor_pool import ActorPool
            >>> pool = ActorPool(...) # doctest: +SKIP
            >>> pool.submit(lambda a, v: a.double.remote(v), 1) # doctest: +SKIP
            >>> print(pool.get_next()) # doctest: +SKIP
            2
        """
        if not self.has_next():
            raise StopIteration("No more results to get")
        if self._next_return_index >= self._next_task_index:
            raise ValueError(
                "It is not allowed to call get_next() after get_next_unordered()."
            )
        future = self._index_to_future[self._next_return_index]
        if timeout is not None:
            res, _ = ray.wait([future], timeout=timeout)
            if not res:
                raise TimeoutError("Timed out waiting for result")
        del self._index_to_future[self._next_return_index]
        self._next_return_index += 1

        future_key = tuple(future) if isinstance(future, list) else future
        i, a = self._future_to_actor.pop(future_key)

        self._return_actor(a)
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
            >>> from ray.util.actor_pool import ActorPool
            >>> pool = ActorPool(...) # doctest: +SKIP
            >>> pool.submit(lambda a, v: a.double.remote(v), 1) # doctest: +SKIP
            >>> pool.submit(lambda a, v: a.double.remote(v), 2) # doctest: +SKIP
            >>> print(pool.get_next_unordered()) # doctest: +SKIP
            4
            >>> print(pool.get_next_unordered()) # doctest: +SKIP
            2
        """
        if not self.has_next():
            raise StopIteration("No more results to get")
        # TODO(ekl) bulk wait for performance
        res, _ = ray.wait(list(self._future_to_actor), num_returns=1, timeout=timeout)
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

    def has_free(self):
        """Returns whether there are any idle actors available.

        Returns:
            True if there are any idle actors and no pending submits.

        Examples:
            >>> @ray.remote # doctest: +SKIP
            >>> class Actor: # doctest: +SKIP
            ...     ... # doctest: +SKIP
            >>> a1 = Actor.remote() # doctest: +SKIP
            >>> pool = ActorPool(a1) # doctest: +SKIP
            >>> pool.submit(lambda a, v: a.double.remote(v), 1) # doctest: +SKIP
            >>> print(pool.has_free()) # doctest: +SKIP
            False
            >>> print(pool.get_next()) # doctest: +SKIP
            2
            >>> print(pool.has_free()) # doctest: +SKIP
            True
        """
        return len(self._idle_actors) > 0 and len(self._pending_submits) == 0

    def pop_idle(self):
        """Removes an idle actor from the pool.

        Returns:
            An idle actor if one is available.
            None if no actor was free to be removed.

        Examples:
            >>> @ray.remote # doctest: +SKIP
            >>> class Actor: # doctest: +SKIP
            ...     ... # doctest: +SKIP
            >>> a1 = Actor.remote() # doctest: +SKIP
            >>> pool = ActorPool([a1]) # doctest: +SKIP
            >>> pool.submit(lambda a, v: a.double.remote(v), 1) # doctest: +SKIP
            >>> print(pool.pop_idle()) # doctest: +SKIP
            None
            >>> print(pool.get_next()) # doctest: +SKIP
            2
            >>> print(pool.pop_idle()) # doctest: +SKIP
            <ptr to a1>
        """
        if self.has_free():
            return self._idle_actors.pop()
        return None

    def push(self, actor):
        """Pushes a new actor into the current list of idle actors.

        Examples:
            >>> @ray.remote # doctest: +SKIP
            >>> class Actor: # doctest: +SKIP
            ...     ... # doctest: +SKIP
            >>> a1, b1 = Actor.remote(), Actor.remote() # doctest: +SKIP
            >>> pool = ActorPool([a1]) # doctest: +SKIP
            >>> pool.submit(lambda a, v: a.double.remote(v), 1) # doctest: +SKIP
            >>> print(pool.get_next()) # doctest: +SKIP
            2
            >>> pool2 = ActorPool([b1]) # doctest: +SKIP
            >>> pool2.push(pool.pop_idle()) # doctest: +SKIP
        """
        busy_actors = []
        if self._future_to_actor.values():
            _, busy_actors = zip(*self._future_to_actor.values())
        if actor in self._idle_actors or actor in busy_actors:
            raise ValueError("Actor already belongs to current ActorPool")
        else:
            self._idle_actors.append(actor)
