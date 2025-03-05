from typing import TYPE_CHECKING, Any, Callable, List, TypeVar

import ray
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import ray.actor

V = TypeVar("V")


@DeveloperAPI
class ActorPool:
    """Utility class to operate on a fixed pool of actors.

    Arguments:
        actors: List of Ray actor handles to use in this pool.

    Examples:
        .. testcode::

            import ray
            from ray.util.actor_pool import ActorPool

            @ray.remote
            class Actor:
                def double(self, v):
                    return 2 * v

            a1, a2 = Actor.remote(), Actor.remote()
            pool = ActorPool([a1, a2])
            print(list(pool.map(lambda a, v: a.double.remote(v),
                                [1, 2, 3, 4])))

        .. testoutput::

            [2, 4, 6, 8]
    """

    def __init__(self, actors: list):
        from ray._private.usage.usage_lib import record_library_usage

        record_library_usage("util.ActorPool")

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

    def map(self, fn: Callable[["ray.actor.ActorHandle", V], Any], values: List[V]):
        """Apply the given function in parallel over the actors and values.

        This returns an ordered iterator that will return results of the map
        as they finish. Note that you must iterate over the iterator to force
        the computation to finish.

        Arguments:
            fn: Function that takes (actor, value) as argument and
                returns an ObjectRef computing the result over the value. The
                actor will be considered busy until the ObjectRef completes.
            values: List of values that fn(actor, value) should be
                applied to.

        Returns:
            Iterator over results from applying fn to the actors and values.

        Examples:
            .. testcode::

                import ray
                from ray.util.actor_pool import ActorPool

                @ray.remote
                class Actor:
                    def double(self, v):
                        return 2 * v

                a1, a2 = Actor.remote(), Actor.remote()
                pool = ActorPool([a1, a2])
                print(list(pool.map(lambda a, v: a.double.remote(v),
                                    [1, 2, 3, 4])))

            .. testoutput::

                [2, 4, 6, 8]
        """
        # Ignore/Cancel all the previous submissions
        # by calling `has_next` and `gen_next` repeteadly.
        while self.has_next():
            try:
                self.get_next(timeout=0, ignore_if_timedout=True)
            except TimeoutError:
                pass

        for v in values:
            self.submit(fn, v)

        def get_generator():
            while self.has_next():
                yield self.get_next()

        return get_generator()

    def map_unordered(
        self, fn: Callable[["ray.actor.ActorHandle", V], Any], values: List[V]
    ):
        """Similar to map(), but returning an unordered iterator.

        This returns an unordered iterator that will return results of the map
        as they finish. This can be more efficient that map() if some results
        take longer to compute than others.

        Arguments:
            fn: Function that takes (actor, value) as argument and
                returns an ObjectRef computing the result over the value. The
                actor will be considered busy until the ObjectRef completes.
            values: List of values that fn(actor, value) should be
                applied to.

        Returns:
            Iterator over results from applying fn to the actors and values.

        Examples:
            .. testcode::

                import ray
                from ray.util.actor_pool import ActorPool

                @ray.remote
                class Actor:
                    def double(self, v):
                        return 2 * v

                a1, a2 = Actor.remote(), Actor.remote()
                pool = ActorPool([a1, a2])
                print(list(pool.map_unordered(lambda a, v: a.double.remote(v),
                                              [1, 2, 3, 4])))

            .. testoutput::
                :options: +MOCK

                [6, 8, 4, 2]
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

        def get_generator():
            while self.has_next():
                yield self.get_next_unordered()

        return get_generator()

    def submit(self, fn, value):
        """Schedule a single task to run in the pool.

        This has the same argument semantics as map(), but takes on a single
        value instead of a list of values. The result can be retrieved using
        get_next() / get_next_unordered().

        Arguments:
            fn: Function that takes (actor, value) as argument and
                returns an ObjectRef computing the result over the value. The
                actor will be considered busy until the ObjectRef completes.
            value: Value to compute a result for.

        Examples:
            .. testcode::

                import ray
                from ray.util.actor_pool import ActorPool

                @ray.remote
                class Actor:
                    def double(self, v):
                        return 2 * v

                a1, a2 = Actor.remote(), Actor.remote()
                pool = ActorPool([a1, a2])
                pool.submit(lambda a, v: a.double.remote(v), 1)
                pool.submit(lambda a, v: a.double.remote(v), 2)
                print(pool.get_next(), pool.get_next())

            .. testoutput::

                2 4
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
            .. testcode::

                import ray
                from ray.util.actor_pool import ActorPool

                @ray.remote
                class Actor:
                    def double(self, v):
                        return 2 * v

                a1, a2 = Actor.remote(), Actor.remote()
                pool = ActorPool([a1, a2])
                pool.submit(lambda a, v: a.double.remote(v), 1)
                print(pool.has_next())
                print(pool.get_next())
                print(pool.has_next())

            .. testoutput::

                True
                2
                False
        """
        return bool(self._future_to_actor)

    def get_next(self, timeout=None, ignore_if_timedout=False):
        """Returns the next pending result in order.

        This returns the next result produced by submit(), blocking for up to
        the specified timeout until it is available.

        Returns:
            The next result.

        Raises:
            TimeoutError: if the timeout is reached.

        Examples:
            .. testcode::

                import ray
                from ray.util.actor_pool import ActorPool

                @ray.remote
                class Actor:
                    def double(self, v):
                        return 2 * v

                a1, a2 = Actor.remote(), Actor.remote()
                pool = ActorPool([a1, a2])
                pool.submit(lambda a, v: a.double.remote(v), 1)
                print(pool.get_next())

            .. testoutput::

                2
        """
        if not self.has_next():
            raise StopIteration("No more results to get")
        if self._next_return_index >= self._next_task_index:
            raise ValueError(
                "It is not allowed to call get_next() after get_next_unordered()."
            )
        future = self._index_to_future[self._next_return_index]
        timeout_msg = "Timed out waiting for result"
        raise_timeout_after_ignore = False
        if timeout is not None:
            res, _ = ray.wait([future], timeout=timeout)
            if not res:
                if not ignore_if_timedout:
                    raise TimeoutError(timeout_msg)
                else:
                    raise_timeout_after_ignore = True
        del self._index_to_future[self._next_return_index]
        self._next_return_index += 1

        future_key = tuple(future) if isinstance(future, list) else future
        i, a = self._future_to_actor.pop(future_key)

        self._return_actor(a)
        if raise_timeout_after_ignore:
            raise TimeoutError(
                timeout_msg + ". The task {} has been ignored.".format(future)
            )
        return ray.get(future)

    def get_next_unordered(self, timeout=None, ignore_if_timedout=False):
        """Returns any of the next pending results.

        This returns some result produced by submit(), blocking for up to
        the specified timeout until it is available. Unlike get_next(), the
        results are not always returned in same order as submitted, which can
        improve performance.

        Returns:
            The next result.

        Raises:
            TimeoutError: if the timeout is reached.

        Examples:
            .. testcode::

                import ray
                from ray.util.actor_pool import ActorPool

                @ray.remote
                class Actor:
                    def double(self, v):
                        return 2 * v

                a1, a2 = Actor.remote(), Actor.remote()
                pool = ActorPool([a1, a2])
                pool.submit(lambda a, v: a.double.remote(v), 1)
                pool.submit(lambda a, v: a.double.remote(v), 2)
                print(pool.get_next_unordered())
                print(pool.get_next_unordered())

            .. testoutput::
                :options: +MOCK

                4
                2
        """
        if not self.has_next():
            raise StopIteration("No more results to get")
        # TODO(ekl) bulk wait for performance
        res, _ = ray.wait(list(self._future_to_actor), num_returns=1, timeout=timeout)
        timeout_msg = "Timed out waiting for result"
        raise_timeout_after_ignore = False
        if res:
            [future] = res
        else:
            if not ignore_if_timedout:
                raise TimeoutError(timeout_msg)
            else:
                raise_timeout_after_ignore = True
        i, a = self._future_to_actor.pop(future)
        self._return_actor(a)
        del self._index_to_future[i]
        self._next_return_index = max(self._next_return_index, i + 1)
        if raise_timeout_after_ignore:
            raise TimeoutError(
                timeout_msg + ". The task {} has been ignored.".format(future)
            )
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
            .. testcode::

                import ray
                from ray.util.actor_pool import ActorPool

                @ray.remote
                class Actor:
                    def double(self, v):
                        return 2 * v

                a1 = Actor.remote()
                pool = ActorPool([a1])
                pool.submit(lambda a, v: a.double.remote(v), 1)
                print(pool.has_free())
                print(pool.get_next())
                print(pool.has_free())

            .. testoutput::

                False
                2
                True
        """
        return len(self._idle_actors) > 0 and len(self._pending_submits) == 0

    def pop_idle(self):
        """Removes an idle actor from the pool.

        Returns:
            An idle actor if one is available.
            None if no actor was free to be removed.

        Examples:
            .. testcode::

                import ray
                from ray.util.actor_pool import ActorPool

                @ray.remote
                class Actor:
                    def double(self, v):
                        return 2 * v

                a1 = Actor.remote()
                pool = ActorPool([a1])
                pool.submit(lambda a, v: a.double.remote(v), 1)
                assert pool.pop_idle() is None
                assert pool.get_next() == 2
                assert pool.pop_idle() == a1

        """
        if self.has_free():
            return self._idle_actors.pop()
        return None

    def push(self, actor):
        """Pushes a new actor into the current list of idle actors.

        Examples:
            .. testcode::

                import ray
                from ray.util.actor_pool import ActorPool

                @ray.remote
                class Actor:
                    def double(self, v):
                        return 2 * v

                a1, a2 = Actor.remote(), Actor.remote()
                pool = ActorPool([a1])
                pool.push(a2)
        """
        busy_actors = []
        if self._future_to_actor.values():
            _, busy_actors = zip(*self._future_to_actor.values())
        if actor in self._idle_actors or actor in busy_actors:
            raise ValueError("Actor already belongs to current ActorPool")
        else:
            self._return_actor(actor)
