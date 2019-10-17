import ray
import time


class ActorPool(object):
    def __init__(self, actors):
        self._idle_actors = list(actors)
        self._future_to_actor = {}
        self._index_to_future = {}
        self._next_task_index = 0
        self._next_return_index = 0
        self._pending_submits = []

    def map(self, fn, values):
        for v in values:
            self.submit(lambda a, v: fn(a, v), v)
        while self.has_next():
            yield self.get_next()

    def map_unordered(self, fn, values):
        for v in values:
            self.submit(lambda a, v: fn(a, v), v)
        while self.has_next():
            yield self.get_next_unordered()

    def submit(self, fn, value):
        if self._idle_actors:
            actor = self._idle_actors.pop()
            future = fn(actor, value)
            self._future_to_actor[future] = (self._next_task_index, actor)
            self._index_to_future[self._next_task_index] = future
            self._next_task_index += 1
        else:
            self._pending_submits.append((fn, value))

    def has_next(self):
        return bool(self._future_to_actor)

    def get_next(self, timeout=None):
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
