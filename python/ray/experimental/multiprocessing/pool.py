from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from multiprocessing import TimeoutError
import random

import ray


# TODO: callbacks not working
class AsyncResult(object):
    def __init__(self, object_ids, callback=None, result_callback=None):
        self._object_ids = object_ids
        self._callback = callback
        self._result_callback = result_callback

    @property
    def _object_id_list(self):
        if isinstance(self._object_ids, list):
            return self._object_ids
        else:
            return [self._object_ids]

    def get(self, timeout=None):
        try:
            return ray.get(self._object_ids, timeout=timeout)
        except ray.exceptions.RayTimeoutError:
            raise TimeoutError

    def wait(self, timeout=None):
        ray.wait(
            self._object_id_list,
            num_returns=len(self._object_id_list),
            timeout=timeout)

    def ready(self):
        ready_ids, _ = ray.wait(
            self._object_id_list,
            num_returns=len(self._object_id_list),
            timeout=0.0)
        return len(ready_ids) == len(self._object_id_list)

    def successful(self):
        if not self.ready():
            raise ValueError("{0!r} not ready".format(self))
        try:
            ray.get(self._object_ids)
        except Exception:
            return False
        return True


class IMapIterator(object):
    def __init__(self, object_ids):
        self._object_ids = object_ids
        self._index = 0

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()


class OrderedIMapIterator(IMapIterator):
    def next(self, timeout=None):
        if self._index == len(self._object_ids):
            raise StopIteration
        try:
            result = ray.get(self._object_ids[self._index], timeout=timeout)
        except ray.exceptions.RayTimeoutError:
            raise TimeoutError
        self._index += 1
        return result


class UnorderedIMapIterator(IMapIterator):
    def next(self, timeout=None):
        if self._index == len(self._object_ids):
            raise StopIteration
        ready_ids, _ = ray.wait(
            self._object_ids[self._index:], timeout=timeout)
        if len(ready_ids == 0):
            raise TimeoutError

        self._index += 1
        return ray.get(ready_ids[0])


@ray.remote
class PoolActor(object):
    def __init__(self, initializer=None, *initargs):
        print("init actor")
        if initializer:
            print("\tinitializer")
            initializer(*initargs)

    def run(self, func, *args, **kwargs):
        return func(*args, **kwargs)


# https://docs.python.org/3/library/multiprocessing.html#module-multiprocessing.pool
class Pool(object):
    # TODO: what about context argument?
    # TODO: maxtasksperchild doesn't quite map correctly
    def __init__(self,
                 processes=None,
                 initializer=None,
                 initargs=None,
                 maxtasksperchild=None,
                 context=None):
        ray.init(num_cpus=processes)
        self._closed = False
        self._initializer = initializer
        self._initargs = initargs if initargs else ()
        self._maxtasksperchild = maxtasksperchild if maxtasksperchild else -1
        self._actor_pool = [self._new_actor_entry() for _ in range(processes)]
        self._actor_deletion_ids = []

    def _wait_for_stopping_actors(self, timeout=None):
        if len(self._actor_deletion_ids) == 0:
            return
        _, deleting = ray.wait(
            self._actor_deletion_ids,
            num_returns=len(self._actor_deletion_ids),
            timeout=timeout)
        self._actor_deletion_ids = deleting

    def _stop_actor(self, actor):
        # Remove finished deletion IDs so.
        # The deletion task will block until the actor has finished executing
        # all pending tasks.
        self._wait_for_stopping_actors(timeout=0.0)
        self._actor_deletion_ids.append(actor.__ray_terminate__.remote())

    def _check_running(self):
        if self._closed:
            raise ValueError("Pool not running")

    def _new_actor_entry(self):
        return (PoolActor.remote(self._initializer, *self._initargs), 0)

    def _run(self, actor_index, func, args=None, kwargs=None):
        if not args:
            args = ()
        if not kwargs:
            kwargs = {}
        actor, count = self._actor_pool[actor_index]
        object_id = actor.run.remote(func, *args, **kwargs)
        count += 1
        if count == self._maxtasksperchild:
            self._stop_actor(actor)
            actor, count = self._new_actor_entry()
        self._actor_pool[actor_index] = (actor, count)
        return object_id

    def _run_random(self, func, args=None, kwargs=None):
        return self._run(
            random.randrange(len(self._actor_pool)), func, args, kwargs)

    def apply(self, func, args=None, kwargs=None):
        return self.apply_async(func, args, kwargs).get()

    # TODO: do callbacks in background thread?
    def apply_async(self,
                    func,
                    args=None,
                    kwargs=None,
                    callback=None,
                    error_callback=None):
        self._check_running()
        object_id = self._run_random(func, args, kwargs)
        return AsyncResult(object_id, callback, error_callback)

    def map(self, func, iterable, chunksize=None):
        self._check_running()
        return self._map_async(func, iterable, unpack_args=False).get()

    def map_async(self,
                  func,
                  iterable,
                  chunksize=None,
                  callback=None,
                  error_callback=None):
        self._check_running()
        return self._map_async(
            func,
            iterable,
            unpack_args=False,
            callback=callback,
            error_callback=error_callback)

    def starmap(self, func, iterable, chunksize=None):
        self._check_running()
        return self._map_async(func, iterable, unpack_args=True).get()

    def starmap_async(self, func, iterable, callback=None,
                      error_callback=None):
        self._check_running()
        return self._map_async(
            func,
            iterable,
            unpack_args=True,
            callback=callback,
            error_callback=error_callback)

    # TODO: chunksize, callbacks
    def _map_async(self,
                   func,
                   iterable,
                   unpack_args=False,
                   chunksize=None,
                   callback=None,
                   error_callback=None):
        object_ids = []
        # TODO: check that it's an iterable
        for i, args in enumerate(iterable):
            if not unpack_args:
                args = (args, )
            object_ids.append(
                self._run(i % len(self._actor_pool), func, args=args))
        return AsyncResult(object_ids, callback, error_callback)

    # TODO: this shouldn't actually submit everything at once for memory
    # considerations. should just submit as we get results in iterator?
    # TODO
    def imap(self, func, iterable, chunksize=None):
        self._check_running()
        remote_func = self._decorator(func)
        object_ids = [remote_func.remote(arg) for arg in iterable]
        return IMapIterator(object_ids, in_order=False)

    # TODO
    def imap_unordered(self, func, iterable, chunksize=None):
        self._check_running()
        remote_func = self._decorator(func)
        object_ids = [remote_func.remote(arg) for arg in iterable]
        return IMapIterator(object_ids, in_order=False)

    def close(self):
        for actor, _ in self._actor_pool:
            self._stop_actor(actor)
        self._closed = True

    # TODO: shouldn't complete outstanding work.
    def terminate(self):
        return self.close()

    def join(self):
        if not self._closed:
            raise ValueError("Pool is still running")
        self._wait_for_stopping_actors()
