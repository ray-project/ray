from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from multiprocessing import TimeoutError

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
            timeout=0)
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
        self._decorator = ray.remote(max_calls=maxtasksperchild)
        if initializer is not None:
            if initargs is None:
                initargs = ()

            def wrapped(worker_info):
                initializer(*initargs)

            ray.worker.global_worker.run_function_on_all_workers(wrapped)

    def _check_running(self):
        if self._closed:
            raise ValueError("Pool not running")

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
        if not args:
            args = ()
        if not kwargs:
            kwargs = {}
        remote_func = self._decorator(func)
        object_id = remote_func.remote(*args, **kwargs)
        return AsyncResult(object_id, callback, error_callback)

    # TODO: chunksize? batching?
    def map(self, func, iterable, chunksize=None):
        return self.map_async(func, iterable).get()

    def starmap(self, func, iterable, chunksize=None):
        self._check_running()
        remote_func = self._decorator(func)
        return ray.get([remote_func(*args) for args in iterable])

    def map_async(self,
                  func,
                  iterable,
                  chunksize=None,
                  callback=None,
                  error_callback=None):
        self._check_running()
        remote_func = self._decorator(func)
        object_ids = [remote_func.remote(arg) for arg in iterable]
        return AsyncResult(object_ids, callback, error_callback)

    def starmap_async(self, func, iterable, callback=None,
                      error_callback=None):
        self._check_running()
        remote_func = self._decorator(func)
        object_ids = [remote_func.remote(*args) for args in iterable]
        return AsyncResult(object_ids, callback, error_callback)

    # TODO: this shouldn't actually submit everything at once for memory
    # considerations. should just submit as we get results in iterator?
    def imap(self, func, iterable, chunksize=None):
        self._check_running()
        remote_func = self._decorator(func)
        object_ids = [remote_func.remote(arg) for arg in iterable]
        return IMapIterator(object_ids, in_order=False)

    def imap_unordered(self, func, iterable, chunksize=None):
        self._check_running()
        remote_func = self._decorator(func)
        object_ids = [remote_func.remote(arg) for arg in iterable]
        return IMapIterator(object_ids, in_order=False)

    def close(self):
        self._closed = True

    def terminate(self):
        ray.shutdown()

    def join(self):
        ray.shutdown()
