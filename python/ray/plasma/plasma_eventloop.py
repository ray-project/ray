import asyncio
import collections
import ctypes
import functools
import selectors
import socket
import sys

import ray


def _release_waiter(waiter, *args):
    if not waiter.done():
        waiter.set_result(None)


class PlasmaObjectFuture(asyncio.Future):
    def __init__(self, loop, object_id):
        super().__init__(loop=loop)
        self.ref_count = 0
        self.object_id = object_id

    def inc_refcount(self):
        self.ref_count += 1

    def dec_refcount(self):
        assert self.ref_count > 0
        self.ref_count -= 1
        if self.ref_count <= 0:
            self.cancel()

    def complete(self):
        self.set_result(self.object_id)

    def __repr__(self):
        return super().__repr__() + "{object_id=%s, ref_count=%d}" % (
            self.object_id, self.ref_count)


class PlasmaFutureGroup(asyncio.Future):
    def __init__(self, loop, return_exceptions=False, keep_duplicated=True):
        super().__init__(loop=loop)
        self._children = []
        self._future_set = set()
        self._keep_duplicated = keep_duplicated
        self.return_exceptions = return_exceptions
        self.nfinished = 0

    def append(self, coro_or_future):
        if not asyncio.futures.isfuture(coro_or_future):
            fut = asyncio.ensure_future(coro_or_future, loop=self._loop)
            if self.loop is None:
                self.loop = fut._loop
            # The caller cannot control this future, the "destroy pending task"
            # warning should not be emitted.
            fut._log_destroy_pending = False
        else:
            fut = coro_or_future

            if self._loop is None:
                self._loop = fut._loop
            elif fut._loop is not self._loop:
                raise ValueError(
                    "futures are tied to different event loops")

        if fut in self._future_set and not self._keep_duplicated:
            return
        fut.add_done_callback(self._done_callback)
        self._children.append(fut)
        self._future_set.add(fut)

    def pop(self, index=0):
        fut = self._children.pop(index)
        fut.remove_done_callback(self._done_callback)
        self._future_set.remove(fut)
        return fut

    @property
    def children(self):
        return self._children

    @property
    def nchildren(self):
        return len(self._children)

    def halt_on_all_finished(self):
        return self.nfinished >= len(self._children)

    def halt_on_any_finished(self):
        return self.nfinished > 0

    def halt_on_some_finished(self, n):
        return self.nfinished >= n

    def _halt_on(self):
        """
        This function can be override to change the halt condition.

        Returns:
        bool
        """

        return self.halt_on_all_finished()

    def set_halt_condition(self, cond):
        self._halt_on = cond

    def _collect_results(self):
        results = []

        for fut in self._children:
            if fut.cancelled() and self.return_exceptions:
                res = asyncio.futures.CancelledError()
            elif fut._exception is not None and self.return_exceptions:
                res = fut.exception()  # Mark exception retrieved.
            else:
                res = fut._result
            results.append(res)

        return results

    def _done_callback(self, fut):
        if self.done():
            if not fut.cancelled():
                # Mark exception retrieved.
                fut.exception()
            return

        if not self.return_exceptions:
            if fut.cancelled():
                self.set_exception(asyncio.futures.CancelledError())
                return
            elif fut._exception is not None:
                self.set_exception(fut.exception())
                return

        self.nfinished += 1

        if self._halt_on():
            self.set_result(self._collect_results())

    def cancel(self):
        if self.done():
            return False
        ret = False
        for child in self._children:
            if child.cancel():
                ret = True
        return ret

    def flush_results(self):
        done, pending = [], []
        for f in self._children:
            if f.done():
                done.append(f)
            else:
                pending.append(f)
            f.remove_done_callback(self._done_callback)
        return done, pending

    @asyncio.coroutine
    def wait(self, timeout=None):
        if not self._children:
            return [], []

        loop = self._loop

        waiter = loop.create_future()
        timeout_handle = None
        if timeout is not None:
            timeout_handle = loop.call_later(timeout, _release_waiter, waiter)

        def _on_completion(_):
            if timeout_handle is not None:
                timeout_handle.cancel()
            if not waiter.done():
                waiter.set_result(None)

        self.add_done_callback(_on_completion)

        try:
            yield from waiter
        finally:
            if timeout_handle is not None:
                timeout_handle.cancel()

        return self.flush_results()


def gather(*coros_or_futures, loop=None, return_exceptions=False):
    """
    This method resembles `asyncio.gather`

    Args:
        *coros_or_futures:
        loop:
        return_exceptions:

    Returns:

    """

    fut = PlasmaFutureGroup(loop=loop, return_exceptions=return_exceptions)
    for f in coros_or_futures:
        fut.append(f)
    return fut


@asyncio.coroutine
def wait(*coros_or_futures, timeout, num_returns, loop=None,
         return_exceptions=False):
    """
    This method resembles `asyncio.wait`

    Args:
        *coros_or_futures:
        timeout:
        num_returns:
        loop:
        return_exceptions:

    Returns:

    """

    fut = PlasmaFutureGroup(loop=loop, return_exceptions=return_exceptions)
    fut.set_halt_condition(functools.partial(
        fut.halt_on_some_finished,
        n=num_returns,
    ))
    for f in coros_or_futures:
        fut.append(f)

    return (yield from fut.wait(timeout))


class PlasmaEpoll(selectors.BaseSelector):
    def __init__(self, worker):
        self.worker = worker
        self.client = self.worker.plasma_client
        self.socket = socket.fromfd(self.client.notification_fd,
                                    socket.AF_UNIX, socket.SOCK_STREAM, 0)
        self.waiting_dict = collections.defaultdict(list)

    def _read_message(self):
        size_data = self.socket.recv(ctypes.sizeof(ctypes.c_int64))
        size = int.from_bytes(size_data, sys.byteorder, signed=True)
        data = self.socket.recv(size)
        return data

    def _decode_data(self, data):
        # `ObjectInfo` is defined in `ray/src/common/format/common.fbs`
        # TODO: decode data
        raise NotImplementedError

    def select(self, timeout=None):
        self.socket.settimeout(timeout)

        try:
            data = self._read_message()
        except (BlockingIOError, socket.timeout):
            return []
        finally:
            self.socket.settimeout(None)

        ready_keys = []
        object_ids = self._decode_data(data)
        for oid in object_ids:
            if oid in self.waiting_dict:
                key = self.waiting_dict[oid]
                ready_keys.append(key)
        return ready_keys

    def register(self, plasma_fut, events=None, data=None):
        if plasma_fut.object_id in self.waiting_dict:
            raise Exception("ObjectID already been registered.")
        else:
            key = selectors.SelectorKey(fileobj=plasma_fut,
                                        fd=plasma_fut.object_id, events=events,
                                        data=data)
            self.waiting_dict[key.fd] = key
            return key

    def unregister(self, plasma_fut):
        return self.waiting_dict.pop(plasma_fut.object_id)

    def get_map(self):
        return self.waiting_dict

    def get_key(self, object_id):
        return self.waiting_dict[object_id]


class PlasmaPoll(selectors.BaseSelector):
    def __init__(self, worker):
        self.worker = worker
        self.waiting_dict = {}

    def close(self):
        self.waiting_dict.clear()

    def select(self, timeout=None):
        if not self.waiting_dict:
            return []

        polling_ids = list(self.waiting_dict.keys())
        ready_keys = []
        object_ids, _ = ray.wait(polling_ids, num_returns=len(polling_ids),
                                 timeout=timeout, worker=self.worker)
        for oid in object_ids:
            key = self.waiting_dict[oid]
            ready_keys.append(key)
        return ready_keys

    def register(self, plasma_fut, events=None, data=None):
        if plasma_fut.object_id in self.waiting_dict:
            raise Exception("ObjectID already been registered.")
        else:
            key = selectors.SelectorKey(fileobj=plasma_fut,
                                        fd=plasma_fut.object_id, events=events,
                                        data=data)
            self.waiting_dict[key.fd] = key
            return key

    def unregister(self, plasma_fut):
        return self.waiting_dict.pop(plasma_fut.object_id)

    def get_map(self):
        return self.waiting_dict

    def get_key(self, object_id):
        return self.waiting_dict[object_id]


class PlasmaSelectorEventLoop(asyncio.BaseEventLoop):

    def __init__(self, selector, worker):
        super().__init__()
        assert isinstance(selector, selectors.BaseSelector)
        self._selector = selector
        self._worker = worker

    def _process_events(self, event_list):
        for key in event_list:
            handle = key.data
            assert isinstance(handle, asyncio.events.Handle), \
                "A Handle is required here"
            if handle._cancelled:
                return
            assert not isinstance(handle, asyncio.events.TimerHandle)
            self._ready.append(handle)

    def close(self):
        if self.is_running():
            raise RuntimeError("Cannot close a running event loop")
        if self.is_closed():
            return
        super().close()
        if self._selector is not None:
            self._selector.close()
            self._selector = None

    def _register_future(self, future):
        future = asyncio.ensure_future(future, loop=self)
        fut = PlasmaObjectFuture(loop=self,
                                 object_id=ray.local_scheduler.ObjectID(
                                     b'\0' * 20))
        if self.get_debug():
            print("Processing indirect future %s" % future)

        def callback(_future):
            object_id = _future.result()
            assert isinstance(object_id, ray.local_scheduler.ObjectID)
            if self.get_debug():
                print("Registering indirect future...")
            reg_future = self._register_id(object_id)
            fut.object_id = object_id  # here we get the waiting id

            def reg_callback(_fut):
                result = _fut.result()
                fut.set_result(result)

            reg_future.add_done_callback(reg_callback)

        future.add_done_callback(callback)
        return fut

    def _register_id(self, object_id):
        self._check_closed()

        if not isinstance(object_id, ray.local_scheduler.ObjectID):
            return self._register_future(object_id)

        try:
            key = self._selector.get_key(object_id)
        except KeyError:
            def callback(future):
                # set result and remove it from the selector
                if future.cancelled():
                    return
                future.complete()
                # done object_ids should all be unregistered
                self._selector.unregister(future)
                if self.get_debug():
                    print("%s removed from the selector." % future)

            fut = PlasmaObjectFuture(loop=self, object_id=object_id)
            handle = asyncio.events.Handle(callback, args=[fut], loop=self)
            self._selector.register(fut, events=None, data=handle)
            if self.get_debug():
                print("%s added to the selector." % fut)
        else:
            # Keep a unique Future object for an object_id.
            # Increase ref_count instead.
            fut = key.fileobj
            if self.get_debug():
                print("%s exists." % fut)

        fut.inc_refcount()

        return fut

    def _release(self, *fut):
        for f in fut:
            f.dec_refcount()
            if f.cancelled():
                self._selector.unregister(f)

    @asyncio.coroutine
    def get(self, object_ids):
        if not isinstance(object_ids, list):
            ready_ids = yield from self._register_id(object_ids)
        else:
            ready_ids = yield from gather(
                *[self._register_id(oid) for oid in object_ids], loop=self)

        return ray.get(ready_ids, worker=self._worker)

    @asyncio.coroutine
    def wait(self, object_ids, num_returns=1, timeout=None,
             return_exact_num=True):
        futures = [self._register_id(oid) for oid in object_ids]
        _done, _pending = yield from wait(
            *futures, timeout=timeout,
            num_returns=num_returns, loop=self,
        )

        self._release(*_pending)
        done = [fut.object_id for fut in _done]
        pending = [fut.object_id for fut in _pending]

        if return_exact_num and len(done) > num_returns:
            done, pending = done[:num_returns], done[num_returns:] + pending

        return done, pending
