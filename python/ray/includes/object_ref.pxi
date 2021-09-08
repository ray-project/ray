from ray.includes.unique_ids cimport CObjectID

import asyncio
import concurrent.futures
import functools
import logging
from typing import Callable, Any, Union

import ray
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.util.client as client

logger = logging.getLogger(__name__)


def _set_future_helper(
    result: Any,
    *,
    py_future: Union[asyncio.Future, concurrent.futures.Future],
):
    # Issue #11030, #8841
    # If this future has result set already, we just need to
    # skip the set result/exception procedure.
    if py_future.done():
        return

    if isinstance(result, RayTaskError):
        py_future.set_exception(result.as_instanceof_cause())
    elif isinstance(result, RayError):
        # Directly raise exception for RayActorError
        py_future.set_exception(result)
    else:
        py_future.set_result(result)


cdef class ObjectRef(BaseID):

    def __init__(self, id, call_site_data=""):
        check_id(id)
        self.data = CObjectID.FromBinary(<c_string>id)
        self.in_core_worker = False
        self.call_site_data = call_site_data

        worker = ray.worker.global_worker
        # TODO(edoakes): We should be able to remove the in_core_worker flag.
        # But there are still some dummy object refs being created outside the
        # context of a core worker.
        if hasattr(worker, "core_worker"):
            worker.core_worker.add_object_ref_reference(self)
            self.in_core_worker = True

    def __dealloc__(self):
        if self.in_core_worker:
            try:
                worker = ray.worker.global_worker
                worker.core_worker.remove_object_ref_reference(self)
            except Exception as e:
                # There is a strange error in rllib that causes the above to
                # fail. Somehow the global 'ray' variable corresponding to the
                # imported package is None when this gets called. Unfortunately
                # this is hard to debug because __dealloc__ is called during
                # garbage collection so we can't get a good stack trace. In any
                # case, there's not much we can do besides ignore it
                # (re-importing ray won't help).
                pass

    cdef CObjectID native(self):
        return <CObjectID>self.data

    def size(self):
        return CObjectID.Size()

    def binary(self):
        return self.data.Binary()

    def hex(self):
        return decode(self.data.Hex())

    def is_nil(self):
        return self.data.IsNil()

    def task_id(self):
        return TaskID(self.data.TaskId().Binary())

    def job_id(self):
        return self.task_id().job_id()

    def call_site(self):
        return decode(self.call_site_data)

    cdef size_t hash(self):
        return self.data.Hash()

    @classmethod
    def nil(cls):
        return cls(CObjectID.Nil().Binary())

    @classmethod
    def from_random(cls):
        return cls(CObjectID.FromRandom().Binary())

    def future(self) -> concurrent.futures.Future:
        """Wrap ObjectRef with a concurrent.futures.Future

        Note that the future cancellation will not cancel the correspoding
        task when the ObjectRef representing return object of a task.
        Additionally, future.running() will always be ``False`` even if the
        underlying task is running.
        """
        py_future = concurrent.futures.Future()

        self._on_completed(
            functools.partial(_set_future_helper, py_future=py_future))

        # A hack to keep a reference to the object ref for ref counting.
        py_future.object_ref = self
        return py_future

    def __await__(self):
        return self.as_future(_internal=True).__await__()

    def as_future(self, _internal=False) -> asyncio.Future:
        """Wrap ObjectRef with an asyncio.Future.

        Note that the future cancellation will not cancel the correspoding
        task when the ObjectRef representing return object of a task.
        """
        if not _internal:
            logger.warning("ref.as_future() is deprecated in favor of "
                           "asyncio.wrap_future(ref.future()).")
        return asyncio.wrap_future(self.future())

    def _on_completed(self, py_callback: Callable[[Any], None]):
        """Register a callback that will be called after Object is ready.
        If the ObjectRef is already ready, the callback will be called soon.
        The callback should take the result as the only argument. The result
        can be an exception object in case of task error.
        """
        core_worker = ray.worker.global_worker.core_worker
        core_worker.set_get_async_callback(self, py_callback)
        return self


cdef class ClientObjectRef(ObjectRef):

    def __init__(self, id: bytes):
        check_id(id)
        self.data = CObjectID.FromBinary(<c_string>id)
        client.ray.call_retain(id)
        self.in_core_worker = False

    def __dealloc__(self):
        if client is None or client.ray is None:
            # Similar issue as mentioned in ObjectRef.__dealloc__ above. The
            # client package or client.ray object might be set
            # to None when the script exits. Should be safe to skip
            # call_release in this case, since the client should have already
            # disconnected at this point.
            return
        if client.ray.is_connected() and not self.data.IsNil():
            client.ray.call_release(self.id)

    @property
    def id(self):
        return self.binary()

    def future(self) -> concurrent.futures.Future:
        fut = concurrent.futures.Future()

        def set_value(data: Any) -> None:
            """Schedules a callback to set the exception or result
            in the Future."""

            if isinstance(data, Exception):
                fut.set_exception(data)
            else:
                fut.set_result(data)

        self._on_completed(set_value)

        # Prevent this object ref from being released.
        fut.object_ref = self
        return fut

    def _on_completed(self, py_callback: Callable[[Any], None]) -> None:
        """Register a callback that will be called after Object is ready.
        If the ObjectRef is already ready, the callback will be called soon.
        The callback should take the result as the only argument. The result
        can be an exception object in case of task error.
        """
        from ray.util.client.client_pickler import loads_from_server

        def deserialize_obj(resp: ray_client_pb2.DataResponse) -> None:
            """Converts from a GetResponse proto to a python object."""
            obj = resp.get
            data = None
            if not obj.valid:
                data = loads_from_server(resp.get.error)
            else:
                data = loads_from_server(resp.get.data)

            py_callback(data)

        client.ray._register_callback(self, deserialize_obj)
