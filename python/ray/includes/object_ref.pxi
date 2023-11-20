from ray.includes.unique_ids cimport CObjectID

import asyncio
import concurrent.futures
import functools
import logging
import threading
from typing import Callable, Any, Union

import ray
import cython

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

    def __cinit__(self):
        self.in_core_worker = False

    def __init__(
            self, id, owner_addr="", call_site_data="",
            skip_adding_local_ref=False):
        self._set_id(id)
        self.owner_addr = owner_addr
        self.in_core_worker = False
        self.call_site_data = call_site_data

        worker = ray._private.worker.global_worker
        # TODO(edoakes): We should be able to remove the in_core_worker flag.
        # But there are still some dummy object refs being created outside the
        # context of a core worker.
        if hasattr(worker, "core_worker"):
            if not skip_adding_local_ref:
                worker.core_worker.add_object_ref_reference(self)
            self.in_core_worker = True

    def __dealloc__(self):
        if self.in_core_worker:
            try:
                worker = ray._private.worker.global_worker
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
        return self.data

    def binary(self):
        return self.data.Binary()

    def hex(self):
        return decode(self.data.Hex())

    def is_nil(self):
        return self.data.IsNil()

    cdef size_t hash(self):
        return self.data.Hash()

    def task_id(self):
        return TaskID(self.data.TaskId().Binary())

    def job_id(self):
        return self.task_id().job_id()

    def owner_address(self):
        return self.owner_addr

    def call_site(self):
        return decode(self.call_site_data)

    def size(self):
        return CObjectID.Size()

    def _set_id(self, id):
        check_id(id)
        self.data = CObjectID.FromBinary(<c_string>id)

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
        core_worker = ray._private.worker.global_worker.core_worker
        core_worker.set_get_async_callback(self, py_callback)
        return self
