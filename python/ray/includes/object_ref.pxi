from ray.includes.unique_ids cimport CObjectID

import asyncio

import ray

cdef class ObjectRef(BaseID):

    def __init__(self, id):
        check_id(id)
        self.data = CObjectID.FromBinary(<c_string>id)
        self.in_core_worker = False

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

    cdef size_t hash(self):
        return self.data.Hash()

    @classmethod
    def nil(cls):
        return cls(CObjectID.Nil().Binary())

    @classmethod
    def from_random(cls):
        return cls(CObjectID.FromRandom().Binary())

    def __await__(self):
        return self.as_future().__await__()

    def as_future(self):
        loop = asyncio.get_event_loop()
        core_worker = ray.worker.global_worker.core_worker

        future = loop.create_future()
        core_worker.get_async(self, future)
        # A hack to keep a reference to the object ref for ref counting.
        future.object_ref = self
        return future
