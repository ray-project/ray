"""This is a module for unique IDs in Ray.
We define different types for different IDs for type safety.

See https://github.com/ray-project/ray/issues/3721.
"""

# WARNING: Any additional ID types defined in this file must be added to the
# _ID_TYPES list at the bottom of this file.
import os

from ray.includes.unique_ids cimport (
    CActorClassID,
    CActorID,
    CNodeID,
    CConfigID,
    CJobID,
    CFunctionID,
    CObjectID,
    CTaskID,
    CUniqueID,
    CWorkerID,
    CPlacementGroupID
)

import ray
from ray._private.utils import decode


def check_id(b, size=kUniqueIDSize):
    if not isinstance(b, bytes):
        raise TypeError("Unsupported type: " + str(type(b)))
    if len(b) != size:
        raise ValueError("ID string needs to have length " +
                         str(size) + ", got " + str(len(b)))


cdef extern from "ray/common/constants.h" nogil:
    cdef int64_t kUniqueIDSize


cdef class BaseID:

    cdef size_t hash(self):
        pass

    def binary(self):
        pass

    def size(self):
        pass

    def hex(self):
        pass

    def is_nil(self):
        pass

    def __hash__(self):
        return self.hash()

    def __eq__(self, other):
        return type(self) == type(other) and self.binary() == other.binary()

    def __ne__(self, other):
        return type(self) != type(other) or self.binary() != other.binary()

    def __bytes__(self):
        return self.binary()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return self.__class__.__name__ + "(" + self.hex() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.binary(),)

    def redis_shard_hash(self):
        # NOTE: The hash function used here must match the one in
        # GetRedisContext in src/ray/gcs/tables.h. Changes to the
        # hash function should only be made through std::hash in
        # src/common/common.h.
        # Do not use __hash__ that returns signed uint64_t, which
        # is different from std::hash in c++ code.
        return self.hash()


cdef class UniqueID(BaseID):
    cdef CUniqueID data

    def __init__(self, id):
        check_id(id)
        self.data = CUniqueID.FromBinary(id)

    @classmethod
    def from_binary(cls, id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return cls(id_bytes)

    @classmethod
    def nil(cls):
        return cls(CUniqueID.Nil().Binary())

    @classmethod
    def from_random(cls):
        return cls(CUniqueID.FromRandom().Binary())

    def size(self):
        return CUniqueID.Size()

    def binary(self):
        return self.data.Binary()

    def hex(self):
        return decode(self.data.Hex())

    def is_nil(self):
        return self.data.IsNil()

    cdef size_t hash(self):
        return self.data.Hash()


cdef class TaskID(BaseID):
    cdef CTaskID data

    def __init__(self, id):
        check_id(id, CTaskID.Size())
        self.data = CTaskID.FromBinary(<c_string>id)

    cdef CTaskID native(self):
        return <CTaskID>self.data

    def size(self):
        return CTaskID.Size()

    def binary(self):
        return self.data.Binary()

    def hex(self):
        return decode(self.data.Hex())

    def is_nil(self):
        return self.data.IsNil()

    def actor_id(self):
        return ActorID(self.data.ActorId().Binary())

    def job_id(self):
        return JobID(self.data.JobId().Binary())

    cdef size_t hash(self):
        return self.data.Hash()

    @classmethod
    def nil(cls):
        return cls(CTaskID.Nil().Binary())

    @classmethod
    def size(cls):
        return CTaskID.Size()

    @classmethod
    def for_fake_task(cls):
        return cls(CTaskID.ForFakeTask().Binary())

    @classmethod
    def for_driver_task(cls, job_id):
        return cls(CTaskID.ForDriverTask(
            CJobID.FromBinary(job_id.binary())).Binary())

    @classmethod
    def for_actor_creation_task(cls, actor_id):
        assert isinstance(actor_id, ActorID)
        return cls(CTaskID.ForActorCreationTask(
            CActorID.FromBinary(actor_id.binary())).Binary())

    @classmethod
    def for_actor_task(cls, job_id, parent_task_id,
                       parent_task_counter, actor_id):
        assert isinstance(job_id, JobID)
        assert isinstance(parent_task_id, TaskID)
        assert isinstance(actor_id, ActorID)
        return cls(CTaskID.ForActorTask(
            CJobID.FromBinary(job_id.binary()),
            CTaskID.FromBinary(parent_task_id.binary()),
            parent_task_counter,
            CActorID.FromBinary(actor_id.binary())).Binary())

    @classmethod
    def for_normal_task(cls, job_id, parent_task_id, parent_task_counter):
        assert isinstance(job_id, JobID)
        assert isinstance(parent_task_id, TaskID)
        return cls(CTaskID.ForNormalTask(
            CJobID.FromBinary(job_id.binary()),
            CTaskID.FromBinary(parent_task_id.binary()),
            parent_task_counter).Binary())

cdef class NodeID(UniqueID):

    def __init__(self, id):
        check_id(id)
        self.data = CNodeID.FromBinary(<c_string>id)

    cdef CNodeID native(self):
        return <CNodeID>self.data


cdef class JobID(BaseID):
    cdef CJobID data

    def __init__(self, id):
        check_id(id, CJobID.Size())
        self.data = CJobID.FromBinary(<c_string>id)

    cdef CJobID native(self):
        return <CJobID>self.data

    @classmethod
    def from_int(cls, value):
        assert value < 2**32, "Maximum JobID integer is 2**32 - 1."
        return cls(CJobID.FromInt(value).Binary())

    @classmethod
    def nil(cls):
        return cls(CJobID.Nil().Binary())

    @classmethod
    def size(cls):
        return CJobID.Size()

    def int(self):
        return self.data.ToInt()

    def binary(self):
        return self.data.Binary()

    def hex(self):
        return decode(self.data.Hex())

    def size(self):
        return CJobID.Size()

    def is_nil(self):
        return self.data.IsNil()

    cdef size_t hash(self):
        return self.data.Hash()

cdef class WorkerID(UniqueID):

    def __init__(self, id):
        check_id(id)
        self.data = CWorkerID.FromBinary(<c_string>id)

    cdef CWorkerID native(self):
        return <CWorkerID>self.data

cdef class ActorID(BaseID):

    def __init__(self, id):
        check_id(id, CActorID.Size())
        self.data = CActorID.FromBinary(<c_string>id)

    cdef CActorID native(self):
        return <CActorID>self.data

    @classmethod
    def of(cls, job_id, parent_task_id, parent_task_counter):
        assert isinstance(job_id, JobID)
        assert isinstance(parent_task_id, TaskID)
        return cls(CActorID.Of(CJobID.FromBinary(job_id.binary()),
                               CTaskID.FromBinary(parent_task_id.binary()),
                               parent_task_counter).Binary())

    @classmethod
    def nil(cls):
        return cls(CActorID.Nil().Binary())

    @classmethod
    def from_random(cls):
        return cls(os.urandom(CActorID.Size()))

    @classmethod
    def size(cls):
        return CActorID.Size()

    def binary(self):
        return self.data.Binary()

    def hex(self):
        return decode(self.data.Hex())

    def size(self):
        return CActorID.Size()

    def is_nil(self):
        return self.data.IsNil()

    cdef size_t hash(self):
        return self.data.Hash()


cdef class ClientActorRef(ActorID):

    def __init__(self, id: bytes):
        check_id(id, CActorID.Size())
        self.data = CActorID.FromBinary(<c_string>id)
        client.ray.call_retain(id)

    def __dealloc__(self):
        if client is None or client.ray is None:
            # The client package or client.ray object might be set
            # to None when the script exits. Should be safe to skip
            # call_release in this case, since the client should have already
            # disconnected at this point.
            return
        if client.ray.is_connected() and not self.data.IsNil():
            client.ray.call_release(self.id)

    @property
    def id(self):
        return self.binary()


cdef class FunctionID(UniqueID):

    def __init__(self, id):
        check_id(id)
        self.data = CFunctionID.FromBinary(<c_string>id)

    cdef CFunctionID native(self):
        return <CFunctionID>self.data


cdef class ActorClassID(UniqueID):

    def __init__(self, id):
        check_id(id)
        self.data = CActorClassID.FromBinary(<c_string>id)

    cdef CActorClassID native(self):
        return <CActorClassID>self.data

# This type alias is for backward compatibility.
ObjectID = ObjectRef

cdef class PlacementGroupID(BaseID):
    cdef CPlacementGroupID data

    def __init__(self, id):
        check_id(id, CPlacementGroupID.Size())
        self.data = CPlacementGroupID.FromBinary(<c_string>id)

    cdef CPlacementGroupID native(self):
        return <CPlacementGroupID>self.data

    @classmethod
    def from_random(cls):
        return cls(CPlacementGroupID.FromRandom().Binary())

    @classmethod
    def nil(cls):
        return cls(CPlacementGroupID.Nil().Binary())

    @classmethod
    def size(cls):
        return CPlacementGroupID.Size()

    def binary(self):
        return self.data.Binary()

    def hex(self):
        return decode(self.data.Hex())

    def size(self):
        return CPlacementGroupID.Size()

    def is_nil(self):
        return self.data.IsNil()

    cdef size_t hash(self):
        return self.data.Hash()

_ID_TYPES = [
    ActorClassID,
    ActorID,
    NodeID,
    JobID,
    WorkerID,
    FunctionID,
    ObjectID,
    TaskID,
    UniqueID,
    PlacementGroupID,
]
