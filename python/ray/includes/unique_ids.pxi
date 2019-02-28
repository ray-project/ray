"""This is a module for unique IDs in Ray.
We define different types for different IDs for type safety.

See https://github.com/ray-project/ray/issues/3721.
"""

# WARNING: Any additional ID types defined in this file must be added to the
# _ID_TYPES list at the bottom of this file.
from ray.includes.common cimport (
    ComputePutId,
    ComputeTaskId,
)
from ray.includes.unique_ids cimport (
    CActorCheckpointID,
    CActorClassID,
    CActorHandleID,
    CActorID,
    CClientID,
    CConfigID,
    CDriverID,
    CFunctionID,
    CObjectID,
    CTaskID,
    CUniqueID,
    CWorkerID,
)

from ray.utils import decode


def check_id(b):
    if not isinstance(b, bytes):
        raise TypeError("Unsupported type: " + str(type(b)))
    if len(b) != kUniqueIDSize:
        raise ValueError("ID string needs to have length " +
                         str(kUniqueIDSize))


cdef extern from "ray/constants.h" nogil:
    cdef int64_t kUniqueIDSize
    cdef int64_t kMaxTaskPuts


cdef class UniqueID:
    cdef CUniqueID data

    def __init__(self, id):
        if not id:
            self.data = CUniqueID()
        else:
            check_id(id)
            self.data = CUniqueID.from_binary(id)

    @classmethod
    def from_binary(cls, id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return cls(id_bytes)

    @classmethod
    def nil(cls):
        return cls(b"")

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        return type(self) == type(other) and self.binary() == other.binary()

    def __ne__(self, other):
        return self.binary() != other.binary()

    def size(self):
        return self.data.size()

    def binary(self):
        return self.data.binary()

    def __bytes__(self):
        return self.binary()

    def hex(self):
        return decode(self.data.hex())

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
        # src/common/common.h
        return self.data.hash()


cdef class ObjectID(UniqueID):
    pass


cdef class TaskID(UniqueID):
    pass


cdef class ClientID(UniqueID):
    pass


cdef class DriverID(UniqueID):
    pass


cdef class ActorID(UniqueID):
    pass


cdef class ActorHandleID(UniqueID):
    pass


cdef class ActorCheckpointID(UniqueID):
    pass


cdef class FunctionID(UniqueID):
    pass


cdef class ActorClassID(UniqueID):
    pass


_ID_TYPES = [
    ActorCheckpointID,
    ActorClassID,
    ActorHandleID,
    ActorID,
    ClientID,
    DriverID,
    FunctionID,
    ObjectID,
    TaskID,
    UniqueID,
]
