"""This is a module for unique IDs in Ray.
We define different types for different IDs for type safety.

See https://github.com/ray-project/ray/issues/3721.
"""

# WARNING: Any additional ID types defined in this file must be added to the
# _ID_TYPES list at the bottom of this file.
import os

from ray.includes.unique_ids cimport (
    CActorCheckpointId,
    CActorClassId,
    CActorHandleId,
    CActorId,
    CClientId,
    CConfigId,
    CDriverId,
    CFunctionId,
    CObjectId,
    CTaskId,
    CUniqueID,
    CWorkerId,
)

from ray.utils import decode


def check_id(b, size=kUniqueIDSize):
    if not isinstance(b, bytes):
        raise TypeError("Unsupported type: " + str(type(b)))
    if len(b) != size:
        raise ValueError("ID string needs to have length " +
                         str(size))


cdef extern from "ray/constants.h" nogil:
    cdef int64_t kUniqueIDSize
    cdef int64_t kMaxTaskPuts


cdef class BaseID:

    # To avoid the error of "Python int too large to convert to C ssize_t",
    # here `cdef size_t` is required.
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
        return self.binary() != other.binary()

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
        self.data = CUniqueID.from_binary(id)

    @classmethod
    def from_binary(cls, id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return cls(id_bytes)

    @classmethod
    def nil(cls):
        return cls(CUniqueID.nil().binary())

    
    @classmethod
    def from_random(cls):
        return cls(os.urandom(CUniqueID.size()))

    def size(self):
        return CUniqueID.size()

    def binary(self):
        return self.data.binary()

    def hex(self):
        return decode(self.data.hex())
    
    def is_nil(self):
        return self.data.is_nil()

    cdef size_t hash(self):
        return self.data.hash()


cdef class ObjectId(BaseID):
    cdef CObjectId data

    def __init__(self, id):
        check_id(id)
        self.data = CObjectId.from_binary(<c_string>id)

    cdef CObjectId native(self):
        return <CObjectId>self.data

    def size(self):
        return CObjectId.size()

    def binary(self):
        return self.data.binary()

    def hex(self):
        return decode(self.data.hex())
    
    def is_nil(self):
        return self.data.is_nil()

    cdef size_t hash(self):
        return self.data.hash()

    @classmethod
    def nil(cls):
        return cls(CObjectId.nil().binary())

    @classmethod
    def from_random(cls):
        return cls(os.urandom(CObjectId.size()))


cdef class TaskId(BaseID):
    cdef CTaskId data

    def __init__(self, id):
        check_id(id, CTaskId.size())
        self.data = CTaskId.from_binary(<c_string>id)

    cdef CTaskId native(self):
        return <CTaskId>self.data

    def size(self):
        return CTaskId.size()

    def binary(self):
        return self.data.binary()

    def hex(self):
        return decode(self.data.hex())

    def is_nil(self):
        return self.data.is_nil()

    cdef size_t hash(self):
        return self.data.hash()

    @classmethod
    def nil(cls):
        return cls(CTaskId.nil().binary())

    @classmethod
    def size(cla):
        return CTaskId.size()

    @classmethod
    def from_random(cls):
        return cls(os.urandom(CTaskId.size()))


cdef class ClientId(UniqueID):

    def __init__(self, id):
        check_id(id)
        self.data = CClientId.from_binary(<c_string>id)

    cdef CClientId native(self):
        return <CClientId>self.data


cdef class DriverId(UniqueID):

    def __init__(self, id):
        check_id(id)
        self.data = CDriverId.from_binary(<c_string>id)

    cdef CDriverId native(self):
        return <CDriverId>self.data


cdef class ActorId(UniqueID):

    def __init__(self, id):
        check_id(id)
        self.data = CActorId.from_binary(<c_string>id)

    cdef CActorId native(self):
        return <CActorId>self.data


cdef class ActorHandleId(UniqueID):

    def __init__(self, id):
        check_id(id)
        self.data = CActorHandleId.from_binary(<c_string>id)

    cdef CActorHandleId native(self):
        return <CActorHandleId>self.data


cdef class ActorCheckpointId(UniqueID):

    def __init__(self, id):
        check_id(id)
        self.data = CActorCheckpointId.from_binary(<c_string>id)

    cdef CActorCheckpointId native(self):
        return <CActorCheckpointId>self.data


cdef class FunctionId(UniqueID):

    def __init__(self, id):
        check_id(id)
        self.data = CFunctionId.from_binary(<c_string>id)

    cdef CFunctionId native(self):
        return <CFunctionId>self.data


cdef class ActorClassId(UniqueID):

    def __init__(self, id):
        check_id(id)
        self.data = CActorClassId.from_binary(<c_string>id)

    cdef CActorClassId native(self):
        return <CActorClassId>self.data

_ID_TYPES = [
    ActorCheckpointId,
    ActorClassId,
    ActorHandleId,
    ActorId,
    ClientId,
    DriverId,
    FunctionId,
    ObjectId,
    TaskId,
    UniqueID,
]
