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

    @staticmethod
    cdef from_native(const CUniqueID& cpp_id):
        cdef UniqueID self = UniqueID.__new__(UniqueID)
        self.data = cpp_id
        return self

    @classmethod
    def from_binary(cls, id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return cls(id_bytes)

    @staticmethod
    def nil():
        return UniqueID.from_native(CUniqueID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        return self.binary() == other.binary()

    def __ne__(self, other):
        return self.binary() != other.binary()

    def size(self):
        return self.data.size()

    def __len__(self):
        return self.size()

    def binary(self):
        return self.data.binary()

    def __bytes__(self):
        return self.binary()

    def hex(self):
        return decode(self.data.hex())

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "UniqueID(" + self.hex() + ")"

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

    def __init__(self, id):
        if not id:
            self.data = CUniqueID()
        else:
            check_id(id)
            self.data = CUniqueID.from_binary(id)

    @staticmethod
    cdef from_native(const CObjectID& cpp_id):
        cdef ObjectID self = ObjectID.__new__(ObjectID)
        self.data = cpp_id
        return self

    @staticmethod
    def nil():
        return ObjectID.from_native(CObjectID.nil())

    def __repr__(self):
        return "ObjectID(" + self.hex() + ")"


cdef class TaskID(UniqueID):

    def __init__(self, id):
        if not id:
            self.data = CUniqueID()
        else:
            check_id(id)
            self.data = CUniqueID.from_binary(id)

    @staticmethod
    cdef from_native(const CTaskID& cpp_id):
        cdef TaskID self = TaskID.__new__(TaskID)
        self.data = cpp_id
        return self

    @staticmethod
    def nil():
        return TaskID.from_native(CTaskID.nil())

    def __repr__(self):
        return "TaskID(" + self.hex() + ")"


cdef class ClientID(UniqueID):

    def __init__(self, id):
        if not id:
            self.data = CUniqueID()
        else:
            check_id(id)
            self.data = CUniqueID.from_binary(id)

    @staticmethod
    cdef from_native(const CClientID& cpp_id):
        cdef ClientID self = ClientID.__new__(ClientID)
        self.data = cpp_id
        return self

    @staticmethod
    def nil():
        return ClientID.from_native(CClientID.nil())

    def __repr__(self):
        return "ClientID(" + self.hex() + ")"


cdef class DriverID(UniqueID):

    def __init__(self, id):
        if not id:
            self.data = CUniqueID()
        else:
            check_id(id)
            self.data = CUniqueID.from_binary(id)

    @staticmethod
    cdef from_native(const CDriverID& cpp_id):
        cdef DriverID self = DriverID.__new__(DriverID)
        self.data = cpp_id
        return self

    @staticmethod
    def nil():
        return DriverID.from_native(CDriverID.nil())

    def __repr__(self):
        return "DriverID(" + self.hex() + ")"


cdef class ActorID(UniqueID):

    def __init__(self, id):
        if not id:
            self.data = CUniqueID()
        else:
            check_id(id)
            self.data = CUniqueID.from_binary(id)

    @staticmethod
    cdef from_native(const CActorID& cpp_id):
        cdef ActorID self = ActorID.__new__(ActorID)
        self.data = cpp_id
        return self

    @staticmethod
    def nil():
        return ActorID.from_native(CActorID.nil())

    def __repr__(self):
        return "ActorID(" + self.hex() + ")"


cdef class ActorHandleID(UniqueID):

    def __init__(self, id):
        if not id:
            self.data = CUniqueID()
        else:
            check_id(id)
            self.data = CUniqueID.from_binary(id)

    @staticmethod
    cdef from_native(const CActorHandleID& cpp_id):
        cdef ActorHandleID self = ActorHandleID.__new__(ActorHandleID)
        self.data = cpp_id
        return self

    @staticmethod
    def nil():
        return ActorHandleID.from_native(CActorHandleID.nil())

    def __repr__(self):
        return "ActorHandleID(" + self.hex() + ")"


cdef class ActorCheckpointID(UniqueID):

    def __init__(self, id):
        if not id:
            self.data = CUniqueID()
        else:
            check_id(id)
            self.data = CUniqueID.from_binary(id)

    @staticmethod
    cdef from_native(const CActorCheckpointID& cpp_id):
        cdef ActorCheckpointID self = ActorCheckpointID.__new__(ActorHandleID)
        self.data = cpp_id
        return self

    @staticmethod
    def nil():
        return ActorCheckpointID.from_native(CActorCheckpointID.nil())

    def __repr__(self):
        return "ActorCheckpointID(" + self.hex() + ")"


cdef class FunctionID(UniqueID):

    def __init__(self, id):
        if not id:
            self.data = CUniqueID()
        else:
            check_id(id)
            self.data = CUniqueID.from_binary(id)

    @staticmethod
    cdef from_native(const CFunctionID& cpp_id):
        cdef FunctionID self = FunctionID.__new__(FunctionID)
        self.data = cpp_id
        return self

    @staticmethod
    def nil():
        return FunctionID.from_native(CFunctionID.nil())

    def __repr__(self):
        return "FunctionID(" + self.hex() + ")"


cdef class ActorClassID(UniqueID):

    def __init__(self, id):
        if not id:
            self.data = CUniqueID()
        else:
            check_id(id)
            self.data = CUniqueID.from_binary(id)

    @staticmethod
    cdef from_native(const CActorClassID& cpp_id):
        cdef ActorClassID self = ActorClassID.__new__(ActorClassID)
        self.data = cpp_id
        return self

    @staticmethod
    def nil():
        return ActorClassID.from_native(CActorClassID.nil())

    def __repr__(self):
        return "ActorClassID(" + self.hex() + ")"


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
