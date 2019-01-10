"""This is a module for unique IDs in Ray.
We define different types for different IDs for type safe.

See https://github.com/ray-project/ray/issues/3721.
"""

from ray.includes.common cimport *


cdef class UniqueID:
    cdef CUniqueID data
    def __cinit__(self, object_id):
        if object_id is None:
            self.data = CUniqueID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CUniqueID.from_binary(object_id)
        elif isinstance (object_id, UniqueID):
            self.data = CUniqueID((<UniqueID>object_id).data)
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    def from_random():
        return UniqueID(CUniqueID.from_random().binary())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return UniqueID(id_bytes)

    @staticmethod
    def nil():
        return UniqueID(CUniqueID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<UniqueID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<UniqueID?>other).data
        except TypeError:
            return False

    def size(self):
        return self.data.size()

    def __len__(self):
        return self.size()

    def binary(self):
        return self.data.binary()

    def __bytes__(self):
        return self.binary()

    def hex(self):
        return self.data.hex()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "UniqueID(" + self.data.hex().decode() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), self.binary()


cdef class ObjectID:
    cdef CObjectID data
    def __cinit__(self, object_id):
        if object_id is None:
            self.data = CObjectID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CObjectID.from_binary(object_id)
        elif isinstance(object_id, ObjectID):
            self.data = CObjectID((<ObjectID>object_id).data)
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    def from_random():
        return ObjectID(CObjectID.from_random().binary())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return ObjectID(id_bytes)

    @staticmethod
    def nil():
        return ObjectID(CObjectID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<ObjectID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<ObjectID?>other).data
        except TypeError:
            return False

    def size(self):
        return self.data.size()

    def __len__(self):
        return self.size()

    def binary(self):
        return self.data.binary()

    def __bytes__(self):
        return self.binary()

    def hex(self):
        return self.data.hex()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "ObjectID(" + self.data.hex().decode() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), self.binary()


cdef class JobID:
    cdef CJobID data
    def __cinit__(self, object_id):
        if object_id is None:
            self.data = CJobID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CJobID.from_binary(object_id)
        elif isinstance(object_id, JobID):
            self.data = CJobID((<JobID>object_id).data)
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    cdef from_native(const CJobID& cpp_id):
        return JobID(cpp_id.binary())

    @staticmethod
    def from_random():
        return JobID(CJobID.from_random().binary())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return JobID(id_bytes)

    @staticmethod
    def nil():
        return JobID(CJobID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<JobID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<JobID?>other).data
        except TypeError:
            return False

    def size(self):
        return self.data.size()

    def __len__(self):
        return self.size()

    def binary(self):
        return self.data.binary()

    def __bytes__(self):
        return self.binary()

    def hex(self):
        return self.data.hex()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "JobID(" + self.data.hex().decode() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), self.binary()


cdef class TaskID:
    cdef CTaskID data
    def __cinit__(self, object_id):
        if object_id is None:
            self.data = CTaskID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CTaskID.from_binary(object_id)
        elif isinstance (object_id, TaskID):
            self.data = CTaskID((<TaskID>object_id).data)
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    def from_random():
        return TaskID(CTaskID.from_random().binary())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return TaskID(id_bytes)

    @staticmethod
    def nil():
        return TaskID(CTaskID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<TaskID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<TaskID?>other).data
        except TypeError:
            return False

    def size(self):
        return self.data.size()

    def __len__(self):
        return self.size()

    def binary(self):
        return self.data.binary()

    def __bytes__(self):
        return self.binary()

    def hex(self):
        return self.data.hex()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "TaskID(" + self.data.hex().decode() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), self.binary()


cdef class ClientID:
    cdef CClientID data
    def __cinit__(self, object_id):
        if object_id is None:
            self.data = CClientID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CClientID.from_binary(object_id)
        elif isinstance (object_id, ClientID):
            self.data = CClientID((<ClientID>object_id).data)
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    cdef from_native(const CClientID& cpp_id):
        return ClientID(cpp_id.binary())

    @staticmethod
    def from_random():
        return ClientID(CClientID.from_random().binary())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return ClientID(id_bytes)

    @staticmethod
    def nil():
        return ClientID(CClientID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<ClientID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<ClientID?>other).data
        except TypeError:
            return False

    def size(self):
        return self.data.size()

    def __len__(self):
        return self.size()

    def binary(self):
        return self.data.binary()

    def __bytes__(self):
        return self.binary()

    def hex(self):
        return self.data.hex()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "ClientID(" + self.data.hex().decode() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), self.binary()


cdef class WorkerID:
    cdef CWorkerID data
    def __cinit__(self, object_id):
        if object_id is None:
            self.data = CWorkerID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CWorkerID.from_binary(object_id)
        elif isinstance(object_id, WorkerID):
            self.data = CWorkerID((<WorkerID>object_id).data)
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    def from_random():
        return WorkerID(CWorkerID.from_random().binary())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return WorkerID(id_bytes)

    @staticmethod
    def nil():
        return WorkerID(CWorkerID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<WorkerID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<WorkerID?>other).data
        except TypeError:
            return False

    def size(self):
        return self.data.size()

    def __len__(self):
        return self.size()

    def binary(self):
        return self.data.binary()

    def __bytes__(self):
        return self.binary()

    def hex(self):
        return self.data.hex()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "WorkerID(" + self.data.hex().decode() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), self.binary()


cdef class DriverID:
    cdef CDriverID data
    def __cinit__(self, object_id):
        if object_id is None:
            self.data = CDriverID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CDriverID.from_binary(object_id)
        elif isinstance(object_id, DriverID):
            self.data = CDriverID((<DriverID>object_id).data)
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    def from_random():
        return DriverID(CDriverID.from_random().binary())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return DriverID(id_bytes)

    @staticmethod
    def nil():
        return DriverID(CDriverID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<DriverID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<DriverID?>other).data
        except TypeError:
            return False

    def size(self):
        return self.data.size()

    def __len__(self):
        return self.size()

    def binary(self):
        return self.data.binary()

    def __bytes__(self):
        return self.binary()

    def hex(self):
        return self.data.hex()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "DriverID(" + self.data.hex().decode() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), self.binary()


cdef class ActorID:
    cdef CActorID data
    def __cinit__(self, object_id):
        if object_id is None:
            self.data = CActorID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CActorID.from_binary(object_id)
        elif isinstance (object_id, ActorID):
            self.data = CActorID((<ActorID>object_id).data)
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    def from_random():
        return ActorID(CActorID.from_random().binary())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return ActorID(id_bytes)

    @staticmethod
    def nil():
        return ActorID(CActorID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<ActorID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<ActorID?>other).data
        except TypeError:
            return False

    def size(self):
        return self.data.size()

    def __len__(self):
        return self.size()

    def binary(self):
        return self.data.binary()

    def __bytes__(self):
        return self.binary()

    def hex(self):
        return self.data.hex()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "ActorID(" + self.data.hex().decode() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), self.binary()


cdef class ActorHandleID:
    cdef CActorHandleID data
    def __cinit__(self, object_id):
        if object_id is None:
            self.data = CActorHandleID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CActorHandleID.from_binary(object_id)
        elif isinstance(object_id, ActorHandleID):
            self.data = CActorHandleID((<ActorHandleID>object_id).data)
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    def from_random():
        return ActorHandleID(CActorHandleID.from_random().binary())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return ActorHandleID(id_bytes)

    @staticmethod
    def nil():
        return ActorHandleID(CActorHandleID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<ActorHandleID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<ActorHandleID?>other).data
        except TypeError:
            return False

    def size(self):
        return self.data.size()

    def __len__(self):
        return self.size()

    def binary(self):
        return self.data.binary()

    def __bytes__(self):
        return self.binary()

    def hex(self):
        return self.data.hex()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "ActorHandleID(" + self.data.hex().decode() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), self.binary()


cdef class ConfigID:
    cdef CConfigID data
    def __cinit__(self, object_id):
        if object_id is None:
            self.data = CConfigID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CConfigID.from_binary(object_id)
        elif isinstance (object_id, ConfigID):
            self.data = CConfigID((<ConfigID>object_id).data)
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    def from_random():
        return ConfigID(CConfigID.from_random().binary())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return ConfigID(id_bytes)

    @staticmethod
    def nil():
        return ConfigID(CConfigID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<ConfigID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<ConfigID?>other).data
        except TypeError:
            return False

    def size(self):
        return self.data.size()

    def __len__(self):
        return self.size()

    def binary(self):
        return self.data.binary()

    def __bytes__(self):
        return self.binary()

    def hex(self):
        return self.data.hex()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "ConfigID(" + self.data.hex().decode() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), self.binary()


cdef class FunctionID:
    cdef CFunctionID data
    def __cinit__(self, object_id):
        if object_id is None:
            self.data = CFunctionID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CFunctionID.from_binary(object_id)
        elif isinstance(object_id, FunctionID):
            self.data = CFunctionID((<FunctionID>object_id).data)
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    def from_random():
        return FunctionID(CFunctionID.from_random().binary())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return FunctionID(id_bytes)

    @staticmethod
    def nil():
        return FunctionID(CFunctionID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<FunctionID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<FunctionID?>other).data
        except TypeError:
            return False

    def size(self):
        return self.data.size()

    def __len__(self):
        return self.size()

    def binary(self):
        return self.data.binary()

    def __bytes__(self):
        return self.binary()

    def hex(self):
        return self.data.hex()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "FunctionID(" + self.data.hex().decode() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), self.binary()


cdef class ClassID:
    cdef CClassID data
    def __cinit__(self, object_id):
        if object_id is None:
            self.data = CClassID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CClassID.from_binary(object_id)
        elif isinstance (object_id, ClassID):
            self.data = CClassID((<ClassID>object_id).data)
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    def from_random():
        return ClassID(CClassID.from_random().binary())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return ClassID(id_bytes)

    @staticmethod
    def nil():
        return ClassID(CClassID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<ClassID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<ClassID?>other).data
        except TypeError:
            return False

    def size(self):
        return self.data.size()

    def __len__(self):
        return self.size()

    def binary(self):
        return self.data.binary()

    def __bytes__(self):
        return self.binary()

    def hex(self):
        return self.data.hex()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "ClassID(" + self.data.hex().decode() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), self.binary()