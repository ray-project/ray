"""This is a module for unique IDs in Ray.
We define different types for different IDs for type safe.

See https://github.com/ray-project/ray/issues/3721.
"""

from ray.includes.common cimport (
    UniqueID as CUniqueID,
    TaskID as CTaskID,
    ObjectID as CObjectID,
    JobID as CJobID,
    FunctionID as CFunctionID,
    ClassID as CClassID,
    ActorID as CActorID,
    ActorHandleID as CActorHandleID,
    WorkerID as CWorkerID,
    DriverID as CDriverID,
    ConfigID as CConfigID,
    ClientID as CClientID,
)


cdef class PyUniqueID:
    cdef CUniqueID data
    def __init__(self, object_id):
        if object_id is None:
            self.data = CUniqueID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CUniqueID.from_binary(object_id)
        elif isinstance(object_id, PyUniqueID):
            self.data = (<PyUniqueID>object_id).data
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    cdef from_native(const CUniqueID& cpp_id):
        cdef PyUniqueID self = PyUniqueID.__new__(PyUniqueID)
        self.data = cpp_id
        return self

    @staticmethod
    def from_random():
        return PyUniqueID.from_native(CUniqueID.from_random())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return PyUniqueID(id_bytes)

    @staticmethod
    def nil():
        return PyUniqueID.from_native(CUniqueID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<PyUniqueID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<PyUniqueID?>other).data
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
        return self.data.hex().decode()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "UniqueID(" + self.hex() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.binary(),)


cdef class PyObjectID:
    cdef CObjectID data
    def __init__(self, object_id):
        if object_id is None:
            self.data = CObjectID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CObjectID.from_binary(object_id)
        elif isinstance(object_id, PyObjectID):
            self.data = (<PyObjectID>object_id).data
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    cdef from_native(const CObjectID& cpp_id):
        cdef PyObjectID self = PyObjectID.__new__(PyObjectID)
        self.data = cpp_id
        return self

    @staticmethod
    def from_random():
        return PyObjectID.from_native(CObjectID.from_random())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return PyObjectID(id_bytes)

    @staticmethod
    def nil():
        return PyObjectID.from_native(CObjectID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<PyObjectID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<PyObjectID?>other).data
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
        return self.data.hex().decode()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "ObjectID(" + self.hex() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.binary(),)


cdef class PyJobID:
    cdef CJobID data
    def __init__(self, object_id):
        if object_id is None:
            self.data = CJobID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CJobID.from_binary(object_id)
        elif isinstance(object_id, PyJobID):
            self.data = (<PyJobID>object_id).data
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    cdef from_native(const CJobID& cpp_id):
        cdef PyJobID self = PyJobID.__new__(PyJobID)
        self.data = cpp_id
        return self

    @staticmethod
    def from_random():
        return PyJobID.from_native(CJobID.from_random())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return PyJobID(id_bytes)

    @staticmethod
    def nil():
        return PyJobID.from_native(CJobID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<PyJobID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<PyJobID?>other).data
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
        return self.data.hex().decode()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "JobID(" + self.hex() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.binary(),)


cdef class PyTaskID:
    cdef CTaskID data
    def __init__(self, object_id):
        if object_id is None:
            self.data = CTaskID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CTaskID.from_binary(object_id)
        elif isinstance(object_id, PyTaskID):
            self.data = (<PyTaskID>object_id).data
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    cdef from_native(const CTaskID& cpp_id):
        cdef PyTaskID self = PyTaskID.__new__(PyTaskID)
        self.data = cpp_id
        return self

    @staticmethod
    def from_random():
        return PyTaskID.from_native(CTaskID.from_random())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return PyTaskID(id_bytes)

    @staticmethod
    def nil():
        return PyTaskID.from_native(CTaskID.nil())

    def __hash__(self):
        return self.data.hash()

    def redis_shard_hash(self):
        # NOTE: The hash function used here must match the one in GetRedisContext in
        # src/ray/gcs/tables.h. Changes to the hash function should only be made
        # through std::hash in src/common/common.h
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<PyTaskID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<PyTaskID?>other).data
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
        return self.data.hex().decode()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "TaskID(" + self.hex() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.binary(),)


cdef class PyClientID:
    cdef CClientID data
    def __init__(self, object_id):
        if object_id is None:
            self.data = CClientID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CClientID.from_binary(object_id)
        elif isinstance(object_id, PyClientID):
            self.data = (<PyClientID>object_id).data
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    cdef from_native(const CClientID& cpp_id):
        cdef PyClientID self = PyClientID.__new__(PyClientID)
        self.data = cpp_id
        return self

    @staticmethod
    def from_random():
        return PyClientID.from_native(CClientID.from_random())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return PyClientID(id_bytes)

    @staticmethod
    def nil():
        return PyClientID.from_native(CClientID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<PyClientID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<PyClientID?>other).data
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
        return self.data.hex().decode()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "ClientID(" + self.hex() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.binary(),)


cdef class PyWorkerID:
    cdef CWorkerID data
    def __init__(self, object_id):
        if object_id is None:
            self.data = CWorkerID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CWorkerID.from_binary(object_id)
        elif isinstance(object_id, PyWorkerID):
            self.data = (<PyWorkerID>object_id).data
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    cdef from_native(const CWorkerID& cpp_id):
        cdef PyWorkerID self = PyWorkerID.__new__(PyWorkerID)
        self.data = cpp_id
        return self

    @staticmethod
    def from_random():
        return PyWorkerID.from_native(CWorkerID.from_random())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return PyWorkerID(id_bytes)

    @staticmethod
    def nil():
        return PyWorkerID.from_native(CWorkerID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<PyWorkerID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<PyWorkerID?>other).data
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
        return self.data.hex().decode()

    def __hex__(self):
        return self.hex().decode()

    def __repr__(self):
        return "WorkerID(" + self.hex() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.binary(),)


cdef class PyDriverID:
    cdef CDriverID data
    def __init__(self, object_id):
        if object_id is None:
            self.data = CDriverID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CDriverID.from_binary(object_id)
        elif isinstance(object_id, PyDriverID):
            self.data = (<PyDriverID>object_id).data
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    cdef from_native(const CDriverID& cpp_id):
        cdef PyDriverID self = PyDriverID.__new__(PyDriverID)
        self.data = cpp_id
        return self

    @staticmethod
    def from_random():
        return PyDriverID.from_native(CDriverID.from_random())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return PyDriverID(id_bytes)

    @staticmethod
    def nil():
        return PyDriverID.from_native(CDriverID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<PyDriverID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<PyDriverID?>other).data
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
        return self.data.hex().decode()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "DriverID(" + self.hex() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.binary(),)


cdef class PyActorID:
    cdef CActorID data
    def __init__(self, object_id):
        if object_id is None:
            self.data = CActorID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CActorID.from_binary(object_id)
        elif isinstance(object_id, PyActorID):
            self.data = (<PyActorID>object_id).data
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    cdef from_native(const CActorID& cpp_id):
        cdef PyActorID self = PyActorID.__new__(PyActorID)
        self.data = cpp_id
        return self

    @staticmethod
    def from_random():
        return PyActorID.from_native(CActorID.from_random())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return PyActorID(id_bytes)

    @staticmethod
    def nil():
        return PyActorID.from_native(CActorID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<PyActorID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<PyActorID?>other).data
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
        return self.data.hex().decode()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "ActorID(" + self.hex() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.binary(),)


cdef class PyActorHandleID:
    cdef CActorHandleID data
    def __init__(self, object_id):
        if object_id is None:
            self.data = CActorHandleID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CActorHandleID.from_binary(object_id)
        elif isinstance(object_id, PyActorHandleID):
            self.data = (<PyActorHandleID>object_id).data
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    cdef from_native(const CActorHandleID& cpp_id):
        cdef PyActorHandleID self = PyActorHandleID.__new__(PyActorHandleID)
        self.data = cpp_id
        return self

    @staticmethod
    def from_random():
        return PyActorHandleID.from_native(CActorHandleID.from_random())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return PyActorHandleID(id_bytes)

    @staticmethod
    def nil():
        return PyActorHandleID.from_native(CActorHandleID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<PyActorHandleID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<PyActorHandleID?>other).data
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
        return self.data.hex().decode()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "ActorHandleID(" + self.hex() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.binary(),)


cdef class PyConfigID:
    cdef CConfigID data
    def __init__(self, object_id):
        if object_id is None:
            self.data = CConfigID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CConfigID.from_binary(object_id)
        elif isinstance(object_id, PyConfigID):
            self.data = (<PyConfigID>object_id).data
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    cdef from_native(const CConfigID& cpp_id):
        cdef PyConfigID self = PyConfigID.__new__(PyConfigID)
        self.data = cpp_id
        return self

    @staticmethod
    def from_random():
        return PyConfigID.from_native(CConfigID.from_random())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return PyConfigID(id_bytes)

    @staticmethod
    def nil():
        return PyConfigID.from_native(CConfigID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<PyConfigID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<PyConfigID?>other).data
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
        return self.data.hex().decode()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "ConfigID(" + self.hex() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.binary(),)


cdef class PyFunctionID:
    cdef CFunctionID data
    def __init__(self, object_id):
        if object_id is None:
            self.data = CFunctionID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CFunctionID.from_binary(object_id)
        elif isinstance(object_id, PyFunctionID):
            self.data = (<PyFunctionID>object_id).data
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    cdef from_native(const CFunctionID& cpp_id):
        cdef PyFunctionID self = PyFunctionID.__new__(PyFunctionID)
        self.data = cpp_id
        return self

    @staticmethod
    def from_random():
        return PyFunctionID.from_native(CFunctionID.from_random())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return PyFunctionID(id_bytes)

    @staticmethod
    def nil():
        return PyFunctionID.from_native(CFunctionID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<PyFunctionID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<PyFunctionID?>other).data
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
        return self.data.hex().decode()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "FunctionID(" + self.hex() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.binary(),)


cdef class PyClassID:
    cdef CClassID data
    def __init__(self, object_id):
        if object_id is None:
            self.data = CClassID()
        elif isinstance(object_id, bytes):
            if len(object_id) != kUniqueIDSize:
                raise ValueError("ID string needs to have length " + str(kUniqueIDSize))
            self.data = CClassID.from_binary(object_id)
        elif isinstance(object_id, PyClassID):
            self.data = (<PyClassID>object_id).data
        else:
            raise TypeError("Unsupported type: " + str(type(object_id)))

    @staticmethod
    cdef from_native(const CClassID& cpp_id):
        cdef PyClassID self = PyClassID.__new__(PyClassID)
        self.data = cpp_id
        return self

    @staticmethod
    def from_random():
        return PyClassID.from_native(CClassID.from_random())

    @staticmethod
    def from_binary(id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return PyClassID(id_bytes)

    @staticmethod
    def nil():
        return PyClassID.from_native(CClassID.nil())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        try:
            return self.data == (<PyClassID?>other).data
        except TypeError:
            return False

    def __ne__(self, other):
        try:
            return self.data != (<PyClassID?>other).data
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
        return self.data.hex().decode()

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return "ClassID(" + self.hex() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.binary(),)
