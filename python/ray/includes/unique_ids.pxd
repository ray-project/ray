from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libc.stdint cimport uint8_t

cdef extern from "ray/id.h" namespace "ray" nogil:
    cdef cppclass CBaseId[T]:
        @staticmethod
        T from_random()

        @staticmethod
        T from_binary(const c_string &binary)

        @staticmethod
        const T nil()

        @staticmethod
        size_t size()

        size_t hash() const
        c_bool is_nil() const
        c_bool operator==(const CBaseId &rhs) const
        c_bool operator!=(const CBaseId &rhs) const
        const uint8_t *data() const;

        c_string binary() const;
        c_string hex() const;

    cdef cppclass CUniqueID "ray::UniqueID"(CBaseId):
        CUniqueID()

        @staticmethod
        size_t size()

        @staticmethod
        CUniqueID from_random()

        @staticmethod
        CUniqueID from_binary(const c_string &binary)

        @staticmethod
        const CUniqueID nil()

        @staticmethod
        size_t size()

    cdef cppclass CActorCheckpointID "ray::ActorCheckpointID"(CUniqueID):

        @staticmethod
        CActorCheckpointID from_binary(const c_string &binary)

    cdef cppclass CActorClassID "ray::ActorClassID"(CUniqueID):

        @staticmethod
        CActorClassID from_binary(const c_string &binary)

    cdef cppclass CActorID "ray::ActorID"(CUniqueID):

        @staticmethod
        CActorID from_binary(const c_string &binary)

    cdef cppclass CActorHandleID "ray::ActorHandleID"(CUniqueID):

        @staticmethod
        CActorHandleID from_binary(const c_string &binary)

    cdef cppclass CClientID "ray::ClientID"(CUniqueID):

        @staticmethod
        CClientID from_binary(const c_string &binary)

    cdef cppclass CConfigID "ray::ConfigID"(CUniqueID):

        @staticmethod
        CConfigID from_binary(const c_string &binary)

    cdef cppclass CFunctionID "ray::FunctionID"(CUniqueID):

        @staticmethod
        CFunctionID from_binary(const c_string &binary)

    cdef cppclass CDriverID "ray::DriverID"(CUniqueID):

        @staticmethod
        CDriverID from_binary(const c_string &binary)

    cdef cppclass CTaskID "ray::TaskID"(CBaseId[CTaskID]):

        @staticmethod
        CTaskID from_binary(const c_string &binary)

        @staticmethod
        const CTaskID nil()

        @staticmethod
        size_t size()

    cdef cppclass CObjectID" ray::ObjectID"(CBaseId[CObjectID]):

        @staticmethod
        CObjectID from_binary(const c_string &binary)

        @staticmethod
        const CObjectID nil()

        @staticmethod
        size_t size()

    cdef cppclass CWorkerID "ray::WorkerID"(CUniqueID):

        @staticmethod
        CWorkerID from_binary(const c_string &binary)
