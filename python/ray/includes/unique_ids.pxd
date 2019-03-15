from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libc.stdint cimport uint8_t

cdef extern from "ray/id.h" namespace "ray" nogil:
    cdef cppclass CUniqueID "ray::UniqueID":
        CUniqueID()
        CUniqueID(const c_string &binary)
        CUniqueID(const CUniqueID &from_id)

        @staticmethod
        CUniqueID from_random()

        @staticmethod
        CUniqueID from_binary(const c_string &binary)

        @staticmethod
        const CUniqueID nil()

        size_t hash() const
        c_bool is_nil() const
        c_bool operator==(const CUniqueID& rhs) const
        c_bool operator!=(const CUniqueID& rhs) const
        const uint8_t *data() const
        uint8_t *mutable_data()
        size_t size() const
        c_string binary() const
        c_string hex() const

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


    cdef cppclass CJobID "ray::JobID"(CUniqueID):

        @staticmethod
        CJobID from_binary(const c_string &binary)


    cdef cppclass CTaskID "ray::TaskID"(CUniqueID):

        @staticmethod
        CTaskID from_binary(const c_string &binary)


    cdef cppclass CObjectID" ray::ObjectID"(CUniqueID):

        @staticmethod
        CObjectID from_binary(const c_string &binary)


    cdef cppclass CWorkerID "ray::WorkerID"(CUniqueID):

        @staticmethod
        CWorkerID from_binary(const c_string &binary)
