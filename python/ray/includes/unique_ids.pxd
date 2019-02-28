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
        CActorCheckpointID()
        CActorCheckpointID(const CUniqueID &from_id)

        @staticmethod
        CActorCheckpointID from_binary(const c_string &binary)


    cdef cppclass CActorClassID "ray::ActorClassID"(CUniqueID):
        CActorClassID()
        CActorClassID(const CUniqueID &from_id)

        @staticmethod
        CActorClassID from_binary(const c_string &binary)


    cdef cppclass CActorID "ray::ActorID"(CUniqueID):
        CActorID()
        CActorID(const CUniqueID &from_id)

        @staticmethod
        CActorID from_binary(const c_string &binary)


    cdef cppclass CActorHandleID "ray::ActorHandleID"(CUniqueID):
        CActorHandleID()
        CActorHandleID(const CUniqueID &from_id)

        @staticmethod
        CActorHandleID from_binary(const c_string &binary)


    cdef cppclass CClientID "ray::ClientID"(CUniqueID):
        CClientID()
        CClientID(const CUniqueID &from_id)

        @staticmethod
        CClientID from_binary(const c_string &binary)


    cdef cppclass CConfigID "ray::ConfigID"(CUniqueID):
        CConfigID()
        CConfigID(const CUniqueID &from_id)

        @staticmethod
        CConfigID from_binary(const c_string &binary)


    cdef cppclass CFunctionID "ray::FunctionID"(CUniqueID):
        CFunctionID()
        CFunctionID(const CUniqueID &from_id)

        @staticmethod
        CFunctionID from_binary(const c_string &binary)


    cdef cppclass CDriverID "ray::DriverID"(CUniqueID):
        CDriverID()
        CDriverID(const CUniqueID &from_id)

        @staticmethod
        CDriverID from_binary(const c_string &binary)


    cdef cppclass CJobID "ray::JobID"(CUniqueID):
        CJobID()
        CJobID(const CUniqueID &from_id)

        @staticmethod
        CJobID from_binary(const c_string &binary)


    cdef cppclass CTaskID "ray::TaskID"(CUniqueID):
        CTaskID()
        CTaskID(const CUniqueID &from_id)

        @staticmethod
        CTaskID from_binary(const c_string &binary)


    cdef cppclass CObjectID" ray::ObjectID"(CUniqueID):
        CObjectID()
        CObjectID(const CUniqueID &from_id)

        @staticmethod
        CObjectID from_binary(const c_string &binary)


    cdef cppclass CWorkerID "ray::WorkerID"(CUniqueID):
        CWorkerID()
        CWorkerID(const CUniqueID &from_id)

        @staticmethod
        CWorkerID from_binary(const c_string &binary)
