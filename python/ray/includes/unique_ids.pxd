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
        CActorCheckpointID(const c_string &binary)
        CActorCheckpointID(const CUniqueID &from_id)


    cdef cppclass CActorClassID "ray::ActorClassID"(CUniqueID):
        CActorClassID()
        CActorClassID(const c_string &binary)
        CActorClassID(const CUniqueID &from_id)


    cdef cppclass CActorID "ray::ActorID"(CUniqueID):
        CActorID()
        CActorID(const c_string &binary)
        CActorID(const CUniqueID &from_id)


    cdef cppclass CActorHandleID "ray::ActorHandleID"(CUniqueID):
        CActorHandleID()
        CActorHandleID(const c_string &binary)
        CActorHandleID(const CUniqueID &from_id)


    cdef cppclass CClientID "ray::ClientID"(CUniqueID):
        CClientID()
        CClientID(const c_string &binary)
        CClientID(const CUniqueID &from_id)


    cdef cppclass CConfigID "ray::ConfigID"(CUniqueID):
        CConfigID()
        CConfigID(const c_string &binary)
        CConfigID(const CUniqueID &from_id)


    cdef cppclass CFunctionID "ray::FunctionID"(CUniqueID):
        CFunctionID()
        CFunctionID(const c_string &binary)
        CFunctionID(const CUniqueID &from_id)


    cdef cppclass CDriverID "ray::DriverID"(CUniqueID):
        CDriverID()
        CDriverID(const c_string &binary)
        CDriverID(const CUniqueID &from_id)


    cdef cppclass CJobID "ray::JobID"(CUniqueID):
        CJobID()
        CJobID(const c_string &binary)
        CJobID(const CUniqueID &from_id)


    cdef cppclass CTaskID "ray::TaskID"(CUniqueID):
        CTaskID()
        CTaskID(const c_string &binary)
        CTaskID(const CUniqueID &from_id)


    cdef cppclass CObjectID" ray::ObjectID"(CUniqueID):
        CObjectID()
        CObjectID(const c_string &binary)
        CObjectID(const CUniqueID &from_id)


    cdef cppclass CWorkerID "ray::WorkerID"(CUniqueID):
        CWorkerID()
        CWorkerID(const c_string &binary)
        CWorkerID(const CUniqueID &from_id)
