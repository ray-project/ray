from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libc.stdint cimport uint8_t, int64_t

cdef extern from "ray/id.h" namespace "ray" nogil:
    cdef cppclass CBaseID[T]:
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
        c_bool operator==(const CBaseID &rhs) const
        c_bool operator!=(const CBaseID &rhs) const
        const uint8_t *data() const;

        c_string binary() const;
        c_string hex() const;

    cdef cppclass CUniqueID "ray::UniqueID"(CBaseID):
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

    cdef cppclass CActorCheckpointId "ray::ActorCheckpointId"(CUniqueID):

        @staticmethod
        CActorCheckpointId from_binary(const c_string &binary)

    cdef cppclass CActorClassId "ray::ActorClassId"(CUniqueID):

        @staticmethod
        CActorClassId from_binary(const c_string &binary)

    cdef cppclass CActorId "ray::ActorId"(CUniqueID):

        @staticmethod
        CActorId from_binary(const c_string &binary)

    cdef cppclass CActorHandleId "ray::ActorHandleId"(CUniqueID):

        @staticmethod
        CActorHandleId from_binary(const c_string &binary)

    cdef cppclass CClientId "ray::ClientId"(CUniqueID):

        @staticmethod
        CClientId from_binary(const c_string &binary)

    cdef cppclass CConfigId "ray::ConfigId"(CUniqueID):

        @staticmethod
        CConfigId from_binary(const c_string &binary)

    cdef cppclass CFunctionId "ray::FunctionId"(CUniqueID):

        @staticmethod
        CFunctionId from_binary(const c_string &binary)

    cdef cppclass CDriverId "ray::DriverId"(CUniqueID):

        @staticmethod
        CDriverId from_binary(const c_string &binary)

    cdef cppclass CTaskId "ray::TaskId"(CBaseID[CTaskId]):

        @staticmethod
        CTaskId from_binary(const c_string &binary)

        @staticmethod
        const CTaskId nil()

        @staticmethod
        size_t size()

    cdef cppclass CObjectId" ray::ObjectId"(CBaseID[CObjectId]):

        @staticmethod
        CObjectId from_binary(const c_string &binary)

        @staticmethod
        const CObjectId nil()

        @staticmethod
        CObjectId for_put(const CTaskId &task_id, int64_t index);

        @staticmethod
        CObjectId for_task_return(const CTaskId &task_id, int64_t index);

        @staticmethod
        size_t size()

        c_bool is_put()

        int64_t object_index() const 

        CTaskId task_id() const

    cdef cppclass CWorkerId "ray::WorkerId"(CUniqueID):

        @staticmethod
        CWorkerId from_binary(const c_string &binary)
