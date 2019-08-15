from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libc.stdint cimport uint8_t, uint32_t, int64_t

cdef extern from "ray/common/id.h" namespace "ray" nogil:
    cdef cppclass CBaseID[T]:
        @staticmethod
        T from_random()

        @staticmethod
        T FromBinary(const c_string &binary)

        @staticmethod
        const T Nil()

        @staticmethod
        size_t Size()

        size_t Hash() const
        c_bool IsNil() const
        c_bool operator==(const CBaseID &rhs) const
        c_bool operator!=(const CBaseID &rhs) const
        const uint8_t *data() const;

        c_string Binary() const;
        c_string Hex() const;

    cdef cppclass CUniqueID "ray::UniqueID"(CBaseID):
        CUniqueID()

        @staticmethod
        size_t Size()

        @staticmethod
        CUniqueID from_random()

        @staticmethod
        CUniqueID FromBinary(const c_string &binary)

        @staticmethod
        const CUniqueID Nil()

        @staticmethod
        size_t Size()

    cdef cppclass CActorCheckpointID "ray::ActorCheckpointID"(CUniqueID):

        @staticmethod
        CActorCheckpointID FromBinary(const c_string &binary)

    cdef cppclass CActorClassID "ray::ActorClassID"(CUniqueID):

        @staticmethod
        CActorClassID FromBinary(const c_string &binary)

    cdef cppclass CActorID "ray::ActorID"(CBaseID[CActorID]):

        @staticmethod
        CActorID FromBinary(const c_string &binary)

        @staticmethod
        const CActorID Nil()

        @staticmethod
        size_t Size()

        @staticmethod
        CActorID Of(CJobID job_id, CTaskID parent_task_id, int64_t parent_task_counter)


    cdef cppclass CActorHandleID "ray::ActorHandleID"(CUniqueID):

        @staticmethod
        CActorHandleID FromBinary(const c_string &binary)

    cdef cppclass CClientID "ray::ClientID"(CUniqueID):

        @staticmethod
        CClientID FromBinary(const c_string &binary)

    cdef cppclass CConfigID "ray::ConfigID"(CUniqueID):

        @staticmethod
        CConfigID FromBinary(const c_string &binary)

    cdef cppclass CFunctionID "ray::FunctionID"(CUniqueID):

        @staticmethod
        CFunctionID FromBinary(const c_string &binary)

    cdef cppclass CJobID "ray::JobID"(CBaseID[CJobID]):

        @staticmethod
        CJobID FromBinary(const c_string &binary)

        @staticmethod
        const CJobID Nil()

        @staticmethod
        size_t Size()

        @staticmethod
        CJobID FromInt(uint32_t value)

    cdef cppclass CTaskID "ray::TaskID"(CBaseID[CTaskID]):

        @staticmethod
        CTaskID FromBinary(const c_string &binary)

        @staticmethod
        const CTaskID Nil()

        @staticmethod
        size_t Size()

        @staticmethod
        CTaskID ForDriverTask(const CJobID &job_id)

        @staticmethod
        CTaskID ForFakeTask()

        @staticmethod
        CTaskID ForActorCreationTask(CActorID actor_id)

        @staticmethod
        CTaskID ForActorTask(CJobID job_id, CTaskID parent_task_id, int64_t parent_task_counter, CActorID actor_id)

        @staticmethod
        CTaskID ForNormalTask(CJobID job_id, CTaskID parent_task_id, int64_t parent_task_counter)

    cdef cppclass CObjectID" ray::ObjectID"(CBaseID[CObjectID]):

        @staticmethod
        int64_t MaxObjectIndex()

        @staticmethod
        CObjectID FromBinary(const c_string &binary)

        @staticmethod
        const CObjectID Nil()

        @staticmethod
        CObjectID ForPut(const CTaskID &task_id, int64_t index, int64_t transport_type);

        @staticmethod
        CObjectID ForTaskReturn(const CTaskID &task_id, int64_t index);

        @staticmethod
        size_t Size()

        c_bool is_put()

        int64_t ObjectIndex() const

        CTaskID TaskId() const

    cdef cppclass CWorkerID "ray::WorkerID"(CUniqueID):

        @staticmethod
        CWorkerID FromBinary(const c_string &binary)
