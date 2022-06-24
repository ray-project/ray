from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libc.stdint cimport uint8_t, uint32_t, int64_t

cdef extern from "ray/common/id.h" namespace "ray" nogil:
    cdef cppclass CBaseID[T]:
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
        const uint8_t *data() const

        c_string Binary() const
        c_string Hex() const

    cdef cppclass CUniqueID "ray::UniqueID"(CBaseID):
        CUniqueID()

        @staticmethod
        size_t Size()

        @staticmethod
        CUniqueID FromRandom()

        @staticmethod
        CUniqueID FromBinary(const c_string &binary)

        @staticmethod
        const CUniqueID Nil()

        @staticmethod
        size_t Size()

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
        CActorID Of(CJobID job_id, CTaskID parent_task_id,
                    int64_t parent_task_counter)

        CJobID JobId()

    cdef cppclass CNodeID "ray::NodeID"(CUniqueID):

        @staticmethod
        CNodeID FromBinary(const c_string &binary)

        @staticmethod
        CNodeID FromHex(const c_string &hex_str)

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

        uint32_t ToInt()

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
        CTaskID FromRandom(const CJobID &job_id)

        @staticmethod
        CTaskID ForActorCreationTask(CActorID actor_id)

        @staticmethod
        CTaskID ForActorTask(CJobID job_id, CTaskID parent_task_id,
                             int64_t parent_task_counter, CActorID actor_id)

        @staticmethod
        CTaskID ForNormalTask(CJobID job_id, CTaskID parent_task_id,
                              int64_t parent_task_counter)

        CActorID ActorId() const

        CJobID JobId() const

    cdef cppclass CObjectID" ray::ObjectID"(CBaseID[CObjectID]):

        @staticmethod
        int64_t MaxObjectIndex()

        @staticmethod
        CObjectID FromBinary(const c_string &binary)

        @staticmethod
        CObjectID FromRandom()

        @staticmethod
        const CObjectID Nil()

        @staticmethod
        CObjectID FromIndex(const CTaskID &task_id, int64_t index)

        @staticmethod
        size_t Size()

        c_bool is_put()

        int64_t ObjectIndex() const

        CTaskID TaskId() const

    cdef cppclass CWorkerID "ray::WorkerID"(CUniqueID):

        @staticmethod
        CWorkerID FromBinary(const c_string &binary)

    cdef cppclass CPlacementGroupID "ray::PlacementGroupID" \
                                    (CBaseID[CPlacementGroupID]):

        @staticmethod
        CPlacementGroupID FromBinary(const c_string &binary)

        @staticmethod
        const CPlacementGroupID Nil()

        @staticmethod
        size_t Size()

        @staticmethod
        CPlacementGroupID Of(CJobID job_id)
