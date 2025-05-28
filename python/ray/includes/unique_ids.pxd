from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libc.stdint cimport uint8_t, uint32_t, int64_t

# Note: we removed the staticmethod declarations in
# https://github.com/ray-project/ray/pull/47984 due
# to a compiler bug in Cython 3.0.x -- we should see
# if we can bring them back in Cython 3.1.x if the
# bug is fixed.
cdef extern from "ray/common/id.h" namespace "ray" nogil:
    cdef cppclass CBaseID[T]:
        size_t Hash() const
        c_bool IsNil() const
        c_bool operator==(const CBaseID &rhs) const
        c_bool operator!=(const CBaseID &rhs) const
        const uint8_t *data() const

        c_string Binary() const
        c_string Hex() const

    cdef cppclass CUniqueID "ray::UniqueID"(CBaseID[CUniqueID]):
        CUniqueID()

        @staticmethod
        size_t Size()

        @staticmethod
        CUniqueID FromRandom()

        @staticmethod
        CUniqueID FromBinary(const c_string &binary)

        @staticmethod
        const CUniqueID Nil()

    cdef cppclass CActorClassID "ray::ActorClassID"(CBaseID[CActorClassID]):

        @staticmethod
        CActorClassID FromHex(const c_string &hex_str)

    cdef cppclass CActorID "ray::ActorID"(CBaseID[CActorID]):

        @staticmethod
        CActorID FromBinary(const c_string &binary)

        @staticmethod
        CActorID FromHex(const c_string &hex_str)

        @staticmethod
        const CActorID Nil()

        @staticmethod
        size_t Size()

        @staticmethod
        CActorID Of(CJobID job_id, CTaskID parent_task_id,
                    int64_t parent_task_counter)

        CJobID JobId()

    cdef cppclass CNodeID "ray::NodeID"(CBaseID[CNodeID]):

        @staticmethod
        CNodeID FromHex(const c_string &hex_str)

        @staticmethod
        const CNodeID Nil()

    cdef cppclass CConfigID "ray::ConfigID"(CBaseID[CConfigID]):
        pass

    cdef cppclass CFunctionID "ray::FunctionID"(CBaseID[CFunctionID]):

        @staticmethod
        CFunctionID FromHex(const c_string &hex_str)

    cdef cppclass CJobID "ray::JobID"(CBaseID[CJobID]):

        @staticmethod
        CJobID FromBinary(const c_string &binary)

        @staticmethod
        CJobID FromHex(const c_string &hex_str)

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
        CTaskID FromHex(const c_string &hex_str)

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

    cdef cppclass CClusterID "ray::ClusterID"(CBaseID[CClusterID]):

        @staticmethod
        CClusterID FromHex(const c_string &hex_str)

        @staticmethod
        CClusterID FromRandom()

        @staticmethod
        const CClusterID Nil()

    cdef cppclass CWorkerID "ray::WorkerID"(CBaseID[CWorkerID]):

        @staticmethod
        CWorkerID FromHex(const c_string &hex_str)

    cdef cppclass CPlacementGroupID "ray::PlacementGroupID" \
                                    (CBaseID[CPlacementGroupID]):

        @staticmethod
        CPlacementGroupID FromBinary(const c_string &binary)

        @staticmethod
        CPlacementGroupID FromHex(const c_string &hex_str)

        @staticmethod
        const CPlacementGroupID Nil()

        @staticmethod
        size_t Size()

        @staticmethod
        CPlacementGroupID Of(CJobID job_id)

    ctypedef uint32_t ObjectIDIndexType
