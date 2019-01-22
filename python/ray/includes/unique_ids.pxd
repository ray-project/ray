from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libc.stdint cimport uint8_t

cdef extern from "ray/id.h" namespace "ray" nogil:
    cdef cppclass CUniqueID "ray::UniqueID":
        CUniqueID()
        CUniqueID(const CUniqueID &from_id)
        @staticmethod
        CUniqueID from_random()
        @staticmethod
        CUniqueID from_binary(const c_string & binary)
        @staticmethod
        const CUniqueID nil()
        size_t hash() const
        c_bool is_nil() const
        c_bool operator==(const CUniqueID& rhs) const
        c_bool operator!=(const CUniqueID& rhs) const
        const uint8_t *data() const
        uint8_t *mutable_data();
        size_t size() const;
        c_string binary() const;
        c_string hex() const;

    ctypedef CUniqueID TaskID
    ctypedef CUniqueID ObjectID
    ctypedef CUniqueID JobID
    ctypedef CUniqueID FunctionID
    ctypedef CUniqueID ClassID
    ctypedef CUniqueID ActorID
    ctypedef CUniqueID ActorHandleID
    ctypedef CUniqueID WorkerID
    ctypedef CUniqueID DriverID
    ctypedef CUniqueID ConfigID
    ctypedef CUniqueID ClientID
