from libcpp.string cimport string as c_string
from libcpp cimport bool as c_bool

from libc.stdint cimport int64_t as c_int64, uint8_t as c_uint8
from cpython cimport PyObject
cimport cpython


cdef extern from "ray/constants.h" nogil:
    cdef c_int64 kUniqueIDSize
    cdef c_int64 kMaxTaskPuts


cdef extern from "ray/status.h" namespace "ray" nogil:
    cdef cppclass StatusCode:
         pass

    cdef cppclass CRayStatus "ray::Status":
        RayStatus()
        RayStatus(StatusCode code, const c_string &msg)
        RayStatus(const CRayStatus &s);

        @staticmethod
        CRayStatus OK()
        @staticmethod
        CRayStatus OutOfMemory()
        @staticmethod
        CRayStatus KeyError()
        @staticmethod
        CRayStatus Invalid()
        @staticmethod
        CRayStatus IOError()
        @staticmethod
        CRayStatus TypeError()
        @staticmethod
        CRayStatus UnknownError()
        @staticmethod
        CRayStatus NotImplemented()
        @staticmethod
        CRayStatus RedisError()

        c_bool ok()
        c_bool IsOutOfMemory()
        c_bool IsKeyError()
        c_bool IsInvalid()
        c_bool IsIOError()
        c_bool IsTypeError()
        c_bool IsUnknownError()
        c_bool IsNotImplemented()
        c_bool IsRedisError()

        c_string ToString()
        c_string CodeAsString()
        StatusCode code()
        c_string message()

    # We can later add more of the common status factory methods as needed
    cdef CRayStatus RayStatus_OK "Status::OK"()
    cdef CRayStatus RayStatus_Invalid "Status::Invalid"()


cdef extern from "ray/status.h" namespace "ray::StatusCode" nogil:
    cdef StatusCode StatusCode_OK "OK"
    cdef StatusCode StatusCode_OutOfMemory "OutOfMemory"
    cdef StatusCode StatusCode_KeyError "KeyError"
    cdef StatusCode StatusCode_TypeError "TypeError"
    cdef StatusCode StatusCode_Invalid "Invalid"
    cdef StatusCode StatusCode_IOError "IOError"
    cdef StatusCode StatusCode_UnknownError "UnknownError"
    cdef StatusCode StatusCode_NotImplemented "NotImplemented"
    cdef StatusCode StatusCode_RedisError "RedisError"


cdef extern from "ray/id.h" namespace "ray" nogil:
    cdef cppclass UniqueID "ray::UniqueID":
        # TODO: Add Plasma UniqueID support.
        UniqueID()
        UniqueID(const UniqueID &from_id)
        @staticmethod
        UniqueID from_random()
        @staticmethod
        UniqueID from_binary(const c_string & binary)
        @staticmethod
        const UniqueID nil()
        size_t hash() const
        c_bool is_nil() const
        c_bool operator==(const UniqueID& rhs) const
        c_bool operator!=(const UniqueID& rhs) const
        const c_uint8 *data() const
        c_uint8 *mutable_data();
        size_t size() const;
        c_string binary() const;
        c_string hex() const;

    # Cython cannot rename typedef. We have to use the original name in C++.
    ctypedef UniqueID TaskID
    ctypedef UniqueID JobID
    ctypedef UniqueID ObjectID
    ctypedef UniqueID FunctionID
    ctypedef UniqueID ClassID
    ctypedef UniqueID ActorID
    ctypedef UniqueID ActorHandleID
    ctypedef UniqueID WorkerID
    ctypedef UniqueID DriverID
    ctypedef UniqueID ConfigID
    ctypedef UniqueID ClientID

    const TaskID FinishTaskId(const TaskID & task_id)
    const ObjectID ComputeReturnId(const TaskID & task_id,
                                   c_int64 return_index)
    const ObjectID ComputePutId(const TaskID & task_id, c_int64 put_index)
    const TaskID ComputeTaskId(const ObjectID & object_id)
    const TaskID GenerateTaskId(const DriverID & driver_id,
                                const TaskID & parent_task_id,
                                int parent_task_counter)
    c_int64 ComputeObjectIndex(const ObjectID & object_id)


cdef extern from "ray/raylet/task_spec.h" namespace "ray::raylet" nogil:
    cdef cppclass RayletTaskSpecification "ray::raylet::TaskSpecification":
        RayletTaskSpecification(const c_string &string)


cdef extern from "ray/gcs/format/gcs_generated.h" nogil:
    cdef cppclass CLanguage "Language":
        pass
    cdef struct GCSProfileTableDataT "ProfileTableDataT":
        GCSProfileTableDataT()


cdef extern from "ray/gcs/format/gcs_generated.h" namespace "Language" nogil:
    cdef CLanguage LANGUAGE_PYTHON "Language::PYTHON"
    cdef CLanguage LANGUAGE_CPP "Language::CPP"
    cdef CLanguage LANGUAGE_JAVA "Language::JAVA"


cdef inline object PyObject_to_object(PyObject*o):
    # Cast to "object" increments reference count
    cdef object result = <object> o
    cpython.Py_DECREF(result)
    return result
