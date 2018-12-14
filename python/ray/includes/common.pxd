from libcpp.string cimport string as c_string
from libcpp cimport bool as c_bool, nullptr

from libcpp.unordered_set cimport unordered_set as c_unordered_set
from libc.stdint cimport int64_t, int32_t, uint8_t, uintptr_t
from cpython cimport PyObject
cimport cpython


cdef extern from "ray/api.h" namespace "ray" nogil:
    cdef cppclass StatusCode;
    # We can later add more of the common status factory methods as needed
    cdef RayStatus RayStatus_OK "Status::OK"()
    cdef RayStatus RayStatus_Invalid "Status::Invalid"()

    cdef cppclass RayStatus "ray::Status":
        RayStatus()
        RayStatus(StatusCode code, const c_string &msg)
        RayStatus(const RayStatus &s);

        @staticmethod
        RayStatus OK()
        @staticmethod
        RayStatus OutOfMemory()
        @staticmethod
        RayStatus KeyError()
        @staticmethod
        RayStatus Invalid()
        @staticmethod
        RayStatus IOError()
        @staticmethod
        RayStatus TypeError()
        @staticmethod
        RayStatus UnknownError()
        @staticmethod
        RayStatus NotImplemented()
        @staticmethod
        RayStatus RedisError()

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

    cdef cppclass UniqueID "ray::UniqueID":
        # TODO: Add Plasma UniqueID support.
        UniqueID()
        @staticmethod
        UniqueID from_random()
        UniqueID from_binary(const c_string & binary)
        @staticmethod
        const UniqueID nil()
        size_t hash() const
        c_bool is_nil() const
        const uint8_t *data() const
        uint8_t *mutable_data();
        size_t size() const;
        c_string binary() const;
        c_string hex() const;

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
                                   int64_t return_index)
    const ObjectID ComputePutId(const TaskID & task_id, int64_t put_index)
    const TaskID ComputeTaskId(const ObjectID & object_id)
    const TaskID GenerateTaskId(const DriverID & driver_id,
                                const TaskID & parent_task_id,
                                int parent_task_counter)
    int64_t ComputeObjectIndex(const ObjectID & object_id)


cdef extern from "ray/api.h" namespace "ray::StatusCode" nogil:
    cdef StatusCode OK
    cdef StatusCode OutOfMemory
    cdef StatusCode KeyError
    cdef StatusCode TypeError
    cdef StatusCode Invalid
    cdef StatusCode IOError
    cdef StatusCode UnknownError
    cdef StatusCode NotImplemented
    cdef StatusCode RedisError


cdef extern from "ray/api.h" namespace "ray::raylet" nogil:
    cdef cppclass RayletTaskSpecification "ray::raylet::TaskSpecification"


cdef extern from "ray/gcs/format/gcs_generated.h" nogil:
    cdef cppclass Language "Language"
    cdef struct ProfileTableDataT

cdef extern from "ray/gcs/format/gcs_generated.h" namespace "Language" nogil:
    cdef Language PYTHON
    cdef Language CPP
    cdef Language JAVA

cdef inline object PyObject_to_object(PyObject*o):
    # Cast to "object" increments reference count
    cdef object result = <object> o
    cpython.Py_DECREF(result)
    return result
