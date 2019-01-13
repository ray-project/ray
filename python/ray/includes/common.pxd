from libcpp.string cimport string as c_string
from libcpp cimport bool as c_bool

from libc.stdint cimport int64_t as c_int64, uint32_t as c_uint32, uint16_t as c_uint16, uint8_t as c_uint8
from libcpp.memory cimport unique_ptr
from libcpp.vector cimport vector as c_vector
from libcpp.unordered_map cimport unordered_map
from cpython cimport PyObject
cimport cpython

ctypedef c_uint32 uoffset_t
ctypedef c_uint16 voffset_t

cdef extern from "flatbuffers/flatbuffers.h" namespace "flatbuffers":
    # Cython cannot rename a template class.
    cdef cppclass Offset[T]:
        uoffset_t o
        Offset()
        Offset(T) except +
        Offset[void] Union() const

    cdef cppclass FlatBufferString "flatbuffers::String":
        const char *c_str() const
        c_string str()
        c_bool operator<(const FlatBufferString &o) const

    cdef T GetRoot[T](void *buf)

    cdef cppclass FlatBufferBuilder:
        FlatBufferBuilder() except +
        void Reset()
        uoffset_t GetSize()
        void Clear()
        c_uint8 *GetBufferPointer()
        c_uint8 *GetCurrentBufferPointer() const
        void Finish[T](T root)
        Offset[FlatBufferString] CreateString(char *str, size_t len)
        void StartVector(size_t len, size_t elemsize)
        uoffset_t EndVector(size_t len)
        uoffset_t StartTable()
        uoffset_t EndTable(uoffset_t start, voffset_t numfields)


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
        # TODO(?): Add Plasma UniqueID support.
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


cdef extern from "ray/gcs/format/gcs_generated.h" nogil:
    cdef cppclass GCSArg "Arg":
        pass

    # Note: This is a C++ enum class.
    cdef cppclass CLanguage "Language":
        pass

    # Note: It is a C++ struct in the header file.
    cdef cppclass GCSProfileEventT "ProfileEventT":
        c_string event_type
        double start_time
        double end_time
        c_string extra_data
        GCSProfileEventT()

    # Note: It is a C++ struct in the header file.
    cdef cppclass GCSProfileTableDataT "ProfileTableDataT":
        c_string component_type
        c_string component_id
        c_string node_ip_address
        c_vector[unique_ptr[GCSProfileEventT]] profile_events
        GCSProfileTableDataT()


# This is a workaround for C++ enum class since Cython has no corresponding representation.
cdef extern from "ray/gcs/format/gcs_generated.h" namespace "Language" nogil:
    cdef CLanguage LANGUAGE_PYTHON "Language::PYTHON"
    cdef CLanguage LANGUAGE_CPP "Language::CPP"
    cdef CLanguage LANGUAGE_JAVA "Language::JAVA"


cdef extern from "ray/raylet/scheduling_resources.h" namespace "ray::raylet" nogil:
    cdef cppclass ResourceSet "ResourceSet":
        ResourceSet()
        ResourceSet(const unordered_map[c_string, double] &resource_map)
        ResourceSet(const c_vector[c_string] &resource_labels, const c_vector[double] resource_capacity)
        c_bool operator==(const ResourceSet &rhs) const
        c_bool IsEqual(const ResourceSet &other) const
        c_bool IsSubset(const ResourceSet &other) const
        c_bool IsSuperset(const ResourceSet &other) const
        c_bool AddResource(const c_string &resource_name, double capacity)
        c_bool RemoveResource(const c_string &resource_name)
        c_bool AddResourcesStrict(const ResourceSet &other)
        void AddResources(const ResourceSet &other)
        c_bool SubtractResourcesStrict(const ResourceSet &other)
        c_bool GetResource(const c_string &resource_name, double *value) const
        double GetNumCpus() const
        c_bool IsEmpty() const
        const unordered_map[c_string, double] &GetResourceMap() const
        const c_string ToString() const


cdef inline object PyObject_to_object(PyObject*o):
    # Cast to "object" increments reference count
    cdef object result = <object> o
    cpython.Py_DECREF(result)
    return result
