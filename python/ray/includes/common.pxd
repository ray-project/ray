from libcpp.string cimport string as c_string
from libcpp cimport bool as c_bool

from libc.stdint cimport int64_t
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector

from ray.includes.unique_ids cimport (
    CDriverID,
    CObjectID,
    CTaskID,
)


cdef extern from "ray/status.h" namespace "ray" nogil:
    cdef cppclass StatusCode:
        pass

    cdef cppclass CRayStatus "ray::Status":
        RayStatus()
        RayStatus(StatusCode code, const c_string &msg)
        RayStatus(const CRayStatus &s)

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
    const CTaskID FinishTaskId(const CTaskID &task_id)
    const CObjectID ComputeReturnId(const CTaskID &task_id,
                                    int64_t return_index)
    const CObjectID ComputePutId(const CTaskID &task_id, int64_t put_index)
    const CTaskID ComputeTaskId(const CObjectID &object_id)
    const CTaskID GenerateTaskId(const CDriverID &driver_id,
                                 const CTaskID &parent_task_id,
                                 int parent_task_counter)
    int64_t ComputeObjectIndex(const CObjectID &object_id)


cdef extern from "ray/gcs/format/gcs_generated.h" nogil:
    cdef cppclass GCSArg "Arg":
        pass

    cdef cppclass CLanguage "Language":
        pass


# This is a workaround for C++ enum class since Cython has no corresponding
# representation.
cdef extern from "ray/gcs/format/gcs_generated.h" namespace "Language" nogil:
    cdef CLanguage LANGUAGE_PYTHON "Language::PYTHON"
    cdef CLanguage LANGUAGE_CPP "Language::CPP"
    cdef CLanguage LANGUAGE_JAVA "Language::JAVA"


cdef extern from "ray/raylet/scheduling_resources.h" \
        namespace "ray::raylet" nogil:
    cdef cppclass ResourceSet "ResourceSet":
        ResourceSet()
        ResourceSet(const unordered_map[c_string, double] &resource_map)
        ResourceSet(const c_vector[c_string] &resource_labels,
                    const c_vector[double] resource_capacity)
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
