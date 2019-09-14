from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string as c_string

from libc.stdint cimport uint8_t
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector

from ray.includes.unique_ids cimport (
    CJobID,
    CWorkerID,
    CObjectID,
    CTaskID,
)


cdef extern from "ray/common/status.h" namespace "ray" nogil:
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

        @staticmethod
        CRayStatus ObjectStoreFull()

        c_bool ok()
        c_bool IsOutOfMemory()
        c_bool IsKeyError()
        c_bool IsInvalid()
        c_bool IsIOError()
        c_bool IsTypeError()
        c_bool IsUnknownError()
        c_bool IsNotImplemented()
        c_bool IsRedisError()
        c_bool IsObjectStoreFull()

        c_string ToString()
        c_string CodeAsString()
        StatusCode code()
        c_string message()

    # We can later add more of the common status factory methods as needed
    cdef CRayStatus RayStatus_OK "Status::OK"()
    cdef CRayStatus RayStatus_Invalid "Status::Invalid"()


cdef extern from "ray/common/status.h" namespace "ray::StatusCode" nogil:
    cdef StatusCode StatusCode_OK "OK"
    cdef StatusCode StatusCode_OutOfMemory "OutOfMemory"
    cdef StatusCode StatusCode_KeyError "KeyError"
    cdef StatusCode StatusCode_TypeError "TypeError"
    cdef StatusCode StatusCode_Invalid "Invalid"
    cdef StatusCode StatusCode_IOError "IOError"
    cdef StatusCode StatusCode_UnknownError "UnknownError"
    cdef StatusCode StatusCode_NotImplemented "NotImplemented"
    cdef StatusCode StatusCode_RedisError "RedisError"


cdef extern from "ray/common/id.h" namespace "ray" nogil:
    const CTaskID GenerateTaskId(const CJobID &job_id,
                                 const CTaskID &parent_task_id,
                                 int parent_task_counter)


cdef extern from "ray/protobuf/common.pb.h" nogil:
    cdef cppclass CLanguage "Language":
        pass
    cdef cppclass CWorkerType "ray::WorkerType":
        pass


# This is a workaround for C++ enum class since Cython has no corresponding
# representation.
cdef extern from "ray/protobuf/common.pb.h" nogil:
    cdef CLanguage LANGUAGE_PYTHON "Language::PYTHON"
    cdef CLanguage LANGUAGE_CPP "Language::CPP"
    cdef CLanguage LANGUAGE_JAVA "Language::JAVA"

cdef extern from "ray/protobuf/common.pb.h" nogil:
    cdef CWorkerType WORKER_TYPE_WORKER "ray::WorkerType::WORKER"
    cdef CWorkerType WORKER_TYPE_DRIVER "ray::WorkerType::DRIVER"


cdef extern from "ray/common/task/scheduling_resources.h" nogil:
    cdef cppclass ResourceSet "ray::ResourceSet":
        ResourceSet()
        ResourceSet(const unordered_map[c_string, double] &resource_map)
        ResourceSet(const c_vector[c_string] &resource_labels,
                    const c_vector[double] resource_capacity)
        c_bool operator==(const ResourceSet &rhs) const
        c_bool IsEqual(const ResourceSet &other) const
        c_bool IsSubset(const ResourceSet &other) const
        c_bool IsSuperset(const ResourceSet &other) const
        c_bool AddOrUpdateResource(const c_string &resource_name,
                                   double capacity)
        c_bool RemoveResource(const c_string &resource_name)
        void AddResources(const ResourceSet &other)
        c_bool SubtractResourcesStrict(const ResourceSet &other)
        c_bool GetResource(const c_string &resource_name, double *value) const
        double GetNumCpus() const
        c_bool IsEmpty() const
        const unordered_map[c_string, double] &GetResourceMap() const
        const c_string ToString() const

cdef extern from "ray/common/buffer.h" namespace "ray" nogil:
    cdef cppclass CBuffer "ray::Buffer":
        uint8_t *Data() const
        size_t Size() const

    cdef cppclass LocalMemoryBuffer(CBuffer):
        LocalMemoryBuffer(uint8_t *data, size_t size)

cdef extern from "ray/core_worker/store_provider/store_provider.h" nogil:
    cdef cppclass CRayObject "ray::RayObject":
        const shared_ptr[CBuffer] &GetData()
        const size_t DataSize() const
        const shared_ptr[CBuffer] &GetMetadata() const
        c_bool HasData() const
        c_bool HasMetadata() const

cdef extern from "ray/gcs/gcs_client_interface.h" nogil:
    cdef cppclass CGcsClientOptions "ray::gcs::GcsClientOptions":
        CGcsClientOptions(const c_string &ip, int port,
                          const c_string &password,
                          c_bool is_test_client)
