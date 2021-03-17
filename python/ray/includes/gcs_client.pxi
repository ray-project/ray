from libcpp.memory cimport (
    shared_ptr,
    unique_ptr,
    make_unique,
    make_shared,
    static_pointer_cast
)
from ray.includes.common cimport (
    CGcsClientOptions,
    CRayStatus,
    check_status,
)
from cython.operator cimport dereference
from libcpp cimport nullptr

from ray.includes.gcs_client cimport (
    CKVAccessor,
    CServiceBasedGcsClient,
    CGcsClient,
    c_instrumented_io_context
)


cdef class GcsClient:
    cdef:
        unique_ptr[c_instrumented_io_context] io_context_
        shared_ptr[CGcsClient] inner_

    @staticmethod
    cdef make_from_existing(const shared_ptr[CGcsClient]& gcs_client):
        cdef GcsClient self = GcsClient.__new__(GcsClient)
        self.inner_ = gcs_client
        return self

    @staticmethod
    cdef make_from_addr(ip, port, password, is_test_client):
        cdef GcsClient self = GcsClient.__new__(GcsClient)
        # cdef GcsClientOptions options = CGcsClientOptions(ip, port, password, is_test_client)
        # self.inner_ = static_pointer_cast[CGcsClient, CServiceBasedGcsClient](
        #     make_shared[CServiceBasedGcsClient]())
        # self.io_context_ = make_unique[c_instrumented_io_context]()
        # dereference(self.inner_).Connect(dereference(self.io_context_))

    def __dealloc__(self):
        self.inner_.reset()
        self.io_context_.reset()


    def kv_put(self, c_string key, c_string value):
        status = self.inner_.get().KV().Put(key, value)
        if not status.ok():
            raise IOError("Put failed: {}".format(status.ToString()))

    def kv_del(self, c_string key):
        status = self.inner_.get().KV().Del(key)
        if not status.ok():
            raise IOError("Del failed: {}".format(status.ToString()))

    def kv_get(self, c_string key):
        cdef:
            c_string value
            c_bool exists = True
        status = self.inner_.get().KV().Get(key, value)
        if status.IsNotFound():
            exists = False
        else:
            raise IOError("Get failed: {}".format(status.ToString()))
        return value if exists else None

    def kv_exists(self, c_string key):
        cdef:
            c_bool exist
        status = self.inner_.get().KV().Exists(key, exist)
        if not status.ok():
            raise IOError("Exists failed: {}".format(status.ToString()))
        return exist
