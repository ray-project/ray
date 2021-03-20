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
)
from cython.operator cimport dereference
from libcpp cimport nullptr

from ray.includes.gcs_client cimport (
    CKVAccessor,
    CGcsClient,
    make_gcs,
)

import threading

cdef class GcsClient:
    cdef:
        shared_ptr[CGcsClient] inner_

    @staticmethod
    cdef make_from_address(
      const c_string &ip,
      int port,
      const c_string &password):
        cdef GcsClient self = GcsClient.__new__(GcsClient)
        self.inner_ = make_gcs(ip, port, password)
        return self

    @staticmethod
    cdef make_from_existing(const shared_ptr[CGcsClient]& gcs_client):
        cdef GcsClient self = GcsClient.__new__(GcsClient)
        self.inner_ = gcs_client
        return self

    def kv_put(self, c_string key, c_string value, c_bool overwrite):
        cdef c_bool added = False
        status = self.inner_.get().KV().Put(key, value, overwrite, added)
        if not status.ok():
            raise IOError("Put failed: {}".format(status.ToString()))
        return added


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
        elif not status.ok():
            raise IOError("Get failed: {}".format(status.ToString()))
        return value if exists else None

    def kv_keys(self, c_string key):
        cdef:
            c_vector[c_string] results
        status = self.inner_.get().KV().Keys(key, results)
        if not status.ok():
            raise IOError("Keys failed: {}".format(status.ToString()))
        return results

    def kv_exists(self, c_string key):
        cdef:
            c_bool exist = False
        status = self.inner_.get().KV().Exists(key, exist)
        if not status.ok():
            raise IOError("Exists failed: {}".format(status.ToString()))
        return exist
