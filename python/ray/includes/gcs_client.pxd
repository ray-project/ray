from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string as c_string

from libc.stdint cimport uint8_t, int32_t, uint64_t, int64_t
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector
from libcpp.pair cimport pair as c_pair

from ray.includes.common cimport (
    CRayStatus,
    CGcsClientOptions,
)

cdef extern from "ray/gcs/accessor.h" nogil:
    cdef cppclass CInternalKVAccessor "ray::gcs::InternalKVAccessor":
        CRayStatus Put(const c_string &key,
                       const c_string &value,
                       c_bool overwrite,
                       c_bool &updated)
        CRayStatus Get(const c_string &key, c_string &value)
        CRayStatus Del(const c_string &key)
        CRayStatus Exists(const c_string &key, c_bool &exist)
        CRayStatus Keys(const c_string &key, c_vector[c_string]& results)

cdef extern from "ray/gcs/gcs_client.h" nogil:
    cdef cppclass CGcsClient "ray::gcs::GcsClient":
        CInternalKVAccessor &InternalKV()


cdef extern from * namespace "_gcs_maker":
    """
    #include "ray/common/asio/instrumented_io_context.h"
    #include "ray/gcs/gcs_client/service_based_gcs_client.h"
    #include "ray/common/asio/instrumented_io_context.h"
    #include <memory>
    #include <thread>
    namespace _gcs_maker {
      class RayletGcsClient : public ray::gcs::ServiceBasedGcsClient {
       public:
        RayletGcsClient(const ray::gcs::GcsClientOptions &options)
            : ray::gcs::ServiceBasedGcsClient(options),
              work_(io_context_),
              thread_([this](){
                  io_context_.run();
              }) {
           RAY_CHECK_OK(Connect(io_context_));
        }
        ~RayletGcsClient() {
          RAY_LOG(DEBUG)
            << "Destructing GCS client and associated event loop thread.";
          Disconnect();
          io_context_.stop();
          thread_.join();
        }
       private:
        instrumented_io_context io_context_;
        boost::asio::io_service::work work_;
        std::thread thread_;
      };
      std::shared_ptr<ray::gcs::GcsClient> make_gcs(
          const std::string& ip,
          int port,
          const std::string& password) {
        return std::make_shared<RayletGcsClient>(
            ray::gcs::GcsClientOptions(ip, port, password));
      }
    }
    """
    shared_ptr[CGcsClient] make_gcs(
        const c_string &ip, int port, const c_string &password)
