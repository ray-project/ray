from libcpp.string cimport string as c_string
from libcpp cimport bool as c_bool
from libcpp.vector cimport vector as c_vector
from libcpp.memory cimport unique_ptr
from libc.stdint cimport int32_t as c_int32_t
from ray.includes.unique_ids cimport (
    CActorID,
    CJobID,
    CNodeID,
    CObjectID,
    CWorkerID,
    CPlacementGroupID,
)
from ray.includes.common cimport (
    CRayStatus,
    CGcsClientOptions,
)

cdef extern from "ray/gcs/gcs_client/global_state_accessor.h" nogil:
    cdef cppclass CGlobalStateAccessor "ray::gcs::GlobalStateAccessor":
        CGlobalStateAccessor(const CGcsClientOptions&)
        c_bool Connect()
        void Disconnect()
        c_vector[c_string] GetAllJobInfo()
        CJobID GetNextJobID()
        c_vector[c_string] GetAllNodeInfo()
        c_vector[c_string] GetAllAvailableResources()
        c_vector[c_string] GetAllTaskEvents()
        unique_ptr[c_string] GetObjectInfo(const CObjectID &object_id)
        unique_ptr[c_string] GetAllResourceUsage()
        c_vector[c_string] GetAllActorInfo()
        unique_ptr[c_string] GetActorInfo(const CActorID &actor_id)
        unique_ptr[c_string] GetWorkerInfo(const CWorkerID &worker_id)
        c_vector[c_string] GetAllWorkerInfo()
        c_bool AddWorkerInfo(const c_string &serialized_string)
        unique_ptr[c_string] GetPlacementGroupInfo(
            const CPlacementGroupID &placement_group_id)
        unique_ptr[c_string] GetPlacementGroupByName(
            const c_string &placement_group_name,
            const c_string &ray_namespace,
        )
        c_vector[c_string] GetAllPlacementGroupInfo()
        c_string GetSystemConfig()
        CRayStatus GetNodeToConnectForDriver(
            const c_string &node_ip_address,
            c_string *node_to_connect)

cdef extern from * namespace "ray::gcs" nogil:
    """
    #include <thread>
    #include "ray/gcs/redis_client.h"
    namespace ray {
    namespace gcs {

    class Cleanup {
      public:
        Cleanup(std::function<void()> f): f_(f) {}
        ~Cleanup() { f_(); }
      private:
        std::function<void()> f_;
    };

    bool RedisDelKeySync(const std::string& host,
                         int32_t port,
                         const std::string& password,
                         bool use_ssl,
                         const std::string& key) {
      RedisClientOptions options(host, port, password, false, use_ssl);
      auto cli = std::make_unique<RedisClient>(options);

      instrumented_io_context io_service;

      auto thread = std::make_unique<std::thread>([&]() {
        boost::asio::io_service::work work(io_service);
        io_service.run();
      });

      Cleanup _([&](){
        io_service.stop();
        thread->join();
      });

      auto status = cli->Connect(io_service);
      if(!status.ok()) {
        RAY_LOG(ERROR) << "Failed to connect to redis: " << status.ToString();
        return false;
      }

      auto context = cli->GetShardContext(key);
      auto cmd = std::vector<std::string>{"DEL", key};
      auto reply = context->RunArgvSync(cmd);
      if(reply->ReadAsInteger() == 1) {
        RAY_LOG(INFO) << "Successfully deleted " << key;
        return true;
      } else {
        RAY_LOG(ERROR) << "Failed to delete " << key;
        return false;
      }
    }

    }
    }
    """
    c_bool RedisDelKeySync(const c_string& host,
                           c_int32_t port,
                           const c_string& password,
                           c_bool use_ssl,
                           const c_string& key)
