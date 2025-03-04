from libcpp.string cimport string as c_string
from libcpp cimport bool as c_bool
from libcpp.vector cimport vector as c_vector
from libcpp.unordered_map cimport unordered_map
from libcpp.memory cimport unique_ptr
from libc.stdint cimport (
  int32_t as c_int32_t,
  uint32_t as c_uint32_t,
  int64_t as c_int64_t,
)
from ray.includes.unique_ids cimport (
    CActorID,
    CJobID,
    CNodeID,
    CObjectID,
    CWorkerID,
    CPlacementGroupID,
    CVirtualClusterID,
)
from ray.includes.common cimport (
    CRayStatus,
    CGcsClientOptions,
)
from ray.includes.optional cimport (
    optional
)

cdef extern from "ray/gcs/gcs_client/global_state_accessor.h" nogil:
    cdef cppclass CGlobalStateAccessor "ray::gcs::GlobalStateAccessor":
        CGlobalStateAccessor(const CGcsClientOptions&)
        c_bool Connect()
        void Disconnect()
        c_vector[c_string] GetAllJobInfo(
          c_bool skip_submission_job_info_field, c_bool skip_is_running_tasks_field)
        CJobID GetNextJobID()
        c_vector[c_string] GetAllNodeInfo(const c_string virtual_cluster_id)
        c_vector[c_string] GetAllAvailableResources(const c_string virtual_cluster_id)
        c_vector[c_string] GetAllTotalResources(const c_string virtual_cluster_id)
        unordered_map[CNodeID, c_int64_t] GetDrainingNodes()
        unique_ptr[c_string] GetInternalKV(
          const c_string &namespace, const c_string &key)
        c_vector[c_string] GetAllTaskEvents()
        unique_ptr[c_string] GetObjectInfo(const CObjectID &object_id)
        unique_ptr[c_string] GetAllResourceUsage()
        c_vector[c_string] GetAllActorInfo(
            optional[CActorID], optional[CJobID], optional[c_string])
        unique_ptr[c_string] GetActorInfo(const CActorID &actor_id)
        unique_ptr[c_string] GetWorkerInfo(const CWorkerID &worker_id)
        c_vector[c_string] GetAllWorkerInfo()
        c_bool AddWorkerInfo(const c_string &serialized_string)
        c_bool UpdateWorkerDebuggerPort(const CWorkerID &worker_id,
                                        const c_uint32_t debuger_port)
        c_bool UpdateWorkerNumPausedThreads(const CWorkerID &worker_id,
                                            const c_int32_t num_paused_threads_delta)
        c_uint32_t GetWorkerDebuggerPort(const CWorkerID &worker_id)
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
        CRayStatus GetNode(
          const c_string &node_id_hex_str,
          c_string *node_info)

cdef extern from * namespace "ray::gcs" nogil:
    """
    #include <thread>
    #include "ray/gcs/gcs_server/store_client_kv.h"
    #include "ray/gcs/redis_client.h"
    #include "ray/gcs/store_client/redis_store_client.h"
    namespace ray {
    namespace gcs {

    bool RedisGetKeySync(const std::string& host,
                         int32_t port,
                         const std::string& username,
                         const std::string& password,
                         bool use_ssl,
                         const std::string& config,
                         const std::string& key,
                         std::string* data) {
      // Logging default value see class `RayLog`.
      InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                             ray::RayLog::ShutDownRayLog,
                                             "ray_init",
                                             ray::RayLogLevel::WARNING,
                                             /*log_filepath=*/"",
                                             /*err_log_filepath=*/"",
                                             /*log_rotation_max_size=*/1ULL << 29,
                                             /*log_rotation_file_num=*/10);

      RedisClientOptions options(host, port, username, password, use_ssl);

      std::string config_list;
      RAY_CHECK(absl::Base64Unescape(config, &config_list));
      RayConfig::instance().initialize(config_list);

      instrumented_io_context io_service;

      auto redis_client = std::make_shared<RedisClient>(options);
      auto status = redis_client->Connect(io_service);
      RAY_CHECK_OK(status) << "Failed to connect to redis.";

      auto cli = std::make_unique<StoreClientInternalKV>(
        std::make_unique<RedisStoreClient>(std::move(redis_client)));

      bool ret_val = false;
      cli->Get("session", key, {[&](std::optional<std::string> result) {
        if (result.has_value()) {
          *data = result.value();
          ret_val = true;
        } else {
          RAY_LOG(INFO) << "Failed to retrieve the key " << key
                        << " from persistent storage.";
          ret_val = false;
        }
      }, io_service});
      io_service.run_for(std::chrono::milliseconds(1000));

      return ret_val;
    }

    }
    }
    """
    c_bool RedisGetKeySync(const c_string& host,
                           c_int32_t port,
                           const c_string& username,
                           const c_string& password,
                           c_bool use_ssl,
                           const c_string& config,
                           const c_string& key,
                           c_string* data)


cdef extern from "ray/gcs/store_client/redis_store_client.h" namespace "ray::gcs" nogil:
    c_bool RedisDelKeyPrefixSync(const c_string& host,
                                 c_int32_t port,
                                 const c_string& username,
                                 const c_string& password,
                                 c_bool use_ssl,
                                 const c_string& key_prefix)
