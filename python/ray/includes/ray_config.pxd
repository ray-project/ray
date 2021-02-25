from libcpp cimport bool as c_bool
from libc.stdint cimport int64_t, uint64_t, uint32_t
from libcpp.string cimport string as c_string
from libcpp.unordered_map cimport unordered_map


cdef extern from "ray/common/ray_config.h" nogil:
    cdef cppclass RayConfig "RayConfig":
        @staticmethod
        RayConfig &instance()

        int64_t ray_cookie() const

        int64_t handler_warning_timeout_ms() const

        int64_t raylet_heartbeat_period_milliseconds() const

        int64_t debug_dump_period_milliseconds() const

        int64_t num_heartbeats_timeout() const

        uint64_t num_heartbeats_warning() const

        int64_t object_timeout_milliseconds() const

        int64_t get_timeout_milliseconds() const

        int64_t worker_get_request_size() const

        int64_t worker_fetch_request_size() const

        int64_t raylet_client_num_connect_attempts() const

        int64_t raylet_client_connect_timeout_milliseconds() const

        int64_t raylet_fetch_timeout_milliseconds() const

        int64_t kill_worker_timeout_milliseconds() const

        int64_t worker_register_timeout_seconds() const

        int64_t redis_db_connect_retries()

        int64_t redis_db_connect_wait_milliseconds() const

        int object_manager_pull_timeout_ms() const

        int object_manager_push_timeout_ms() const

        uint64_t object_manager_default_chunk_size() const

        uint32_t maximum_gcs_deletion_batch_size() const

        int64_t max_direct_call_object_size() const

        c_bool put_small_object_in_memory_store() const

        uint32_t max_tasks_in_flight_per_worker() const

        uint64_t metrics_report_interval_ms() const

        c_bool enable_timeline() const

        c_bool automatic_object_deletion_enabled() const

        uint32_t max_grpc_message_size() const
