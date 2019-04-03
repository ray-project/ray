from libc.stdint cimport int64_t, uint64_t, uint32_t
from libcpp.string cimport string as c_string
from libcpp.unordered_map cimport unordered_map


cdef extern from "ray/ray_config.h" nogil:
    cdef cppclass RayConfig "RayConfig":
        @staticmethod
        RayConfig &instance()

        int64_t ray_cookie() const

        int64_t handler_warning_timeout_ms() const

        int64_t heartbeat_timeout_milliseconds() const

        int64_t debug_dump_period_milliseconds() const

        int64_t num_heartbeats_timeout() const

        uint64_t num_heartbeats_warning() const

        int64_t initial_reconstruction_timeout_milliseconds() const

        int64_t get_timeout_milliseconds() const

        uint64_t max_lineage_size() const

        int64_t worker_get_request_size() const

        int64_t worker_fetch_request_size() const

        int64_t actor_max_dummy_objects() const

        int64_t num_connect_attempts() const

        int64_t connect_timeout_milliseconds() const

        int64_t local_scheduler_fetch_timeout_milliseconds() const

        int64_t local_scheduler_reconstruction_timeout_milliseconds() const

        int64_t max_num_to_reconstruct() const

        int64_t local_scheduler_fetch_request_size() const

        int64_t kill_worker_timeout_milliseconds() const

        int64_t max_time_for_handler_milliseconds() const

        int64_t size_limit() const

        int64_t num_elements_limit() const

        int64_t max_time_for_loop() const

        int64_t redis_db_connect_retries()

        int64_t redis_db_connect_wait_milliseconds() const

        int64_t plasma_default_release_delay() const

        int64_t L3_cache_size_bytes() const

        int64_t max_tasks_to_spillback() const

        int64_t actor_creation_num_spillbacks_warning() const

        int node_manager_forward_task_retry_timeout_milliseconds() const

        int object_manager_pull_timeout_ms() const

        int object_manager_push_timeout_ms() const

        int object_manager_repeated_push_delay_ms() const

        uint64_t object_manager_default_chunk_size() const

        int num_workers_per_process() const

        int64_t max_task_lease_timeout_ms() const

        uint32_t num_actor_checkpoints_to_keep() const

        uint32_t maximum_gcs_deletion_batch_size() const

        void initialize(const unordered_map[c_string, int] &config_map)
