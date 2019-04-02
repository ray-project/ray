from ray.includes.ray_config cimport RayConfig

cdef class Config:
    @staticmethod
    def ray_cookie():
        return RayConfig.instance().ray_cookie()

    @staticmethod
    def handler_warning_timeout_ms():
        return RayConfig.instance().handler_warning_timeout_ms()

    @staticmethod
    def heartbeat_timeout_milliseconds():
        return RayConfig.instance().heartbeat_timeout_milliseconds()

    @staticmethod
    def debug_dump_period_milliseconds():
        return RayConfig.instance().debug_dump_period_milliseconds()

    @staticmethod
    def num_heartbeats_timeout():
        return RayConfig.instance().num_heartbeats_timeout()

    @staticmethod
    def num_heartbeats_warning():
        return RayConfig.instance().num_heartbeats_warning()

    @staticmethod
    def initial_reconstruction_timeout_milliseconds():
        return (RayConfig.instance()
                .initial_reconstruction_timeout_milliseconds())

    @staticmethod
    def get_timeout_milliseconds():
        return RayConfig.instance().get_timeout_milliseconds()

    @staticmethod
    def max_lineage_size():
        return RayConfig.instance().max_lineage_size()

    @staticmethod
    def worker_get_request_size():
        return RayConfig.instance().worker_get_request_size()

    @staticmethod
    def worker_fetch_request_size():
        return RayConfig.instance().worker_fetch_request_size()

    @staticmethod
    def actor_max_dummy_objects():
        return RayConfig.instance().actor_max_dummy_objects()

    @staticmethod
    def num_connect_attempts():
        return RayConfig.instance().num_connect_attempts()

    @staticmethod
    def connect_timeout_milliseconds():
        return RayConfig.instance().connect_timeout_milliseconds()

    @staticmethod
    def local_scheduler_fetch_timeout_milliseconds():
        return (RayConfig.instance()
                .local_scheduler_fetch_timeout_milliseconds())

    @staticmethod
    def local_scheduler_reconstruction_timeout_milliseconds():
        return (RayConfig.instance()
                .local_scheduler_reconstruction_timeout_milliseconds())

    @staticmethod
    def max_num_to_reconstruct():
        return RayConfig.instance().max_num_to_reconstruct()

    @staticmethod
    def local_scheduler_fetch_request_size():
        return RayConfig.instance().local_scheduler_fetch_request_size()

    @staticmethod
    def kill_worker_timeout_milliseconds():
        return RayConfig.instance().kill_worker_timeout_milliseconds()

    @staticmethod
    def max_time_for_handler_milliseconds():
        return RayConfig.instance().max_time_for_handler_milliseconds()

    @staticmethod
    def size_limit():
        return RayConfig.instance().size_limit()

    @staticmethod
    def num_elements_limit():
        return RayConfig.instance().num_elements_limit()

    @staticmethod
    def max_time_for_loop():
        return RayConfig.instance().max_time_for_loop()

    @staticmethod
    def redis_db_connect_retries():
        return RayConfig.instance().redis_db_connect_retries()

    @staticmethod
    def redis_db_connect_wait_milliseconds():
        return RayConfig.instance().redis_db_connect_wait_milliseconds()

    @staticmethod
    def plasma_default_release_delay():
        return RayConfig.instance().plasma_default_release_delay()

    @staticmethod
    def L3_cache_size_bytes():
        return RayConfig.instance().L3_cache_size_bytes()

    @staticmethod
    def max_tasks_to_spillback():
        return RayConfig.instance().max_tasks_to_spillback()

    @staticmethod
    def actor_creation_num_spillbacks_warning():
        return RayConfig.instance().actor_creation_num_spillbacks_warning()

    @staticmethod
    def node_manager_forward_task_retry_timeout_milliseconds():
        return (RayConfig.instance()
                .node_manager_forward_task_retry_timeout_milliseconds())

    @staticmethod
    def object_manager_pull_timeout_ms():
        return RayConfig.instance().object_manager_pull_timeout_ms()

    @staticmethod
    def object_manager_push_timeout_ms():
        return RayConfig.instance().object_manager_push_timeout_ms()

    @staticmethod
    def object_manager_repeated_push_delay_ms():
        return RayConfig.instance().object_manager_repeated_push_delay_ms()

    @staticmethod
    def object_manager_default_chunk_size():
        return RayConfig.instance().object_manager_default_chunk_size()

    @staticmethod
    def num_workers_per_process():
        return RayConfig.instance().num_workers_per_process()

    @staticmethod
    def max_task_lease_timeout_ms():
        return RayConfig.instance().max_task_lease_timeout_ms()

    @staticmethod
    def num_actor_checkpoints_to_keep():
        return RayConfig.instance().num_actor_checkpoints_to_keep()

    @staticmethod
    def maximum_gcs_deletion_batch_size():
        return RayConfig.instance().maximum_gcs_deletion_batch_size()
