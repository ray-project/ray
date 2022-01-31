from ray.includes.ray_config cimport RayConfig

cdef class Config:
    @staticmethod
    def ray_cookie():
        return RayConfig.instance().ray_cookie()

    @staticmethod
    def handler_warning_timeout_ms():
        return RayConfig.instance().handler_warning_timeout_ms()

    @staticmethod
    def raylet_heartbeat_period_milliseconds():
        return RayConfig.instance().raylet_heartbeat_period_milliseconds()

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
    def object_timeout_milliseconds():
        return (RayConfig.instance()
                .object_timeout_milliseconds())

    @staticmethod
    def get_timeout_milliseconds():
        return RayConfig.instance().get_timeout_milliseconds()

    @staticmethod
    def worker_get_request_size():
        return RayConfig.instance().worker_get_request_size()

    @staticmethod
    def worker_fetch_request_size():
        return RayConfig.instance().worker_fetch_request_size()

    @staticmethod
    def raylet_client_num_connect_attempts():
        return RayConfig.instance().raylet_client_num_connect_attempts()

    @staticmethod
    def raylet_client_connect_timeout_milliseconds():
        return (RayConfig.instance()
                .raylet_client_connect_timeout_milliseconds())

    @staticmethod
    def raylet_fetch_timeout_milliseconds():
        return (RayConfig.instance()
                .raylet_fetch_timeout_milliseconds())

    @staticmethod
    def kill_worker_timeout_milliseconds():
        return RayConfig.instance().kill_worker_timeout_milliseconds()

    @staticmethod
    def worker_register_timeout_seconds():
        return RayConfig.instance().worker_register_timeout_seconds()

    @staticmethod
    def redis_db_connect_retries():
        return RayConfig.instance().redis_db_connect_retries()

    @staticmethod
    def redis_db_connect_wait_milliseconds():
        return RayConfig.instance().redis_db_connect_wait_milliseconds()

    @staticmethod
    def object_manager_pull_timeout_ms():
        return RayConfig.instance().object_manager_pull_timeout_ms()

    @staticmethod
    def object_manager_push_timeout_ms():
        return RayConfig.instance().object_manager_push_timeout_ms()

    @staticmethod
    def object_manager_default_chunk_size():
        return RayConfig.instance().object_manager_default_chunk_size()

    @staticmethod
    def maximum_gcs_deletion_batch_size():
        return RayConfig.instance().maximum_gcs_deletion_batch_size()

    @staticmethod
    def metrics_report_interval_ms():
        return RayConfig.instance().metrics_report_interval_ms()

    @staticmethod
    def enable_timeline():
        return RayConfig.instance().enable_timeline()

    @staticmethod
    def max_grpc_message_size():
        return RayConfig.instance().max_grpc_message_size()

    @staticmethod
    def record_ref_creation_sites():
        return RayConfig.instance().record_ref_creation_sites()

    @staticmethod
    def gcs_grpc_based_pubsub():
        return RayConfig.instance().gcs_grpc_based_pubsub()

    @staticmethod
    def bootstrap_with_gcs():
        return RayConfig.instance().bootstrap_with_gcs()
