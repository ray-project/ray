from libcpp.string cimport string as c_string
from ray.includes.ray_config cimport RayConfig

cdef class Config:
    @staticmethod
    def initialize(c_string config_list):
        return RayConfig.instance().initialize(config_list)

    @staticmethod
    def ray_cookie():
        return RayConfig.instance().ray_cookie()

    @staticmethod
    def handler_warning_timeout_ms():
        return RayConfig.instance().handler_warning_timeout_ms()

    @staticmethod
    def debug_dump_period_milliseconds():
        return RayConfig.instance().debug_dump_period_milliseconds()

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
    def start_python_importer_thread():
        return RayConfig.instance().start_python_importer_thread()

    @staticmethod
    def use_ray_syncer():
        return RayConfig.instance().use_ray_syncer()

    @staticmethod
    def REDIS_CA_CERT():
        return RayConfig.instance().REDIS_CA_CERT()

    @staticmethod
    def REDIS_CA_PATH():
        return RayConfig.instance().REDIS_CA_PATH()

    @staticmethod
    def REDIS_CLIENT_CERT():
        return RayConfig.instance().REDIS_CLIENT_CERT()

    @staticmethod
    def REDIS_CLIENT_KEY():
        return RayConfig.instance().REDIS_CLIENT_KEY()

    @staticmethod
    def REDIS_SERVER_NAME():
        return RayConfig.instance().REDIS_SERVER_NAME()

    @staticmethod
    def health_check_initial_delay_ms():
        return RayConfig.instance().health_check_initial_delay_ms()

    @staticmethod
    def health_check_period_ms():
        return RayConfig.instance().health_check_period_ms()

    @staticmethod
    def health_check_timeout_ms():
        return RayConfig.instance().health_check_timeout_ms()

    @staticmethod
    def health_check_failure_threshold():
        return RayConfig.instance().health_check_failure_threshold()

    @staticmethod
    def memory_monitor_refresh_ms():
        return (RayConfig.instance().memory_monitor_refresh_ms())
