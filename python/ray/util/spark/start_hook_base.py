class RayOnSparkStartHook:
    def __init__(self, is_global):
        self.is_global = is_global

    def get_default_temp_root_dir(self):
        return "/tmp"

    def on_ray_dashboard_created(self, port):
        pass

    def on_cluster_created(self, ray_cluster_handler):
        pass

    def on_spark_job_created(self, job_group_id):
        pass

    def custom_environment_variables(self):
        return {}
