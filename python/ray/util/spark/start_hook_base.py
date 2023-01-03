class RayOnSparkStartHook:
    def get_default_temp_dir(self):
        return "/tmp"

    def on_ray_dashboard_created(self, port):
        pass

    def on_spark_background_job_created(self, job_group):
        pass
