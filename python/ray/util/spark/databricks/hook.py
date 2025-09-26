import logging
import os
import threading
import time

from .utils import (
    check_databricks_notebook_idle_time,
    display_databricks_driver_proxy_url,
    get_databricks_auto_shutdown_config,
    get_databricks_display_html_function,
    get_databricks_entry_point,
    get_databricks_environment_variables,
    get_databricks_temp_dir,
    register_databricks_spark_job,
    verify_databricks_auth_env,
)
from ..start_hook_base import RayOnSparkStartHook
from ..utils import get_spark_session

_logger = logging.getLogger(__name__)


class DefaultDatabricksRayOnSparkStartHook(RayOnSparkStartHook):
    def get_default_temp_root_dir(self):
        return get_databricks_temp_dir()

    def on_ray_dashboard_created(self, port):
        display_databricks_driver_proxy_url(
            get_spark_session().sparkContext, port, "Ray Cluster Dashboard"
        )

    def on_cluster_created(self, ray_cluster_handler):
        db_api_entry = get_db_entry_point()

        if self.is_global:
            # Disable auto shutdown if
            # 1) autoscaling enabled
            #  because in autoscaling mode, background spark job will be killed
            #  automatically when ray cluster is idle.
            # 2) global mode cluster
            #  Because global mode cluster is designed to keep running until
            #  user request to shut down it, and global mode cluster is shared
            #  by other users, the code here cannot track usage from other users
            #  so that we don't know whether it is safe to shut down the global
            #  cluster automatically.
            auto_shutdown_minutes = 0
        else:
            config = get_databricks_auto_shutdown_config()
            auto_shutdown_minutes = float(
                os.environ.get(config["env_var"], config["default_minutes"])
            )
        if auto_shutdown_minutes == 0:
            _logger.info(
                "The Ray cluster will keep running until you manually detach the "
                "Databricks notebook or call "
                "`ray.util.spark.shutdown_ray_cluster()`."
            )
            return
        if auto_shutdown_minutes < 0:
            config = get_databricks_auto_shutdown_config()
            raise ValueError(f"You must set '{config['env_var']}' to a value >= 0.")

        try:
            check_databricks_notebook_idle_time()
        except Exception:
            _logger.warning(
                "Failed to retrieve idle time since last notebook execution, "
                "so that we cannot automatically shut down Ray cluster when "
                "Databricks notebook is inactive for the specified minutes. "
                "You need to manually detach Databricks notebook "
                "or call `ray.util.spark.shutdown_ray_cluster()` to shut down "
                "Ray cluster on spark."
            )
            return

            config = get_databricks_auto_shutdown_config()
            _logger.info(
                "The Ray cluster will be shut down automatically if you don't run "
                "commands on the Databricks notebook for "
                f"{auto_shutdown_minutes} minutes. You can change the "
                "auto-shutdown minutes by setting "
                f"'{config['env_var']}' environment variable, setting it to 0 means "
                "that the Ray cluster keeps running until you manually call "
                "`ray.util.spark.shutdown_ray_cluster()` or detach Databricks notebook."
            )

        def auto_shutdown_watcher():
            auto_shutdown_millis = auto_shutdown_minutes * 60 * 1000
            while True:
                if ray_cluster_handler.is_shutdown:
                    # The cluster is shut down. The watcher thread exits.
                    return

                idle_time = check_databricks_notebook_idle_time()

                if idle_time > auto_shutdown_millis:
                    from ray.util.spark import cluster_init

                    with cluster_init._active_ray_cluster_rwlock:
                        if ray_cluster_handler is cluster_init._active_ray_cluster:
                            cluster_init.shutdown_ray_cluster()
                    return

                config = get_databricks_auto_shutdown_config()
                time.sleep(config["poll_interval_seconds"])

        threading.Thread(target=auto_shutdown_watcher, daemon=True).start()

    def on_spark_job_created(self, job_group_id):
        try:
            register_databricks_spark_job(job_group_id)
        except Exception as e:
            _logger.debug(f"Could not register Databricks Spark job: {e}")

    def custom_environment_variables(self):
        conf = {
            **super().custom_environment_variables(),
            **get_databricks_environment_variables(),
        }

        if verify_databricks_auth_env():
            databricks_host = "DATABRICKS_HOST"
            databricks_token = "DATABRICKS_TOKEN"
            databricks_client_id = "DATABRICKS_CLIENT_ID"
            databricks_client_secret = "DATABRICKS_CLIENT_SECRET"

            conf[databricks_host] = os.environ[databricks_host]
            if databricks_token in os.environ:
                # PAT auth
                conf[databricks_token] = os.environ[databricks_token]
            else:
                # OAuth
                conf[databricks_client_id] = os.environ[databricks_client_id]
                conf[databricks_client_secret] = os.environ[databricks_client_secret]
        else:
            warn_msg = (
                "MLflow support is not correctly configured within Ray tasks."
                "To enable MLflow integration, "
                "you need to set environmental variables DATABRICKS_HOST + "
                "DATABRICKS_TOKEN, or set environmental variables "
                "DATABRICKS_HOST + DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET "
                "before calling `ray.util.spark.setup_ray_cluster`, these variables "
                "are used to set up authentication with Databricks MLflow "
                "service. For details, you can refer to Databricks documentation at "
                "<a href='https://docs.databricks.com/en/dev-tools/auth/pat.html'>"
                "Databricks PAT auth</a> or "
                "<a href='https://docs.databricks.com/en/dev-tools/auth/"
                "oauth-m2m.html'>Databricks OAuth</a>."
            )
            get_databricks_display_html_function()(
                f"<b style='color:red;'>{warn_msg}<br></b>"
            )

        return conf
