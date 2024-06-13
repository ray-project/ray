import os

from .start_hook_base import RayOnSparkStartHook
from .utils import get_spark_session
import logging
import threading
import time

_logger = logging.getLogger(__name__)


def get_databricks_function(func_name):
    import IPython

    ip_shell = IPython.get_ipython()
    if ip_shell is None:
        raise RuntimeError("No IPython environment.")
    return ip_shell.ns_table["user_global"][func_name]


def get_databricks_display_html_function():
    return get_databricks_function("displayHTML")


def get_db_entry_point():
    """
    Return databricks entry_point instance, it is for calling some
    internal API in databricks runtime
    """
    from dbruntime import UserNamespaceInitializer

    user_namespace_initializer = UserNamespaceInitializer.getOrCreate()
    return user_namespace_initializer.get_spark_entry_point()


def display_databricks_driver_proxy_url(spark_context, port, title):
    """
    This helper function create a proxy URL for databricks driver webapp forwarding.
    In databricks runtime, user does not have permission to directly access web
    service binding on driver machine port, but user can visit it by a proxy URL with
    following format: "/driver-proxy/o/{orgId}/{clusterId}/{port}/".
    """
    driverLocal = spark_context._jvm.com.databricks.backend.daemon.driver.DriverLocal
    commandContextTags = driverLocal.commandContext().get().toStringMap().apply("tags")
    orgId = commandContextTags.apply("orgId")
    clusterId = commandContextTags.apply("clusterId")

    proxy_link = f"/driver-proxy/o/{orgId}/{clusterId}/{port}/"
    proxy_url = f"https://dbc-dp-{orgId}.cloud.databricks.com{proxy_link}"

    print("To monitor and debug Ray from Databricks, view the dashboard at ")
    print(f" {proxy_url}")

    get_databricks_display_html_function()(
        f"""
      <div style="margin-bottom: 16px">
          <a href="{proxy_link}">
              Open {title} in a new tab
          </a>
      </div>
    """
    )


DATABRICKS_AUTO_SHUTDOWN_POLL_INTERVAL_SECONDS = 3
DATABRICKS_RAY_ON_SPARK_AUTOSHUTDOWN_MINUTES = (
    "DATABRICKS_RAY_ON_SPARK_AUTOSHUTDOWN_MINUTES"
)


_DATABRICKS_DEFAULT_TMP_ROOT_DIR = "/local_disk0/tmp"


class DefaultDatabricksRayOnSparkStartHook(RayOnSparkStartHook):
    def get_default_temp_root_dir(self):
        return _DATABRICKS_DEFAULT_TMP_ROOT_DIR

    def on_ray_dashboard_created(self, port):
        display_databricks_driver_proxy_url(
            get_spark_session().sparkContext, port, "Ray Cluster Dashboard"
        )

    def on_cluster_created(self, ray_cluster_handler):
        db_api_entry = get_db_entry_point()

        if ray_cluster_handler.autoscale or self.is_global:
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
            auto_shutdown_minutes = float(
                os.environ.get(DATABRICKS_RAY_ON_SPARK_AUTOSHUTDOWN_MINUTES, "30")
            )
        if auto_shutdown_minutes == 0:
            _logger.info(
                "The Ray cluster will keep running until you manually detach the "
                "Databricks notebook or call "
                "`ray.util.spark.shutdown_ray_cluster()`."
            )
            return
        if auto_shutdown_minutes < 0:
            raise ValueError(
                "You must set "
                f"'{DATABRICKS_RAY_ON_SPARK_AUTOSHUTDOWN_MINUTES}' "
                "to a value >= 0."
            )

        try:
            db_api_entry.getIdleTimeMillisSinceLastNotebookExecution()
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

        _logger.info(
            "The Ray cluster will be shut down automatically if you don't run "
            "commands on the Databricks notebook for "
            f"{auto_shutdown_minutes} minutes. You can change the "
            "auto-shutdown minutes by setting "
            f"'{DATABRICKS_RAY_ON_SPARK_AUTOSHUTDOWN_MINUTES}' environment "
            "variable, setting it to 0 means that the Ray cluster keeps running "
            "until you manually call `ray.util.spark.shutdown_ray_cluster()` or "
            "detach Databricks notebook."
        )

        def auto_shutdown_watcher():
            auto_shutdown_millis = auto_shutdown_minutes * 60 * 1000
            while True:
                if ray_cluster_handler.is_shutdown:
                    # The cluster is shut down. The watcher thread exits.
                    return

                idle_time = db_api_entry.getIdleTimeMillisSinceLastNotebookExecution()

                if idle_time > auto_shutdown_millis:
                    from ray.util.spark import cluster_init

                    with cluster_init._active_ray_cluster_rwlock:
                        if ray_cluster_handler is cluster_init._active_ray_cluster:
                            cluster_init.shutdown_ray_cluster()
                    return

                time.sleep(DATABRICKS_AUTO_SHUTDOWN_POLL_INTERVAL_SECONDS)

        threading.Thread(target=auto_shutdown_watcher, daemon=True).start()

    def on_spark_job_created(self, job_group_id):
        db_api_entry = get_db_entry_point()
        db_api_entry.registerBackgroundSparkJobGroup(job_group_id)

    def custom_environment_variables(self):
        """Hardcode `GLOO_SOCKET_IFNAME` to `eth0` for Databricks runtime.

        Torch on DBR does not reliably detect the correct interface to use,
        and ends up selecting the loopback interface, breaking cross-node
        commnication."""
        return {
            **super().custom_environment_variables(),
            "GLOO_SOCKET_IFNAME": "eth0",
            # Ray nodes runs as subprocess of spark UDF or spark driver,
            # in databricks, it doens't have MLflow service credentials
            # so it can't use MLflow.
            "DISABLE_MLFLOW_INTEGRATION": "TRUE",
        }
