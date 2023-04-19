import os

from .start_hook_base import RayOnSparkStartHook
from .utils import get_spark_session
import logging
import threading
import time

_logger = logging.getLogger(__name__)


class _NoDbutilsError(Exception):
    pass


def get_dbutils():
    """
    Get databricks runtime dbutils module.
    """
    try:
        import IPython

        ip_shell = IPython.get_ipython()
        if ip_shell is None:
            raise _NoDbutilsError
        return ip_shell.ns_table["user_global"]["dbutils"]
    except ImportError:
        raise _NoDbutilsError
    except KeyError:
        raise _NoDbutilsError


def display_databricks_driver_proxy_url(spark_context, port, title):
    """
    This helper function create a proxy URL for databricks driver webapp forwarding.
    In databricks runtime, user does not have permission to directly access web
    service binding on driver machine port, but user can visit it by a proxy URL with
    following format: "/driver-proxy/o/{orgId}/{clusterId}/{port}/".
    """
    from dbruntime.display import displayHTML

    driverLocal = spark_context._jvm.com.databricks.backend.daemon.driver.DriverLocal
    commandContextTags = driverLocal.commandContext().get().toStringMap().apply("tags")
    orgId = commandContextTags.apply("orgId")
    clusterId = commandContextTags.apply("clusterId")

    proxy_link = f"/driver-proxy/o/{orgId}/{clusterId}/{port}/"
    proxy_url = f"https://dbc-dp-{orgId}.cloud.databricks.com{proxy_link}"

    print("To monitor and debug Ray from Databricks, view the dashboard at ")
    print(f" {proxy_url}")

    displayHTML(
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


def _get_db_api_entry():
    """
    Get databricks API entry point.
    """
    return get_dbutils().entry_point


_DATABRICKS_DEFAULT_TMP_DIR = "/local_disk0/tmp"


class DefaultDatabricksRayOnSparkStartHook(RayOnSparkStartHook):
    def get_default_temp_dir(self):
        return _DATABRICKS_DEFAULT_TMP_DIR

    def on_ray_dashboard_created(self, port):
        display_databricks_driver_proxy_url(
            get_spark_session().sparkContext, port, "Ray Cluster Dashboard"
        )

    def on_cluster_created(self, ray_cluster_handler):
        db_api_entry = _get_db_api_entry()
        try:
            db_api_entry.registerBackgroundSparkJobGroup(
                ray_cluster_handler.spark_job_group_id
            )
        except Exception:
            _logger.warning(
                "Registering Ray cluster spark job as background job failed. "
                "You need to manually call `ray.util.spark.shutdown_ray_cluster()` "
                "before detaching your Databricks notebook."
            )

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
