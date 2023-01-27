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

    template = "/driver-proxy/o/{orgId}/{clusterId}/{port}/"
    proxy_url = template.format(orgId=orgId, clusterId=clusterId, port=port)

    displayHTML(
        f"""
      <div style="margin-bottom: 16px">
          <a href="{proxy_url}">
              Open {title} in a new tab
          </a>
      </div>
    """
    )


AUTO_SHUTDOWN_POLL_INTERVAL = 3
DATABRICKS_RAY_ON_SPARK_AUTOSHUTDOWN_TIMEOUT_MINUTES = (
    "DATABRICKS_RAY_ON_SPARK_AUTOSHUTDOWN_TIMEOUT_MINUTES"
)


class DefaultDatabricksRayOnSparkStartHook(RayOnSparkStartHook):
    def get_default_temp_dir(self):
        return "/local_disk0/tmp"

    def on_ray_dashboard_created(self, port):
        display_databricks_driver_proxy_url(
            get_spark_session().sparkContext, port, "Ray Cluster Dashboard"
        )

    def on_cluster_created(self, ray_cluster_handler):
        db_api_entry = get_dbutils().entry_point
        try:
            db_api_entry.registerBackgroundSparkJobGroup(
                ray_cluster_handler.spark_job_group_id
            )
        except Exception:
            _logger.warning(
                "Register ray cluster spark job as background job failed. You need to "
                "manually call `ray_cluster_on_spark.shutdown()` before detaching "
                "your databricks python REPL."
            )

        auto_shutdown_timeout_minutes = float(
            os.environ.get(DATABRICKS_RAY_ON_SPARK_AUTOSHUTDOWN_TIMEOUT_MINUTES, "30")
        )
        if auto_shutdown_timeout_minutes == 0:
            _logger.info(
                "The Ray cluster will keep running until you manually detach the "
                "databricks notebook or call "
                "`ray.util.spark.shutdown_ray_cluster()`."
            )
        else:
            _logger.info(
                "The Ray cluster will be shut down automatically if you don't run "
                "commands on the databricks notebook for "
                f"{auto_shutdown_timeout_minutes} minutes. You can change the "
                "timeout minutes by setting "
                "'DATABRICKS_RAY_ON_SPARK_AUTOSHUTDOWN_TIMEOUT_MINUTES' environment "
                "variable, setting it to 0 means infinite timeout."
            )

        def auto_shutdown_watcher():
            auto_shutdown_timeout_millis = auto_shutdown_timeout_minutes * 60 * 1000
            while True:
                if ray_cluster_handler.is_shutdown:
                    # The cluster is shut down. The watcher thread exits.
                    return

                try:
                    idle_time = (
                        db_api_entry.getIdleTimeMillisSinceLastNotebookExecution()
                    )
                except Exception:
                    _logger.warning(
                        "Your current Databricks Runtime version does not support API "
                        "`getIdleTimeMillisSinceLastNotebookExecution`, we cannot "
                        "automatically shut down Ray cluster when databricks notebook "
                        "is inactive, you need to manually detach databricks notebook "
                        "or call `ray.util.spark.shutdown_ray_cluster()` to shut down "
                        "Ray cluster on spark."
                    )
                    return

                if idle_time > auto_shutdown_timeout_millis:
                    from ray.util.spark import cluster_init

                    ray_cluster_handler.shutdown()
                    if ray_cluster_handler is cluster_init._active_ray_cluster:
                        cluster_init._active_ray_cluster = None
                    return

                time.sleep(AUTO_SHUTDOWN_POLL_INTERVAL)

        if auto_shutdown_timeout_minutes > 0:
            threading.Thread(target=auto_shutdown_watcher, args=(), daemon=True).start()
