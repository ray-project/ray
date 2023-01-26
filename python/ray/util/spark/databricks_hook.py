from .start_hook_base import RayOnSparkStartHook
from .utils import get_spark_session
import logging

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


class DefaultDatabricksRayOnSparkStartHook(RayOnSparkStartHook):
    def get_default_temp_dir(self):
        return "/local_disk0/tmp"

    def on_ray_dashboard_created(self, port):
        display_databricks_driver_proxy_url(
            get_spark_session().sparkContext, port, "Ray Cluster Dashboard"
        )

    def on_spark_background_job_created(self, job_group_id):
        try:
            get_dbutils().entry_point.registerBackgroundSparkJobGroup(job_group_id)
        except Exception:
            _logger.warning(
                "Register ray cluster spark job as background job failed. You need to "
                "manually call `ray_cluster_on_spark.shutdown()` before detaching "
                "your databricks python REPL."
            )
