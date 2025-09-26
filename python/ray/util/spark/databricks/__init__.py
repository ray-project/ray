"""
Databricks-specific functionality for Ray on Spark.

This package contains all features and utilities that are specifically
designed to work only with Databricks runtime environments.
"""

from .hook import DefaultDatabricksRayOnSparkStartHook
from .utils import (
    check_databricks_notebook_idle_time,
    display_databricks_driver_proxy_url,
    get_databricks_auto_shutdown_config,
    get_databricks_display_html_function,
    get_databricks_entry_point,
    get_databricks_environment_variables,
    get_databricks_temp_dir,
    is_in_databricks_runtime,
    register_databricks_spark_job,
    verify_databricks_auth_env,
)

__all__ = [
    "DefaultDatabricksRayOnSparkStartHook",
    "check_databricks_notebook_idle_time",
    "display_databricks_driver_proxy_url",
    "get_databricks_auto_shutdown_config",
    "get_databricks_display_html_function",
    "get_databricks_entry_point",
    "get_databricks_environment_variables",
    "get_databricks_temp_dir",
    "is_in_databricks_runtime",
    "register_databricks_spark_job",
    "verify_databricks_auth_env",
]
