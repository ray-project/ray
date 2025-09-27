"""
Databricks-specific utilities and functionality.

This module contains all Databricks-only features and utilities that are
specifically designed to work with Databricks runtime environments.
These features are not available or applicable to OSS Apache Spark.
"""

import os
from typing import Optional


def is_in_databricks_runtime() -> bool:
    """
    Check if running in Databricks runtime environment.

    This is the only function that determines Databricks-specific behavior
    throughout the Ray on Spark codebase.
    """
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_databricks_runtime_version() -> Optional[str]:
    """Get Databricks runtime version if available."""
    return os.environ.get("DATABRICKS_RUNTIME_VERSION")


def verify_databricks_auth_env() -> bool:
    """Verify Databricks authentication environment variables."""
    databricks_host = "DATABRICKS_HOST"
    databricks_token = "DATABRICKS_TOKEN"
    databricks_client_id = "DATABRICKS_CLIENT_ID"
    databricks_client_secret = "DATABRICKS_CLIENT_SECRET"

    return (databricks_host in os.environ and databricks_token in os.environ) or (
        databricks_host in os.environ
        and databricks_client_id in os.environ
        and databricks_client_secret in os.environ
    )


def get_databricks_temp_dir() -> str:
    """Get Databricks-specific temporary directory path."""
    return "/local_disk0/tmp"


def get_databricks_environment_variables() -> dict:
    """Get Databricks-specific environment variable configurations."""
    return {
        # Hardcode for Databricks runtime networking
        "GLOO_SOCKET_IFNAME": "eth0",
        # Disable MLflow for Databricks runtime compatibility
        "DISABLE_MLFLOW_INTEGRATION": "TRUE",
    }


def get_databricks_function(func_name: str):
    """Get Databricks notebook function by name."""
    try:
        import IPython

        ip_shell = IPython.get_ipython()
        if ip_shell is None:
            raise RuntimeError("No IPython environment.")
        return ip_shell.ns_table["user_global"][func_name]
    except ImportError:
        raise RuntimeError("IPython not available in non-Databricks environment.")


def get_databricks_display_html_function():
    """Get Databricks displayHTML function for notebook output."""
    return get_databricks_function("displayHTML")


def get_databricks_entry_point():
    """Get Databricks internal API entry point."""
    try:
        from dbruntime import UserNamespaceInitializer

        user_namespace_initializer = UserNamespaceInitializer.getOrCreate()
        return user_namespace_initializer.get_spark_entry_point()
    except ImportError:
        raise RuntimeError("dbruntime module only available in Databricks runtime.")


def display_databricks_driver_proxy_url(spark_context, port: int, title: str):
    """Generate and display Databricks-specific proxy URL for driver web apps."""
    try:
        # Access Databricks internal Java objects
        driver_local = (
            spark_context._jvm.com.databricks.backend.daemon.driver.DriverLocal
        )
        command_context_tags = (
            driver_local.commandContext().get().toStringMap().apply("tags")
        )
        org_id = command_context_tags.apply("orgId")
        cluster_id = command_context_tags.apply("clusterId")

        proxy_link = f"/driver-proxy/o/{org_id}/{cluster_id}/{port}/"
        proxy_url = f"https://dbc-dp-{org_id}.cloud.databricks.com{proxy_link}"

        print(
            f"To monitor and debug Ray from Databricks, view the dashboard at {proxy_url}"
        )

        get_databricks_display_html_function()(
            f"""
            <div style="margin-top: 16px;margin-bottom: 16px">
                <a href="{proxy_link}">
                    Open {title} in a new tab
                </a>
            </div>
            """
        )
    except Exception as e:
        print(f"Could not generate Databricks proxy URL: {e}")


def get_databricks_auto_shutdown_config() -> dict:
    """Get Databricks auto-shutdown configuration."""
    return {
        "poll_interval_seconds": 3,
        "env_var": "DATABRICKS_RAY_ON_SPARK_AUTOSHUTDOWN_MINUTES",
        "default_minutes": 30,
    }


def check_databricks_notebook_idle_time():
    """Check Databricks notebook idle time via internal API."""
    try:
        db_entry = get_databricks_entry_point()
        return db_entry.getIdleTimeMillisSinceLastNotebookExecution()
    except Exception:
        raise RuntimeError(
            "Cannot access Databricks notebook idle time outside Databricks runtime."
        )


def register_databricks_spark_job(job_group_id: str):
    """Register Spark job group with Databricks internal API."""
    try:
        db_entry = get_databricks_entry_point()
        db_entry.registerBackgroundSparkJobGroup(job_group_id)
    except Exception:
        raise RuntimeError(
            "Cannot register Spark job group outside Databricks runtime."
        )
