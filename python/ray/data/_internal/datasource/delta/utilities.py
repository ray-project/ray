"""
Utility classes and functions for Delta Lake datasource.

This module contains cloud-specific utilities, helper functions, and
standalone operations for Delta Lake functionality.
"""

import logging
from typing import Any, Dict, List, Optional

import pyarrow.fs as pa_fs
from deltalake import DeltaTable


logger = logging.getLogger(__name__)


class AWSUtilities:
    """Utility class for Amazon Web Services credential management and configuration."""

    @staticmethod
    def _get_aws_credentials():
        """
        Get AWS credentials using boto3.

        Returns:
            Dict with AWS credentials
        """
        try:
            import boto3

            session = boto3.Session()
            credentials = session.get_credentials()

            if credentials:
                return {
                    "AWS_ACCESS_KEY_ID": credentials.access_key,
                    "AWS_SECRET_ACCESS_KEY": credentials.secret_key,
                    "AWS_SESSION_TOKEN": credentials.token,
                    "AWS_REGION": session.region_name or "us-east-1",
                }
        except Exception:
            pass

        return {}

    @staticmethod
    def get_s3_storage_options(path: str) -> Dict[str, str]:
        """
        Get S3 storage options with automatic credential detection.

        Args:
            path: S3 path

        Returns:
            Dict with S3 storage options
        """
        storage_options = {}

        # Try to get AWS credentials
        credentials = AWSUtilities._get_aws_credentials()
        storage_options.update(credentials)

        return storage_options


class GCPUtilities:
    """Utility class for Google Cloud Platform credential management and configuration."""

    @staticmethod
    def _get_gcp_project():
        """
        Get the GCP project ID from various sources.

        Returns:
            Project ID if found
        """
        try:
            # Try to get from google-cloud-core
            from google.cloud import storage

            client = storage.Client()
            return client.project
        except Exception:
            pass

        # Try to get from Ray cluster resources (Anyscale)
        try:
            import ray

            cluster_resources = ray.cluster_resources()
            project_resources = [
                k.split(":")[1]
                for k in cluster_resources.keys()
                if k.startswith("anyscale/gcp-project:")
            ]
            if project_resources:
                return project_resources[0]
        except Exception:
            pass

        return None


class AzureUtilities:
    """Utility class for Microsoft Azure credential management and configuration."""

    @staticmethod
    def _get_azure_credentials():
        """
        Get Azure credentials using azure-identity library.

        Returns:
            Credentials object
        """
        try:
            from azure.core.exceptions import ClientAuthenticationError
            from azure.identity import DefaultAzureCredential

            try:
                credential = DefaultAzureCredential()
                # Test the credential by attempting to get a token
                credential.get_token("https://storage.azure.com/.default")
                return credential
            except ClientAuthenticationError:
                return None
        except ImportError:
            logger.warning(
                "azure-identity not available. Install azure-identity for automatic credential detection."
            )
            return None
        except Exception:
            return None

    @staticmethod
    def get_azure_storage_options(path: str) -> Dict[str, str]:
        """
        Get Azure storage options with automatic credential detection.

        Args:
            path: Azure storage path

        Returns:
            Dict with Azure storage options
        """
        storage_options = {}

        # Try to get Azure credentials
        credentials = AzureUtilities._get_azure_credentials()
        if credentials:
            try:
                token = credentials.get_token("https://storage.azure.com/.default")
                storage_options["AZURE_STORAGE_TOKEN"] = token.token
            except Exception:
                pass

        return storage_options


def try_get_deltatable(
    table_uri: str, storage_options: Optional[Dict[str, str]] = None
) -> Optional[DeltaTable]:
    """
    Try to get a DeltaTable object, returning None if it doesn't exist or can't be read.

    Args:
        table_uri: Path to the Delta table
        storage_options: Storage options for the filesystem

    Returns:
        DeltaTable object if successful, None otherwise
    """
    try:
        return DeltaTable(table_uri, storage_options=storage_options)
    except Exception:
        return None


class DeltaUtilities:
    """Utility class for Delta Lake operations and helper functions."""

    def __init__(self, path: str, storage_options: Optional[Dict[str, str]] = None):
        """
        Initialize Delta utilities.

        Args:
            path: Path to the Delta table
            storage_options: Storage options for the filesystem
        """
        self.path = path
        self.storage_options = storage_options or {}

        # Set up filesystem
        if storage_options:
            self.filesystem = pa_fs.S3FileSystem(**storage_options)
        else:
            self.filesystem = None

        # Detect cloud provider from path scheme
        path_lower = self.path.lower()
        self.is_aws = path_lower.startswith(("s3://", "s3a://"))
        self.is_gcp = path_lower.startswith(("gs://", "gcs://"))
        self.is_azure = path_lower.startswith(("abfss://", "abfs://", "adl://"))
        self.storage_options = self._get_storage_options()

        # Initialize utility classes
        self.aws_utils = AWSUtilities()
        self.gcp_utils = GCPUtilities()
        self.azure_utils = AzureUtilities()

    def _get_storage_options(self) -> Dict[str, str]:
        """
        Get storage options based on the path and detected cloud provider.

        Returns:
            Dict with storage options
        """
        if self.is_aws:
            return self.aws_utils.get_s3_storage_options(self.path)
        elif self.is_azure:
            return self.azure_utils.get_azure_storage_options(self.path)
        else:
            return {}

    def get_table(self) -> Optional[DeltaTable]:
        """
        Get the DeltaTable object.

        Returns:
            DeltaTable object if successful, None otherwise
        """
        return try_get_deltatable(self.path, self.storage_options)

    def table_exists(self) -> bool:
        """
        Check if the Delta table exists.

        Returns:
            True if table exists, False otherwise
        """
        return self.get_table() is not None

    def validate_path(self, path: str) -> None:
        """
        Validate a Delta table path.

        Args:
            path: Path to validate

        Raises:
            ValueError: If path is invalid
        """
        if not path or not isinstance(path, str):
            raise ValueError("Path must be a non-empty string")

        path_lower = path.lower()
        supported_schemes = [
            "file://",
            "local://",
            "s3://",
            "s3a://",
            "gs://",
            "gcs://",
            "abfss://",
            "abfs://",
            "adl://",
            "hdfs://",
        ]
        cloud_schemes = (
            "s3://",
            "s3a://",
            "gs://",
            "gcs://",
            "abfss://",
            "abfs://",
            "adl://",
        )

        if any(path_lower.startswith(scheme) for scheme in supported_schemes):
            if path_lower.startswith(cloud_schemes) and len(path.split("/")) < 4:
                raise ValueError(f"Cloud storage path appears incomplete: {path}")
        elif path_lower.startswith("azure://"):
            raise ValueError(
                "Use 'abfss://' or 'abfs://' instead of 'azure://' for Azure paths"
            )
        elif not path.startswith("/") and "://" not in path:
            # Assume local path
            pass
        else:
            logger.warning(f"Unrecognized path scheme for: {path}")


def compact_delta_table(
    table_path: str,
    storage_options: Optional[Dict[str, str]] = None,
    partition_filters: Optional[List[tuple]] = None,
    target_size: Optional[int] = None,
    max_concurrent_tasks: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Compact a Delta table by merging small files.

    Args:
        table_path: Path to the Delta table
        storage_options: Storage options for the filesystem
        partition_filters: Optional partition filters
        target_size: Target file size in bytes
        max_concurrent_tasks: Maximum concurrent tasks

    Returns:
        Dict with compaction metrics
    """
    try:
        dt = DeltaTable(table_path, storage_options=storage_options)

        # Perform compaction
        metrics = dt.optimize.compact(
            partition_filters=partition_filters,
            target_size=target_size,
            max_concurrent_tasks=max_concurrent_tasks,
        )

        return {
            "files_added": metrics.get("files_added", 0),
            "files_removed": metrics.get("files_removed", 0),
            "partitions_optimized": metrics.get("partitions_optimized", 0),
            "num_batches": metrics.get("num_batches", 0),
            "total_considered_files": metrics.get("total_considered_files", 0),
            "total_files_skipped": metrics.get("total_files_skipped", 0),
        }
    except Exception as e:
        logger.error(f"Compaction failed: {e}")
        raise


def z_order_delta_table(
    table_path: str,
    columns: List[str],
    storage_options: Optional[Dict[str, str]] = None,
    partition_filters: Optional[List[tuple]] = None,
    target_size: Optional[int] = None,
    max_concurrent_tasks: Optional[int] = None,
    max_spill_size: int = 20 * 1024 * 1024 * 1024,  # 20GB
) -> Dict[str, Any]:
    """
    Optimize a Delta table using Z-order clustering.

    Args:
        table_path: Path to the Delta table
        columns: Columns to Z-order by
        storage_options: Storage options for the filesystem
        partition_filters: Optional partition filters
        target_size: Target file size in bytes
        max_concurrent_tasks: Maximum concurrent tasks
        max_spill_size: Maximum spill size in bytes

    Returns:
        Dict with Z-order metrics
    """
    try:
        dt = DeltaTable(table_path, storage_options=storage_options)

        # Perform Z-order optimization
        metrics = dt.optimize.z_order(
            columns=columns,
            partition_filters=partition_filters,
            target_size=target_size,
            max_concurrent_tasks=max_concurrent_tasks,
            max_spill_size=max_spill_size,
        )

        return {
            "files_added": metrics.get("files_added", 0),
            "files_removed": metrics.get("files_removed", 0),
            "partitions_optimized": metrics.get("partitions_optimized", 0),
            "num_batches": metrics.get("num_batches", 0),
            "total_considered_files": metrics.get("total_considered_files", 0),
            "total_files_skipped": metrics.get("total_files_skipped", 0),
        }
    except Exception as e:
        logger.error(f"Z-order optimization failed: {e}")
        raise


def vacuum_delta_table(
    table_path: str,
    retention_hours: Optional[int] = None,
    storage_options: Optional[Dict[str, str]] = None,
    enforce_retention_duration: bool = True,
    dry_run: bool = False,
) -> List[str]:
    """
    Vacuum a Delta table to remove old files.

    Args:
        table_path: Path to the Delta table
        retention_hours: Retention period in hours
        storage_options: Storage options for the filesystem
        enforce_retention_duration: Whether to enforce retention duration
        dry_run: Whether to perform a dry run

    Returns:
        List of files that were or would be deleted
    """
    try:
        dt = DeltaTable(table_path, storage_options=storage_options)

        # Perform vacuum operation
        files = dt.vacuum(
            retention_hours=retention_hours,
            enforce_retention_duration=enforce_retention_duration,
            dry_run=dry_run,
        )

        return files
    except Exception as e:
        logger.error(f"Vacuum failed: {e}")
        raise
