import atexit
import os
import tempfile
from typing import Any, Callable, Dict, Optional

import requests

import ray

_FILE_FORMAT_TO_RAY_READER = {
    "delta": "read_delta",
    "parquet": "read_parquet",
}


class UnityCatalogConnector:
    """
    Load a Unity Catalog table or files into a Ray Dataset, handling cloud credentials automatically.

    Currently only supports Databricks-managed Unity Catalog

    Supported formats: delta, parquet.
    Supports AWS, Azure, and GCP with automatic credential handoff.
    """

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        table_full_name: str,
        region: Optional[str] = None,
        data_format: Optional[str] = "delta",
        operation: str = "READ",
        ray_init_kwargs: Optional[Dict] = None,
        reader_kwargs: Optional[Dict] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.table_full_name = table_full_name
        self.data_format = data_format.lower() if data_format else None
        self.region = region
        self.operation = operation
        self.ray_init_kwargs = ray_init_kwargs or {}
        self.reader_kwargs = reader_kwargs or {}
        self._gcp_temp_file = None

    def _get_table_info(self) -> dict:
        url = f"{self.base_url}/api/2.1/unity-catalog/tables/{self.table_full_name}"
        headers = {"Authorization": f"Bearer {self.token}"}
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        self._table_info = data
        self._table_id = data["table_id"]
        return data

    def _get_creds(self):
        url = f"{self.base_url}/api/2.1/unity-catalog/temporary-table-credentials"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        payload = {"table_id": self._table_id, "operation": self.operation}
        resp = requests.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        self._creds_response = resp.json()
        self._table_url = self._creds_response["url"]

    def _set_env(self):
        env_vars = {}
        creds = self._creds_response

        if "aws_temp_credentials" in creds:
            aws = creds["aws_temp_credentials"]
            env_vars["AWS_ACCESS_KEY_ID"] = aws["access_key_id"]
            env_vars["AWS_SECRET_ACCESS_KEY"] = aws["secret_access_key"]
            env_vars["AWS_SESSION_TOKEN"] = aws["session_token"]
            if self.region:
                env_vars["AWS_REGION"] = self.region
                env_vars["AWS_DEFAULT_REGION"] = self.region
        elif "azuresasuri" in creds:
            env_vars["AZURE_STORAGE_SAS_TOKEN"] = creds["azuresasuri"]
        # Azure UC returns a user delegation SAS; see
        # https://docs.databricks.com/en/data-governance/unity-catalog/credential-vending.html
        elif "azure_user_delegation_sas" in creds:
            azure = creds["azure_user_delegation_sas"] or {}
            sas_token = (
                azure.get("sas_token")
                or azure.get("sas")
                or azure.get("token")
                or azure.get("sasToken")
            )
            if sas_token and sas_token.startswith("?"):
                sas_token = sas_token[1:]
            if sas_token:
                env_vars["AZURE_STORAGE_SAS_TOKEN"] = sas_token
            else:
                known_keys = ", ".join(azure.keys())
                raise ValueError(
                    "Azure UC credentials missing SAS token in azure_user_delegation_sas. "
                    f"Available keys: {known_keys}"
                )
            storage_account = azure.get("storage_account")
            if storage_account:
                env_vars["AZURE_STORAGE_ACCOUNT"] = storage_account
                env_vars["AZURE_STORAGE_ACCOUNT_NAME"] = storage_account
        elif "gcp_service_account" in creds:
            gcp_json = creds["gcp_service_account"]
            temp_file = tempfile.NamedTemporaryFile(
                mode="w",
                prefix="gcp_sa_",
                suffix=".json",
                delete=False,
            )
            temp_file.write(gcp_json)
            temp_file.close()
            env_vars["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file.name
            self._gcp_temp_file = temp_file.name
            atexit.register(self._cleanup_gcp_temp_file, temp_file.name)
        else:
            known_keys = ", ".join(creds.keys())
            raise ValueError(
                "No known credential type found in Databricks UC response. "
                f"Available keys: {known_keys}"
            )

        for k, v in env_vars.items():
            os.environ[k] = v
        self._runtime_env = {"env_vars": env_vars}

    @staticmethod
    def _cleanup_gcp_temp_file(temp_file_path: str):
        """Clean up temporary GCP service account file."""
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except OSError:
                pass

    def _infer_data_format(self) -> str:
        if self.data_format:
            return self.data_format

        info = self._table_info or self._get_table_info()
        if "data_source_format" in info and info["data_source_format"]:
            fmt = info["data_source_format"].lower()
            return fmt

        storage_loc = info.get("storage_location") or getattr(self, "_table_url", None)
        if storage_loc:
            ext = os.path.splitext(storage_loc)[-1].replace(".", "").lower()
            if ext in _FILE_FORMAT_TO_RAY_READER:
                return ext

        raise ValueError("Could not infer data format from table metadata.")

    def _get_ray_reader(self, data_format: str) -> Callable[..., Any]:
        fmt = data_format.lower()
        if fmt in _FILE_FORMAT_TO_RAY_READER:
            reader_func = getattr(ray.data, _FILE_FORMAT_TO_RAY_READER[fmt], None)
            if reader_func:
                return reader_func
        raise ValueError(f"Unsupported data format: {fmt}")

    def _read_delta_with_credentials(self):
        """Read Delta table with proper PyArrow filesystem for session tokens."""
        import pyarrow.fs as pafs

        creds = self._creds_response
        reader_kwargs = self.reader_kwargs.copy()

        # For AWS, create PyArrow S3FileSystem with session tokens
        if "aws_temp_credentials" in creds:
            if not self.region:
                raise ValueError(
                    "The 'region' parameter is required for AWS S3 access. "
                    "Please specify the AWS region (e.g., region='us-west-2')."
                )
            aws = creds["aws_temp_credentials"]
            filesystem = pafs.S3FileSystem(
                access_key=aws["access_key_id"],
                secret_key=aws["secret_access_key"],
                session_token=aws["session_token"],
                region=self.region,
            )
            reader_kwargs["filesystem"] = filesystem

        # Call ray.data.read_delta with proper error handling
        try:
            return ray.data.read_delta(self._table_url, **reader_kwargs)
        except Exception as e:
            error_msg = str(e)
            if (
                "DeletionVectors" in error_msg
                or "Unsupported reader features" in error_msg
            ):
                raise RuntimeError(
                    f"Delta table uses Deletion Vectors, which requires deltalake>=0.10.0. "
                    f"Error: {error_msg}\n"
                    f"Solution: pip install --upgrade 'deltalake>=0.10.0'"
                ) from e
            raise

    def read(self):
        self._get_table_info()
        self._get_creds()
        self._set_env()

        data_format = self._infer_data_format()

        if not ray.is_initialized():
            ray.init(runtime_env=self._runtime_env, **self.ray_init_kwargs)

        # Use special Delta reader for proper filesystem handling
        if data_format == "delta":
            return self._read_delta_with_credentials()

        # Use standard reader for other formats
        reader = self._get_ray_reader(data_format)
        return reader(self._table_url, **self.reader_kwargs)
