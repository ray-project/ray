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
        elif "gcp_service_account" in creds:
            gcp_json = creds["gcp_service_account"]
            with tempfile.NamedTemporaryFile(
                prefix="gcp_sa_", suffix=".json", delete=True
            ) as temp_file:
                temp_file.write(gcp_json.encode())
                temp_file.flush()
                env_vars["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file.name
        else:
            raise ValueError(
                "No known credential type found in Databricks UC response."
            )

        for k, v in env_vars.items():
            os.environ[k] = v
        self._runtime_env = {"env_vars": env_vars}

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

    def read(self):
        self._get_table_info()
        self._get_creds()
        self._set_env()

        data_format = self._infer_data_format()
        reader = self._get_ray_reader(data_format)

        if not ray.is_initialized():
            ray.init(runtime_env=self._runtime_env, **self.ray_init_kwargs)

        url = self._table_url
        ds = reader(url, **self.reader_kwargs)
        return ds
