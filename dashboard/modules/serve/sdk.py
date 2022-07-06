from typing import Any, Dict, Optional, Union

try:
    import aiohttp
    import requests
except ImportError:
    aiohttp = None
    requests = None

from ray.dashboard.modules.dashboard_sdk import SubmissionClient


DEPLOY_PATH = "/api/serve/deployments/"
INFO_PATH = "/api/serve/deployments/"
STATUS_PATH = "/api/serve/deployments/status"
DELETE_PATH = "/api/serve/deployments/"


class ServeSubmissionClient(SubmissionClient):
    def __init__(
        self,
        dashboard_address: str,
        create_cluster_if_needed=False,
        cookies: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ):
        if requests is None:
            raise RuntimeError(
                "The Serve CLI requires the ray[default] "
                "installation: `pip install 'ray[default']``"
            )
        super().__init__(
            address=dashboard_address,
            create_cluster_if_needed=create_cluster_if_needed,
            cookies=cookies,
            metadata=metadata,
            headers=headers,
        )
        self._check_connection_and_version(
            min_version="1.12",
            version_error_message="Serve CLI is not supported on the Ray "
            "cluster. Please ensure the cluster is "
            "running Ray 1.12 or higher.",
        )

    def deploy_application(self, config: Dict) -> None:
        response = self._do_request("PUT", DEPLOY_PATH, json_data=config)

        if response.status_code != 200:
            self._raise_error(response)

    def get_info(self) -> Union[Dict, None]:
        response = self._do_request("GET", INFO_PATH)
        if response.status_code == 200:
            return response.json()
        else:
            self._raise_error(response)

    def get_status(self) -> Union[Dict, None]:
        response = self._do_request("GET", STATUS_PATH)
        if response.status_code == 200:
            return response.json()
        else:
            self._raise_error(response)

    def delete_application(self) -> None:
        response = self._do_request("DELETE", DELETE_PATH)
        if response.status_code != 200:
            self._raise_error(response)
