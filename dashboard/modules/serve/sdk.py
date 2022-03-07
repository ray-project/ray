from typing import Any, Dict, Optional, Union

try:
    import aiohttp
    import requests
except ImportError:
    aiohttp = None
    requests = None

from ray.dashboard.modules.dashboard_sdk import SubmissionClient
from ray.serve.api import Deployment


DEPLOY_PATH = "/api/serve/deployments/"
INFO_PATH = "/api/serve/deployments/"
STATUS_PATH = "/api/serve/deployments/status"
DELETE_PATH = "/api/serve/deployments/"


class ServeSubmissionClient(SubmissionClient):
    def __init__(
        self,
        address: str,
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
            address=address,
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

    def deploy_application(self, app_config: Dict) -> None:
        response = self._do_request("PUT", DEPLOY_PATH, json_data=app_config)

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

    def set_up_runtime_env(
        self,
        deployment: Deployment,
        new_runtime_env: Optional[dict] = None,
        new_working_dir: Optional[str] = None,
    ) -> None:
        """
        1. If not None, new_runtime_env overwrites deployment's runtime_env.
        2. Then, if not None, new_working_dir overwrites deployment's working_dir.
        3. Then, uploads the deployment's working_dir if it's local.

        Mutates the deployment.
        """

        runtime_env = {}

        if deployment.ray_actor_options is not None:
            runtime_env = deployment.ray_actor_options.get("runtime_env", {})
        if new_runtime_env is not None:
            runtime_env = new_runtime_env
        if new_working_dir is not None:
            runtime_env["working_dir"] = new_working_dir

        self._upload_working_dir_if_needed(runtime_env)

        if deployment.ray_actor_options is None:
            deployment._ray_actor_options = {"runtime_env": runtime_env}
        else:
            deployment.ray_actor_options["runtime_env"] = runtime_env
