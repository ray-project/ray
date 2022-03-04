from typing import Any, Dict, Optional, Union

try:
    import aiohttp
    import requests
except ImportError:
    aiohttp = None
    requests = None

from ray.autoscaler._private.cli_logger import cli_logger
from ray.dashboard.modules.dashboard_sdk import SubmissionClient
from ray.serve.api import Deployment


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
        deploy_address = f"{self._address}/api/serve/deployments/"
        response = requests.put(deploy_address, json=app_config)

        if response.status_code == 200:
            cli_logger.newline()
            cli_logger.success(
                "\nSent deploy request successfully!\n "
                "* Use `serve status` to check your deployments' statuses.\n "
                "* Use `serve info` to see your running Serve "
                "application's configuration.\n"
            )
            cli_logger.newline()
        else:
            self._log_failed_request(response)

    def get_info(self) -> Union[Dict, None]:
        info_address = f"{self._address}/api/serve/deployments/"
        response = requests.get(info_address)
        if response.status_code == 200:
            return response.json()
        else:
            self._log_failed_request(response)

    def get_status(self) -> Union[Dict, None]:
        status_address = f"{self._address}/api/serve/deployments/status"
        response = requests.get(status_address)
        if response.status_code == 200:
            return response.json()
        else:
            self._log_failed_request(response)

    def delete_application(self) -> None:
        delete_address = f"{self._address}/api/serve/deployments/"
        response = requests.delete(delete_address)
        if response.status_code == 200:
            cli_logger.newline()
            cli_logger.success("\nSent delete request successfully!\n")
            cli_logger.newline()
        else:
            self._log_failed_request(response)

    def configure_working_dir(self, deployment: Deployment, working_dir: str) -> None:
        """
        Sets the deployment's working_dir to working_dir. Uses the
        submission_client to upload the working_dir if it's local.
        Mutates the deployment.
        """

        runtime_env = {"working_dir": working_dir}
        self._upload_working_dir_if_needed(runtime_env)
        if deployment.ray_actor_options is None:
            deployment._ray_actor_options = {"runtime_env": runtime_env}
        elif "runtime_env" in deployment.ray_actor_options:
            deployment.ray_actor_options["runtime_env"].update(runtime_env)
        else:
            deployment.ray_actor_options["runtime_env"] = runtime_env

    def _log_failed_request(
        self, response: requests.models.Response, address: str = None
    ):
        address = address or self._address
        error_message = (
            f"\nRequest to address {address} failed. Got response status code "
            f"{response.status_code} with the following message:"
            f"\n\n{response.text}"
        )
        cli_logger.newline()
        cli_logger.error(error_message)
        cli_logger.newline()
