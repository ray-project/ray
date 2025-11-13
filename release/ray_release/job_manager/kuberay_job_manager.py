import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
import botocore.exceptions
import requests

from ray_release.exception import (
    CommandTimeout,
    JobStartupTimeout,
)
from ray_release.logger import logger

KUBERAY_SERVICE_SECRET_KEY_SECRET_NAME = "kuberay_service_secret_key"
KUBERAY_SERVER_URL = "https://kuberaytest.anyscale.dev"
DEFAULT_KUBERAY_NAMESPACE = "kuberayportal-kevin"
KUBERAY_PROJECT_ID = "dhyey-dev"
JOB_STATUS_CHECK_INTERVAL = 10  # seconds

job_status_to_return_code = {
    "SUCCEEDED": 0,
    "FAILED": 1,
    "ERRORED": -1,
    "CANCELLED": -2,
}


class KubeRayJobManager:
    def __init__(self):
        self.cluster_startup_timeout = 600
        self.job_id = None
        self._kuberay_service_token = None

    def run_and_wait(
        self,
        job_name: str,
        image: str,
        cmd_to_run: str,
        timeout: int,
        env_vars: Dict[str, Any],
        working_dir: Optional[str] = None,
        pip: Optional[List[str]] = None,
        compute_config: Optional[Dict[str, Any]] = None,
        autoscaler_config: Optional[Dict[str, Any]] = None,
    ) -> Tuple[int, float]:
        self.job_name = job_name
        self._run_job(
            job_name,
            image,
            cmd_to_run,
            env_vars,
            working_dir,
            pip,
            compute_config,
            autoscaler_config,
        )
        return self._wait_job(timeout)

    def _run_job(
        self,
        job_name: str,
        image: str,
        cmd_to_run: str,
        env_vars: Dict[str, Any],
        working_dir: Optional[str] = None,
        pip: Optional[List[str]] = None,
        compute_config: Optional[Dict[str, Any]] = None,
        autoscaler_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        logger.info(f"Executing {cmd_to_run} with {env_vars} via RayJob CRD")
        request = {
            "namespace": DEFAULT_KUBERAY_NAMESPACE,
            "name": job_name,
            "entrypoint": cmd_to_run,
            "ray_image": image,
            "compute_config": compute_config,
            "runtime_env": {
                "env_vars": env_vars,
                "pip": pip or [],
                "working_dir": working_dir,
            },
            "autoscaler_config": autoscaler_config,
        }

        url = f"{KUBERAY_SERVER_URL}/api/v1/jobs"
        token = self._get_kuberay_server_token()
        if not token:
            raise Exception("Failed to get KubeRay service token")
        headers = {
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
        }

        logger.info(f"Submitting KubeRay job request: {request}")
        response = requests.post(url, json=request, headers=headers)
        response.raise_for_status()

    def _wait_job(self, timeout_sec: int = 7200) -> Tuple[int, float]:
        """
        Wait for the job to start and enter a terminal state.
        If the job does not start within the timeout, terminate it and raise an error.
        If the job enters a terminal state, return the return code and the duration.

        Args:
            timeout: The timeout for the job to start and enter a terminal state.
        Returns:
            Tuple[int, float]: The return code and the duration.
        """
        start_timestamp = time.time()
        next_status_timestamp = start_timestamp + JOB_STATUS_CHECK_INTERVAL
        deadline_timestamp = start_timestamp + self.cluster_startup_timeout
        job_running = False

        while True:
            now = time.time()
            if now >= deadline_timestamp:
                self._terminate_job()
                if not job_running:
                    raise JobStartupTimeout(
                        "Cluster did not start within "
                        f"{self.cluster_startup_timeout} seconds."
                    )
                raise CommandTimeout(f"Job timed out after {timeout_sec} seconds")

            if now >= next_status_timestamp:
                if job_running:
                    logger.info(
                        f"... job still running ... ({int(now - start_timestamp)} seconds, {int(deadline_timestamp - now)} seconds to timeout)"
                    )
                else:
                    logger.info(
                        f"... job not yet running ... ({int(now - start_timestamp)} seconds, {int(deadline_timestamp - now)} seconds to timeout)"
                    )
                next_status_timestamp += JOB_STATUS_CHECK_INTERVAL

            status = self._get_job_status()
            logger.info(f"Current job status: {status}")
            if not job_running and status in ["RUNNING", "ERRORED"]:
                logger.info("Job started")
                job_running = True
                deadline_timestamp = now + timeout_sec
            if status in ["SUCCEEDED", "FAILED", "ERRORED", "CANCELLED"]:
                logger.info(f"Job entered terminal state {status}")
                duration = time.time() - start_timestamp
                retcode = job_status_to_return_code[status]
                break

            time.sleep(JOB_STATUS_CHECK_INTERVAL)

        duration = time.time() - start_timestamp
        return retcode, duration

    def _get_job(self) -> Dict[str, Any]:
        url = f"{KUBERAY_SERVER_URL}/api/v1/jobs?namespace={DEFAULT_KUBERAY_NAMESPACE}&names={self.job_name}"
        token = self._get_kuberay_server_token()
        if not token:
            raise Exception("Failed to get KubeRay service token")
        headers = {
            "Authorization": "Bearer " + token,
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        response_json = response.json()
        if "jobs" not in response_json or len(response_json["jobs"]) == 0:
            raise Exception(f"No jobs found for {self.job_name}")
        if len(response_json["jobs"]) > 1:
            raise Exception(f"Multiple jobs found for {self.job_name}")
        return response_json["jobs"][0]

    def _get_job_id(self) -> str:
        job = self._get_job()
        if job.get("id"):
            self.job_id = job["id"]
            return self.job_id
        else:
            raise Exception(f"Job {self.job_name} does not have an ID")

    def _get_job_status(self) -> str:
        job = self._get_job()
        return job["status"]

    def _get_kuberay_server_token(self) -> Optional[str]:
        # Use cached token if available
        if self._kuberay_service_token:
            return self._kuberay_service_token

        session = boto3.session.Session()
        client = session.client("secretsmanager", region_name="us-west-2")
        try:
            secret_response = client.get_secret_value(
                SecretId=KUBERAY_SERVICE_SECRET_KEY_SECRET_NAME
            )
            kuberay_service_secret_key = secret_response["SecretString"]
        except (boto3.exceptions.Boto3Error, botocore.exceptions.ClientError) as e:
            logger.error(
                f"Failed to get KubeRay service token from AWS Secrets Manager: {e}"
            )
            return None
        except Exception as e:
            logger.error(f"Failed to get KubeRay service token: {e}")
            return None
        login_url = f"{KUBERAY_SERVER_URL}/api/v1/login"
        login_request = {"secret_key": kuberay_service_secret_key}
        login_response = requests.post(login_url, json=login_request)
        login_response.raise_for_status()

        # Cache the token as instance variable
        self._kuberay_service_token = login_response.json()["token"]
        return self._kuberay_service_token

    def fetch_results(self) -> None:
        # TODO: implement this
        pass

    def _terminate_job(self) -> None:
        # TODO: implement this
        pass
