from typing import Dict, Any, Optional, List, Tuple
from ray_release.logger import logger
import boto3
import requests
import time
from ray_release.exception import (
    CommandTimeout,
    JobStartupTimeout,
    JobStartupFailed,
)

KUBERAY_SERVICE_SECRET_KEY_SECRET_NAME = "kuberay_service_secret_key"
KUBERAY_SERVER_URL = "https://kuberaytest.anyscale.dev"
DEFAULT_KUBERAY_NAMESPACE = "kuberayportal-kevin"
KUBERAY_PROJECT_ID = "dhyey-dev"

job_status_to_return_code = {
    "SUCCEEDED": 0,
    "FAILED": 1,
    "ERRORED": -1,
    "CANCELLED": -2,
}

class KuberayJobManager:
    def __init__(self):
        self.cluster_startup_timeout = 600
        self.job_id = None

    def run_and_wait(self, job_name: str, image: str, cmd_to_run: str, timeout: int, env_vars: Dict[str, Any], working_dir: Optional[str] = None, pip: Optional[List[str]] = None, compute_config: Optional[Dict[str, Any]] = None) -> Tuple[int, float]:
        self.job_name = job_name
        self._run_job(job_name, image, cmd_to_run, env_vars, working_dir, pip, compute_config)
        return self._wait_job(timeout)

    def _run_job(self, job_name: str, image: str, cmd_to_run: str, env_vars: Dict[str, Any], working_dir: Optional[str] = None, pip: Optional[List[str]] = None, compute_config: Optional[Dict[str, Any]] = None) -> None:
        logger.info(
            f"Executing {cmd_to_run} with {env_vars} via RayJob CRD"
        )
        request = {
            "namespace": DEFAULT_KUBERAY_NAMESPACE,
            "name": job_name,
            "entrypoint": cmd_to_run,
            "rayImage": image,
            "computeConfig": compute_config,
            "runtimeEnv": {
                "env_vars": env_vars,
                "pip": pip or [],
                "working_dir": working_dir
            }
        }
        if compute_config.get("headNode", {}).get("resources", {}):
            request["autoscalerConfig"] = {
                "version": "v2"
            }

        url = f"{KUBERAY_SERVER_URL}/api/v1/jobs"
        token = self._get_kuberay_server_token()
        headers = {
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json"
        }

        logger.info(f"Submitting KubeRay job request: {request}")
        try:
            response = requests.post(url, json=request, headers=headers)
            print(f"Response: {response}")
            response.raise_for_status()
        except Exception as e:
            raise JobStartupFailed(
                "Error starting job with name "
                f"{self.job_name}: "
                f"{e}"
            ) from e

    def _wait_job(self, timeout: int = 7200) -> Tuple[int, float]:
        start_time = time.time()
        next_status = start_time + 10
        timeout_at = start_time + self.cluster_startup_timeout
        job_running = False

        while True:
            now = time.time()
            if now >= timeout_at:
                self._terminate_job()
                if not job_running:
                    raise JobStartupTimeout(
                        "Cluster did not start within "
                        f"{self.cluster_startup_timeout} seconds."
                    )
                raise CommandTimeout(f"Job timed out after {timeout} seconds")

            if now >= next_status:
                if job_running:
                    logger.info(f"... job still running ... ({int(now - start_time)} seconds, {int(timeout_at - now)} seconds to timeout)")
                else:
                    logger.info(f"... job not yet running ... ({int(now - start_time)} seconds, {int(timeout_at - now)} seconds to timeout)")
                next_status += 10

            status = self._get_job_status()
            logger.info(f"Current job status: {status}")
            if not job_running and status in ["RUNNING", "ERRORED"]:
                logger.info(f"Job started")
                job_running = True
                timeout_at = now + timeout
            if status in ["SUCCEEDED", "FAILED", "ERRORED", "CANCELLED"]:
                logger.info(f"Job entered terminal state {status}")
                duration = time.time() - start_time
                retcode = job_status_to_return_code[status]
                break

            time.sleep(10)

        duration = time.time() - start_time
        return retcode, duration

    def _get_job(self) -> Dict[str, Any]:
        url = f"{KUBERAY_SERVER_URL}/api/v1/jobs?namespace={DEFAULT_KUBERAY_NAMESPACE}&names={self.job_name}"
        token = self._get_kuberay_server_token()
        headers = {
            "Authorization": "Bearer " + token,
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        response_json = response.json()
        if "jobs" not in response_json or len(response_json["jobs"]) == 0:
            raise Exception(f"No jobs found for {self.job_name}")
        if len(response_json["jobs"]) > 1:
            print(f"{self.job_name}Jobs: \n")
            for job in response_json["jobs"]:
                print(job)
                print()
            raise Exception(f"Multiple jobs found for {self.job_name}")
        return response_json["jobs"][0]

    def _get_job_id(self) -> str:
        job = self._get_job()
        self.job_id = job["id"]
        return self.job_id

    def _get_job_status(self) -> str:
        job = self._get_job()
        return job["status"]

    def _get_kuberay_server_token(self) -> str:
        session = boto3.session.Session()
        client = session.client('secretsmanager', region_name='us-west-2')
        try:
            secret_response = client.get_secret_value(
                SecretId=KUBERAY_SERVICE_SECRET_KEY_SECRET_NAME
            )
            kuberay_service_secret_key = secret_response['SecretString']
        except Exception as e:
            logger.error(f"Failed to get KubeRay server token from AWS Secrets Manager: {e}")
            raise
        login_url = f"{KUBERAY_SERVER_URL}/api/v1/login"
        login_request = {
            "secretKey": kuberay_service_secret_key
        }
        login_response = requests.post(login_url, json=login_request)
        login_response.raise_for_status()
        return login_response.json()["token"]

    def fetch_results(self) -> Dict[str, Any]:
        return {}
        # client = logging_v2.services.logging_service_v2.LoggingServiceV2Client()
        # # Filter logs for k8s containers with specific ray_submission_id
        # job_id = self._get_job_id()
        # filter_query = f'resource.type="k8s_container" AND jsonPayload.ray_submission_id="{self.job_id}"'
        # result = client.list_log_entries(
        #     request={
        #         "resource_names": [f"projects/{KUBERAY_PROJECT_ID}"],
        #         "filter": filter_query,
        #         "order_by": "timestamp desc",
        #         "page_size": 100,
        #     },
        #     timeout=300,
        # )
        # for entry in result.entries:
        #     log_message = entry.json_payload["log"]
        #     print(log_message)
        
        # now = time.time()
        # twelve_hours_ago = now - (12 * 60 * 60)
        
        # now_str = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(now))
        # start_str = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(twelve_hours_ago))
        
        # logs_url = (
        #     f"https://console.cloud.google.com/logs/query;"
        #     f"query=resource.type%3D%22k8s_container%22%20AND%20"
        #     f"jsonPayload.ray_submission_id%3D%22{self.job_id}%22;"
        #     f"cursorTimestamp={now_str};"
        #     f"startTime={start_str};"
        #     f"endTime={now_str}?"
        #     f"referrer=search&cloudshell=true&inv=1&invt=Abyi5w&"
        #     f"project={KUBERAY_PROJECT_ID}"
        # )
        
        # return {"logs_url": logs_url}
    
    def _terminate_job(self) -> None:
        # TODO: implement this
        pass
