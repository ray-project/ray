from typing import List, Dict
import requests
from datetime import datetime, timedelta

class BuildKiteClient:
    def __init__(self, host: str, api_key: str, organization: str, pipeline_name: str):
        # init with buildkite api key and bk client
        self.host = host
        self.api_key = api_key
        self.organization = organization
        self.pipeline_name = pipeline_name


    def get_bk_builds_for_pipeline(self) -> List[str]:
        url = f"{self.host}/organizations/{self.organization}/pipelines/{self.pipeline_name}/builds?branch=master"
        response = requests.get(url, headers={"Authorization": f"Bearer {self.api_key}"})
        return response.json()


    def get_nightly_bk_builds_for_pipeline(self, diff_hours: int) -> List[str]:
        created_from = datetime.now() - timedelta(hours=diff_hours)
        created_to = datetime.now()
        dt_format = "%Y-%m-%dT%H:%M:%SZ"
        dt_created_from = created_from.strftime(dt_format)
        dt_created_to = created_to.strftime(dt_format)
        url = f"{self.host}/organizations/{self.organization}/pipelines/{self.pipeline_name}/builds?branch=master&created_from={dt_created_from}&created_to={dt_created_to}&state=passed&state=failed"
        print(f"url for getting bk builds: {url}")
        response = requests.get(url, headers={"Authorization": f"Bearer {self.api_key}"})
        return response.json()


    def get_bk_jobs_for_build(self, build_id: str) -> List[str]:
        url = f"{self.host}/builds/{build_id}/jobs"
        response = requests.get(url, headers={"Authorization": f"Bearer {self.api_key}"})
        return response.json()


    def get_release_automation_bk_builds(self, builds: str) -> List[str]:
        builds_array = []
        print(f"getting release-automation bk builds for {len(builds)} builds")
        for build in builds:
            if "Triggered by release-automation build" in build["message"]:
                builds_array.append(build)
        return builds_array


    def get_doc_test_jobs_for_build(self, build: Dict) -> Dict[str, str]:
        job_ids_to_names = {}
        for job in build["jobs"]:
            # need a better way to check if the job is a doc test job
            if ("command" in job and "-only-tags doctest" in job["command"]) or ("name" in job and "doc tests" in job["name"]):
                job_ids_to_names[job["id"]] = job["name"]
        return job_ids_to_names