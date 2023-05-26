import os
import json
import time
from typing import Optional, List
from dataclasses import dataclass

import boto3

from ray_release.result import (
    ResultStatus,
    Result,
)

AWS_BUCKET = "ray-ci-results"
AWS_TEST_KEY = "ray_tests"
AWS_TEST_RESULT_KEY = "ray_test_results"
DEFAULT_PYTHON_VERSION = tuple(
    int(v) for v in os.environ.get("RELEASE_PY", "3.7").split(".")
)
DATAPLANE_ECR = "029272617770.dkr.ecr.us-west-2.amazonaws.com"
DATAPLANE_ECR_REPO = "anyscale/ray"
DATAPLANE_ECR_ML_REPO = "anyscale/ray-ml"


@dataclass
class TestResult:
    status: ResultStatus
    commit: str
    url: str
    timestamp: int

    @classmethod
    def from_result(cls, result: Result):
        return cls(
            status=result.status,
            commit=os.environ.get("BUILDKITE_COMMIT", ""),
            url=result.buildkite_url,
            timestamp=int(time.time() * 1000),
        )

    def from_dict(cls, result: dict):
        return cls(
            status=ResultStatus(result["status"]),
            commit=result["commit"],
            url=result["url"],
            timestamp=result["timestamp"],
        )


class Test(dict):
    """A class represents a test to run on buildkite"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.test_results = None

    def is_byod_cluster(self) -> bool:
        """
        Returns whether this test is running on a BYOD cluster.
        """
        return self["cluster"].get("byod", False)

    def get_byod_type(self) -> Optional[str]:
        """
        Returns the type of the BYOD cluster.
        """
        if not self.is_byod_cluster():
            return None
        return self["cluster"]["byod"]["type"]

    def get_byod_pre_run_cmds(self) -> List[str]:
        """
        Returns the list of pre-run commands for the BYOD cluster.
        """
        if not self.is_byod_cluster():
            return []
        return self["cluster"]["byod"].get("pre_run_cmds", [])

    def get_name(self) -> str:
        """
        Returns the name of the test.
        """
        return self["name"]

    def get_python_version(self) -> str:
        """
        Returns the python version to use for this test. If not specified, use
        the default python version.
        """
        return self.get("python", ".".join(str(v) for v in DEFAULT_PYTHON_VERSION))

    def get_byod_image_tag(self) -> str:
        """
        Returns the byod image tag to use for this test.
        """
        commit = os.environ.get(
            "COMMIT_TO_TEST",
            os.environ["BUILDKITE_COMMIT"],
        )
        ray_version = commit[:6]
        image_suffix = "-gpu" if self.get_byod_type() == "gpu" else ""
        python_version = f"py{self.get_python_version().replace('.',   '')}"
        return f"{ray_version}-{python_version}{image_suffix}"

    def get_byod_repo(self) -> str:
        """
        Returns the byod repo to use for this test.
        """
        return (
            DATAPLANE_ECR_ML_REPO
            if self.get_byod_type() == "gpu"
            else DATAPLANE_ECR_REPO
        )

    def get_ray_image(self) -> str:
        """
        Returns the ray docker image to use for this test.
        """
        ray_project = "ray-ml" if self.get_byod_type() == "gpu" else "ray"
        return f"rayproject/{ray_project}:{self.get_byod_image_tag()}"

    def get_anyscale_byod_image(self) -> str:
        """
        Returns the anyscale byod image to use for this test.
        """
        return f"{DATAPLANE_ECR}/{self.get_byod_repo()}:{self.get_byod_image_tag()}"

    def add_test_result(self, result: Result) -> None:
        """
        Add test result to test object
        """
        self.get_test_results().insert(0, TestResult.from_result(result))

    def get_test_results(self, limit: int = 10) -> List[TestResult]:
        """
        Get test result from test object, or s3
        """
        if self.test_results is not None:
            return self.test_results

        s3_client = boto3.client("s3")
        files = sorted(
            s3_client.list_objects_v2(
                Bucket=AWS_BUCKET,
                Prefix=f"{AWS_TEST_RESULT_KEY}/{self.get_name()}-",
            ).get("Contents", []),
            key=lambda file: int(file["LastModified"].strftime("%s")),
            reverse=True,
        )[:limit]
        self.test_results = [
            TestResult.from_dict(
                json.loads(
                    s3_client.get_object(
                        Bucket=AWS_BUCKET,
                        Key=file["Key"],
                    )
                    .get("Body")
                    .read()
                    .decode("utf-8"),
                ),
            )
            for file in files
        ]
        return self.test_results

    def persist_result_to_s3(self, result: Result) -> bool:
        """
        Persist test object to s3
        """
        boto3.client("s3").put_object(
            Bucket=AWS_BUCKET,
            Key=f"{AWS_TEST_RESULT_KEY}/"
            f"{self.get_name()}-{int(time.time() * 1000)}.json",
            Body=json.dumps(TestResult.from_result(result).__dict__),
        )

    def persist_to_s3(self) -> bool:
        """
        Persist test object to s3
        """
        boto3.client("s3").put_object(
            Bucket=AWS_BUCKET,
            Key=f"{AWS_TEST_KEY}/{self.get_name()}.json",
            Body=json.dumps(self),
        )


class TestDefinition(dict):
    """
    A class represents a definition of a test, such as test name, group, etc. Comparing
    to the test class, there are additional field, for example variations, which can be
    used to define several variations of a test.
    """

    pass
