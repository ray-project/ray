import enum
import os
import json
import time
from typing import Optional, List, Dict
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError
from github import Repository

from ray_release.configs.global_config import get_global_config
from ray_release.result import (
    ResultStatus,
    Result,
)
from ray_release.logger import logger
from ray_release.util import dict_hash

AWS_TEST_KEY = "ray_tests"
AWS_TEST_RESULT_KEY = "ray_test_results"
DEFAULT_PYTHON_VERSION = tuple(
    int(v) for v in os.environ.get("RELEASE_PY", "3.8").split(".")
)
DATAPLANE_ECR_REPO = "anyscale/ray"
DATAPLANE_ECR_ML_REPO = "anyscale/ray-ml"


def _convert_env_list_to_dict(env_list: List[str]) -> Dict[str, str]:
    env_dict = {}
    for env in env_list:
        # an env can be "a=b" or just "a"
        eq_pos = env.find("=")
        if eq_pos < 0:
            env_dict[env] = os.environ.get(env, "")
        else:
            env_dict[env[:eq_pos]] = env[eq_pos + 1 :]
    return env_dict


class TestState(enum.Enum):
    """
    Overall state of the test
    """

    JAILED = "jailed"
    FAILING = "failing"
    CONSITENTLY_FAILING = "consistently_failing"
    PASSING = "passing"


@dataclass
class TestResult:
    status: str
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

    @classmethod
    def from_dict(cls, result: dict):
        return cls(
            status=result["status"],
            commit=result["commit"],
            url=result["url"],
            timestamp=result["timestamp"],
        )

    def is_failing(self) -> bool:
        return not self.is_passing()

    def is_passing(self) -> bool:
        return self.status == ResultStatus.SUCCESS.value


class Test(dict):
    """A class represents a test to run on buildkite"""

    KEY_GITHUB_ISSUE_NUMBER = "github_issue_number"
    KEY_BISECT_BUILD_NUMBER = "bisect_build_number"
    KEY_BISECT_BLAMED_COMMIT = "bisect_blamed_commit"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.test_results = None

    def is_jailed_with_open_issue(self, ray_github: Repository) -> bool:
        """
        Returns whether this test is jailed with open issue.
        """
        # is jailed
        state = self.get_state()
        if state != TestState.JAILED:
            return False

        # has open issue
        issue_number = self.get(self.KEY_GITHUB_ISSUE_NUMBER)
        if issue_number is None:
            return False
        issue = ray_github.get_issue(issue_number)
        return issue.state == "open"

    def is_stable(self) -> bool:
        """
        Returns whether this test is stable.
        """
        return self.get("stable", True)

    def is_byod_cluster(self) -> bool:
        """
        Returns whether this test is running on a BYOD cluster.
        """
        if os.environ.get("BUILDKITE_PULL_REQUEST", "false") != "false":
            # Do not run BYOD tests on PRs
            return False
        return self["cluster"].get("byod") is not None

    def get_byod_type(self) -> Optional[str]:
        """
        Returns the type of the BYOD cluster.
        """
        if not self.is_byod_cluster():
            return None
        return self["cluster"]["byod"].get("type", "cpu")

    def get_byod_post_build_script(self) -> Optional[str]:
        """
        Returns the post-build script for the BYOD cluster.
        """
        if not self.is_byod_cluster():
            return None
        return self["cluster"]["byod"].get("post_build_script")

    def get_byod_runtime_env(self) -> Dict[str, str]:
        """
        Returns the runtime environment variables for the BYOD cluster.
        """
        if not self.is_byod_cluster():
            return {}
        return _convert_env_list_to_dict(self["cluster"]["byod"].get("runtime_env", []))

    def get_byod_pips(self) -> List[str]:
        """
        Returns the list of pips for the BYOD cluster.
        """
        if not self.is_byod_cluster():
            return []
        return self["cluster"]["byod"].get("pip", [])

    def get_name(self) -> str:
        """
        Returns the name of the test.
        """
        return self["name"]

    def get_oncall(self) -> str:
        """
        Returns the oncall for the test.
        """
        return self["team"]

    def update_from_s3(self) -> None:
        """
        Update test object with data field from s3
        """
        try:
            data = (
                boto3.client("s3")
                .get_object(
                    Bucket=get_global_config()["state_machine_aws_bucket"],
                    Key=f"{AWS_TEST_KEY}/{self.get_name()}.json",
                )
                .get("Body")
                .read()
                .decode("utf-8")
            )
        except ClientError as e:
            logger.warning(f"Failed to update data for {self.get_name()} from s3:  {e}")
            return
        self.update(json.loads(data))

    def get_state(self) -> TestState:
        """
        Returns the state of the test.
        """
        return TestState(self.get("state", TestState.PASSING.value))

    def set_state(self, state: TestState) -> None:
        """
        Sets the state of the test.
        """
        self["state"] = state.value

    def get_python_version(self) -> str:
        """
        Returns the python version to use for this test. If not specified, use
        the default python version.
        """
        return self.get("python", ".".join(str(v) for v in DEFAULT_PYTHON_VERSION))

    def get_byod_base_image_tag(self) -> str:
        """
        Returns the byod image tag to use for this test.
        """
        commit = os.environ.get(
            "COMMIT_TO_TEST",
            os.environ["BUILDKITE_COMMIT"],
        )
        branch = os.environ.get(
            "BRANCH_TO_TEST",
            os.environ["BUILDKITE_BRANCH"],
        )
        ray_version = commit[:6]
        assert branch == "master" or branch.startswith(
            "releases/"
        ), f"Invalid branch name {branch}"
        if branch.startswith("releases/"):
            release_name = branch[len("releases/") :]
            ray_version = f"{release_name}.{ray_version}"
        python_version = f"py{self.get_python_version().replace('.',   '')}"
        return f"{ray_version}-{python_version}-{self.get_byod_type()}"

    def get_byod_image_tag(self) -> str:
        """
        Returns the byod custom image tag to use for this test.
        """
        if not self.require_custom_byod_image():
            return self.get_byod_base_image_tag()
        custom_info = {
            "post_build_script": self.get_byod_post_build_script(),
        }
        return f"{self.get_byod_base_image_tag()}-{dict_hash(custom_info)}"

    def get_byod_repo(self) -> str:
        """
        Returns the byod repo to use for this test.
        """
        return (
            DATAPLANE_ECR_REPO
            if self.get_byod_type() == "cpu"
            else DATAPLANE_ECR_ML_REPO
        )

    def get_ray_image(self) -> str:
        """
        Returns the ray docker image to use for this test.
        """
        config = get_global_config()
        ray_project = (
            config["byod_ray_cr_repo"]
            if self.get_byod_type() == "cpu"
            else config["byod_ray_ml_cr_repo"]
        )
        return (
            f"{config['byod_ray_ecr']}/"
            f"{ray_project}:{self.get_byod_base_image_tag()}"
        )

    def get_anyscale_base_byod_image(self) -> str:
        """
        Returns the anyscale byod image to use for this test.
        """
        return (
            f"{get_global_config()['byod_ecr']}/"
            f"{self.get_byod_repo()}:{self.get_byod_base_image_tag()}"
        )

    def require_custom_byod_image(self) -> bool:
        """
        Returns whether this test requires a custom byod image.
        """
        return self.get_byod_post_build_script() is not None

    def get_anyscale_byod_image(self) -> str:
        """
        Returns the anyscale byod image to use for this test.
        """
        return (
            f"{get_global_config()['byod_ecr']}/"
            f"{self.get_byod_repo()}:{self.get_byod_image_tag()}"
        )

    def get_test_results(
        self, limit: int = 10, refresh: bool = False
    ) -> List[TestResult]:
        """
        Get test result from test object, or s3

        :param limit: limit of test results to return
        :param refresh: whether to refresh the test results from s3
        """
        if self.test_results is not None and not refresh:
            return self.test_results

        s3_client = boto3.client("s3")
        files = sorted(
            s3_client.list_objects_v2(
                Bucket=get_global_config()["state_machine_aws_bucket"],
                Prefix=f"{AWS_TEST_RESULT_KEY}/{self.get_name()}-",
            ).get("Contents", []),
            key=lambda file: int(file["LastModified"].strftime("%s")),
            reverse=True,
        )[:limit]
        self.test_results = [
            TestResult.from_dict(
                json.loads(
                    s3_client.get_object(
                        Bucket=get_global_config()["state_machine_aws_bucket"],
                        Key=file["Key"],
                    )
                    .get("Body")
                    .read()
                    .decode("utf-8")
                )
            )
            for file in files
        ]
        return self.test_results

    def persist_result_to_s3(self, result: Result) -> bool:
        """
        Persist test result object to s3
        """
        boto3.client("s3").put_object(
            Bucket=get_global_config()["state_machine_aws_bucket"],
            Key=f"{AWS_TEST_RESULT_KEY}/"
            f"{self.get_name()}-{int(time.time() * 1000)}.json",
            Body=json.dumps(TestResult.from_result(result).__dict__),
        )

    def persist_to_s3(self) -> bool:
        """
        Persist test object to s3
        """
        boto3.client("s3").put_object(
            Bucket=get_global_config()["state_machine_aws_bucket"],
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
