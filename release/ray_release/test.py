import asyncio
import concurrent.futures
import enum
import os
import platform
import json
import time
from itertools import chain
from typing import Awaitable, Optional, List, Dict
from dataclasses import dataclass

import aioboto3
import boto3
from botocore.exceptions import ClientError
from github import Repository

from ray_release.aws import s3_put_rayci_test_data
from ray_release.configs.global_config import get_global_config
from ray_release.result import (
    ResultStatus,
    Result,
)
from ray_release.logger import logger
from ray_release.util import (
    dict_hash,
    get_read_state_machine_aws_bucket,
    get_write_state_machine_aws_bucket,
)

AWS_TEST_KEY = "ray_tests"
AWS_TEST_RESULT_KEY = "ray_test_results"
DEFAULT_PYTHON_VERSION = tuple(
    int(v) for v in os.environ.get("RELEASE_PY", "3.9").split(".")
)
DATAPLANE_ECR_REPO = "anyscale/ray"
DATAPLANE_ECR_ML_REPO = "anyscale/ray-ml"

MACOS_TEST_PREFIX = "darwin://"
LINUX_TEST_PREFIX = "linux://"
WINDOWS_TEST_PREFIX = "windows://"
MACOS_BISECT_DAILY_RATE_LIMIT = 3
LINUX_BISECT_DAILY_RATE_LIMIT = 0  # linux bisect is disabled
WINDOWS_BISECT_DAILY_RATE_LIMIT = 0  # windows bisect is disabled
BISECT_DAILY_RATE_LIMIT = 10

_asyncio_thread_pool = concurrent.futures.ThreadPoolExecutor()


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
    FLAKY = "flaky"
    CONSITENTLY_FAILING = "consistently_failing"
    PASSING = "passing"


class TestType(enum.Enum):
    """
    Type of the test
    """

    RELEASE_TEST = "release_test"
    MACOS_TEST = "macos_test"
    LINUX_TEST = "linux_test"
    WINDOWS_TEST = "windows_test"


@dataclass
class TestResult:
    status: str
    commit: str
    branch: str
    url: str
    timestamp: int
    pull_request: str
    rayci_step_id: str

    @classmethod
    def from_result(cls, result: Result):
        return cls(
            status=result.status,
            commit=os.environ.get("BUILDKITE_COMMIT", ""),
            branch=os.environ.get("BUILDKITE_BRANCH", ""),
            url=result.buildkite_url,
            timestamp=int(time.time() * 1000),
            pull_request=os.environ.get("BUILDKITE_PULL_REQUEST", ""),
            rayci_step_id=os.environ.get("RAYCI_STEP_ID", ""),
        )

    @classmethod
    def from_bazel_event(cls, event: dict):
        return cls.from_result(
            Result(
                status=ResultStatus.SUCCESS.value
                if event["testResult"]["status"] == "PASSED"
                else ResultStatus.ERROR.value,
                buildkite_url=(
                    f"{os.environ.get('BUILDKITE_BUILD_URL')}"
                    f"#{os.environ.get('BUILDKITE_JOB_ID')}"
                ),
            )
        )

    @classmethod
    def from_dict(cls, result: dict):
        return cls(
            status=result["status"],
            commit=result["commit"],
            branch=result.get("branch", ""),
            url=result["url"],
            timestamp=result["timestamp"],
            pull_request=result.get("pull_request", ""),
            rayci_step_id=result.get("rayci_step_id", ""),
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
    # a test is high impact if it catches regressions frequently
    KEY_IS_HIGH_IMPACT = "is_high_impact"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.test_results = None

    @classmethod
    def from_bazel_event(cls, event: dict, team: str):
        name = event["id"]["testResult"]["label"]
        system = platform.system().lower()
        return cls(
            {
                "name": f"{system}:{name}",
                "team": team,
            }
        )

    @classmethod
    def gen_from_name(cls, name: str):
        tests = [
            test
            for test in Test.gen_from_s3(cls._get_s3_name(name))
            if test["name"] == name
        ]
        return tests[0] if tests else None

    @classmethod
    def gen_from_s3(cls, prefix: str):
        """
        Obtain all tests whose names start with the given prefix from s3
        """
        bucket = get_read_state_machine_aws_bucket()
        s3_client = boto3.client("s3")
        pages = s3_client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket,
            Prefix=f"{AWS_TEST_KEY}/{prefix}",
        )
        files = chain.from_iterable([page.get("Contents", []) for page in pages])

        return [
            Test(
                json.loads(
                    s3_client.get_object(Bucket=bucket, Key=file["Key"])
                    .get("Body")
                    .read()
                    .decode("utf-8")
                )
            )
            for file in files
        ]

    @classmethod
    def gen_high_impact_tests(cls, prefix: str) -> Dict[str, List]:
        """
        Obtain the mapping from rayci step id to high impact tests with the given prefix
        """
        high_impact_tests = [
            test for test in cls.gen_from_s3(prefix) if test.is_high_impact()
        ]
        step_id_to_tests = {}
        for test in high_impact_tests:
            step_id = test.get_test_results(limit=1)[0].rayci_step_id
            if not step_id:
                continue
            step_id_to_tests[step_id] = step_id_to_tests.get(step_id, []) + [test]

        return step_id_to_tests

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

    def is_gce(self) -> bool:
        """
        Returns whether this test is running on GCE.
        """
        return self.get("env") == "gce"

    def is_byod_cluster(self) -> bool:
        """
        Returns whether this test is running on a BYOD cluster.
        """
        return self["cluster"].get("byod") is not None

    def is_high_impact(self) -> bool:
        # a test is high impact if it catches regressions frequently, this field is
        # populated by the determine_microcheck_tests.py script
        return self.get(self.KEY_IS_HIGH_IMPACT, None) == "true"

    def get_test_type(self) -> TestType:
        test_name = self.get_name()
        if test_name.startswith(MACOS_TEST_PREFIX):
            return TestType.MACOS_TEST
        if test_name.startswith(LINUX_TEST_PREFIX):
            return TestType.LINUX_TEST
        if test_name.startswith(WINDOWS_TEST_PREFIX):
            return TestType.WINDOWS_TEST
        return TestType.RELEASE_TEST

    def get_bisect_daily_rate_limit(self) -> int:
        test_type = self.get_test_type()
        if test_type == TestType.MACOS_TEST:
            return MACOS_BISECT_DAILY_RATE_LIMIT
        if test_type == TestType.LINUX_TEST:
            return LINUX_BISECT_DAILY_RATE_LIMIT
        if test_type == TestType.WINDOWS_TEST:
            return WINDOWS_BISECT_DAILY_RATE_LIMIT
        return BISECT_DAILY_RATE_LIMIT

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

    @classmethod
    def _get_s3_name(cls, test_name: str) -> str:
        """
        Returns the name of the test for s3. Since '/' is not allowed in s3 key,
        replace it with '_'.
        """
        return test_name.replace("/", "_")

    def get_oncall(self) -> str:
        """
        Returns the oncall for the test.
        """
        return self["team"]

    def update_from_s3(self, force_branch_bucket: bool = True) -> None:
        """
        Update test object with data fields that exist only on s3
        """
        try:
            data = (
                boto3.client("s3")
                .get_object(
                    Bucket=get_read_state_machine_aws_bucket(),
                    Key=f"{AWS_TEST_KEY}/{self._get_s3_name(self.get_name())}.json",
                )
                .get("Body")
                .read()
                .decode("utf-8")
            )
        except ClientError as e:
            logger.warning(f"Failed to update data for {self.get_name()} from s3:  {e}")
            return
        for key, value in json.loads(data).items():
            if key not in self:
                self[key] = value

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
        byod_image_tag = os.environ.get("RAY_IMAGE_TAG")
        if byod_image_tag:
            # Use the image tag specified in the environment variable.
            # TODO(can): this is a temporary backdoor that should be removed
            # once civ2 is fully rolled out.
            return byod_image_tag
        commit = os.environ.get(
            "COMMIT_TO_TEST",
            os.environ["BUILDKITE_COMMIT"],
        )
        branch = os.environ.get(
            "BRANCH_TO_TEST",
            os.environ["BUILDKITE_BRANCH"],
        )
        pr = os.environ.get("BUILDKITE_PULL_REQUEST", "false")
        ray_version = commit[:6]
        if pr != "false":
            ray_version = f"pr-{pr}.{ray_version}"
        elif branch.startswith("releases/"):
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

    def use_byod_ml_image(self) -> bool:
        """Returns whether to use the ML image for this test."""
        return self.get_byod_type() == "gpu"

    def get_byod_repo(self) -> str:
        """
        Returns the byod repo to use for this test.
        """
        if self.use_byod_ml_image():
            return DATAPLANE_ECR_ML_REPO
        return DATAPLANE_ECR_REPO

    def get_byod_ecr(self) -> str:
        """
        Returns the anyscale byod ecr to use for this test.
        """
        if self.is_gce():
            return get_global_config()["byod_gcp_cr"]
        byod_ecr = get_global_config()["byod_aws_cr"]
        if byod_ecr:
            return byod_ecr
        return get_global_config()["byod_ecr"]

    def get_ray_image(self) -> str:
        """
        Returns the ray docker image to use for this test.
        """
        config = get_global_config()
        repo = self.get_byod_repo()
        if repo == DATAPLANE_ECR_REPO:
            repo_name = config["byod_ray_cr_repo"]
        elif repo == DATAPLANE_ECR_ML_REPO:
            repo_name = config["byod_ray_ml_cr_repo"]
        else:
            raise ValueError(f"Unknown repo {repo}")

        ecr = config["byod_ray_ecr"]
        tag = self.get_byod_base_image_tag()
        return f"{ecr}/{repo_name}:{tag}"

    def get_anyscale_base_byod_image(self) -> str:
        """
        Returns the anyscale byod image to use for this test.
        """
        return (
            f"{self.get_byod_ecr()}/"
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
            f"{self.get_byod_ecr()}/"
            f"{self.get_byod_repo()}:{self.get_byod_image_tag()}"
        )

    def get_test_results(
        self, limit: int = 10, refresh: bool = False, aws_bucket: str = None
    ) -> List[TestResult]:
        """
        Get test result from test object, or s3

        :param limit: limit of test results to return
        :param refresh: whether to refresh the test results from s3
        """
        if self.test_results is not None and not refresh:
            return self.test_results

        bucket = aws_bucket or get_read_state_machine_aws_bucket()
        s3_client = boto3.client("s3")
        pages = s3_client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket,
            Prefix=f"{AWS_TEST_RESULT_KEY}/{self._get_s3_name(self.get_name())}-",
        )
        files = sorted(
            chain.from_iterable([page.get("Contents", []) for page in pages]),
            key=lambda file: int(file["LastModified"].timestamp()),
            reverse=True,
        )[:limit]
        self.test_results = _asyncio_thread_pool.submit(
            lambda: asyncio.run(
                self._gen_test_results(bucket, [file["Key"] for file in files])
            )
        ).result()

        return self.test_results

    async def _gen_test_results(
        self,
        bucket: str,
        keys: List[str],
    ) -> Awaitable[List[TestResult]]:
        session = aioboto3.Session()
        async with session.client("s3") as s3_client:
            return await asyncio.gather(
                *[self._gen_test_result(s3_client, bucket, key) for key in keys]
            )

    async def _gen_test_result(
        self,
        s3_client: aioboto3.Session.client,
        bucket: str,
        key: str,
    ) -> Awaitable[TestResult]:
        object = await s3_client.get_object(Bucket=bucket, Key=key)
        object_body = await object["Body"].read()

        return TestResult.from_dict(json.loads(object_body.decode("utf-8")))

    def persist_result_to_s3(self, result: Result) -> bool:
        """
        Persist result object to s3
        """
        self.persist_test_result_to_s3(TestResult.from_result(result))

    def persist_test_result_to_s3(self, test_result: TestResult) -> bool:
        """
        Persist test result object to s3
        """
        s3_put_rayci_test_data(
            Bucket=get_write_state_machine_aws_bucket(),
            Key=f"{AWS_TEST_RESULT_KEY}/"
            f"{self._get_s3_name(self.get_name())}-{int(time.time() * 1000)}.json",
            Body=json.dumps(test_result.__dict__),
        )

    def persist_to_s3(self) -> bool:
        """
        Persist test object to s3
        """
        s3_put_rayci_test_data(
            Bucket=get_write_state_machine_aws_bucket(),
            Key=f"{AWS_TEST_KEY}/{self._get_s3_name(self.get_name())}.json",
            Body=json.dumps(self),
        )


class TestDefinition(dict):
    """
    A class represents a definition of a test, such as test name, group, etc. Comparing
    to the test class, there are additional field, for example variations, which can be
    used to define several variations of a test.
    """

    pass
