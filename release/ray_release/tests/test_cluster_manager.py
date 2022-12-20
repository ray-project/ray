import os
import sys
import time
import unittest
from typing import Callable
from unittest.mock import patch
from uuid import uuid4

from freezegun import freeze_time

from ray_release.exception import (
    ClusterCreationError,
    ClusterStartupError,
    ClusterStartupTimeout,
    ClusterStartupFailed,
    ClusterEnvBuildError,
    ClusterEnvBuildTimeout,
    ClusterComputeCreateError,
    ClusterEnvCreateError,
)
from ray_release.cluster_manager.full import FullClusterManager
from ray_release.cluster_manager.minimal import MinimalClusterManager
from ray_release.tests.utils import (
    UNIT_TEST_PROJECT_ID,
    UNIT_TEST_CLOUD_ID,
    APIDict,
    fail_always,
    fail_once,
    MockSDK,
)
from ray_release.util import get_anyscale_sdk

TEST_CLUSTER_ENV = {
    "base_image": "anyscale/ray:nightly-py37",
    "env_vars": {},
    "python": {
        "pip_packages": [],
    },
    "conda_packages": [],
    "post_build_cmds": [f"echo {uuid4().hex[:8]}"],
}
TEST_CLUSTER_COMPUTE = {
    "cloud_id": UNIT_TEST_CLOUD_ID,
    "region": "us-west-2",
    "max_workers": 0,
    "head_node_type": {"name": "head_node", "instance_type": "m5.4xlarge"},
    "worker_node_types": [
        {
            "name": "worker_node",
            "instance_type": "m5.xlarge",
            "min_workers": 0,
            "max_workers": 0,
            "use_spot": False,
        }
    ],
}


def _fail(*args, **kwargs):
    raise RuntimeError()


class _DelayedResponse:
    def __init__(
        self,
        callback: Callable[[], None],
        finish_after: float,
        before: APIDict,
        after: APIDict,
    ):
        self.callback = callback
        self.finish_after = time.monotonic() + finish_after
        self.before = before
        self.after = after

    def __call__(self, *args, **kwargs):
        self.callback()
        if time.monotonic() > self.finish_after:
            return self.after
        else:
            return self.before


class MinimalSessionManagerTest(unittest.TestCase):
    cls = MinimalClusterManager

    def setUp(self) -> None:
        self.sdk = MockSDK()
        self.sdk.returns["get_project"] = APIDict(
            result=APIDict(name="release_unit_tests")
        )

        self.cluster_env = TEST_CLUSTER_ENV
        self.cluster_compute = TEST_CLUSTER_COMPUTE

        self.cluster_manager = self.cls(
            project_id=UNIT_TEST_PROJECT_ID,
            sdk=self.sdk,
            test_name=f"unit_test__{self.__class__.__name__}",
        )
        self.sdk.reset()

    def testClusterName(self):
        sdk = MockSDK()
        sdk.returns["get_project"] = APIDict(result=APIDict(name="release_unit_tests"))
        cluster_manager = self.cls(
            test_name="test", project_id=UNIT_TEST_PROJECT_ID, smoke_test=False, sdk=sdk
        )
        self.assertRegex(cluster_manager.cluster_name, r"^test_\d+$")
        cluster_manager = self.cls(
            test_name="test", project_id=UNIT_TEST_PROJECT_ID, smoke_test=True, sdk=sdk
        )
        self.assertRegex(cluster_manager.cluster_name, r"^test-smoke-test_\d+$")

    def testSetClusterEnv(self):
        sdk = MockSDK()
        sdk.returns["get_project"] = APIDict(result=APIDict(name="release_unit_tests"))
        cluster_manager = self.cls(
            test_name="test", project_id=UNIT_TEST_PROJECT_ID, smoke_test=False, sdk=sdk
        )
        cluster_manager.set_cluster_env({})
        self.assertEqual(
            cluster_manager.cluster_env["env_vars"]["RAY_USAGE_STATS_EXTRA_TAGS"],
            "test_name=test;smoke_test=False",
        )
        cluster_manager = self.cls(
            test_name="Test", project_id=UNIT_TEST_PROJECT_ID, smoke_test=True, sdk=sdk
        )
        cluster_manager.set_cluster_env({})
        self.assertEqual(
            cluster_manager.cluster_env["env_vars"]["RAY_USAGE_STATS_EXTRA_TAGS"],
            "test_name=Test;smoke_test=True",
        )

    @patch("time.sleep", lambda *a, **kw: None)
    def testFindCreateClusterComputeExisting(self):
        # Find existing compute and succeed
        self.cluster_manager.set_cluster_compute(self.cluster_compute)
        self.assertTrue(self.cluster_manager.cluster_compute_name)
        self.assertFalse(self.cluster_manager.cluster_compute_id)

        self.sdk.returns["search_cluster_computes"] = APIDict(
            metadata=APIDict(
                next_paging_token=None,
            ),
            results=[
                APIDict(
                    name="no_match",
                    id="wrong",
                ),
                APIDict(name=self.cluster_manager.cluster_compute_name, id="correct"),
            ],
        )
        self.cluster_manager.create_cluster_compute()
        self.assertEqual(self.cluster_manager.cluster_compute_id, "correct")
        self.assertEqual(self.sdk.call_counter["search_cluster_computes"], 1)
        self.assertEqual(len(self.sdk.call_counter), 1)

    @patch("time.sleep", lambda *a, **kw: None)
    def testFindCreateClusterComputeCreateFailFail(self):
        # No existing compute, create new, but fail both times
        self.cluster_manager.set_cluster_compute(self.cluster_compute)
        self.assertTrue(self.cluster_manager.cluster_compute_name)
        self.assertFalse(self.cluster_manager.cluster_compute_id)

        self.sdk.returns["search_cluster_computes"] = APIDict(
            metadata=APIDict(
                next_paging_token=None,
            ),
            results=[
                APIDict(
                    name="no_match",
                    id="wrong",
                ),
            ],
        )
        self.sdk.returns["create_cluster_compute"] = fail_always
        with self.assertRaises(ClusterComputeCreateError):
            self.cluster_manager.create_cluster_compute()
        # No cluster ID found or created
        self.assertFalse(self.cluster_manager.cluster_compute_id)
        # Both APIs were called twice (retry after fail)
        self.assertEqual(self.sdk.call_counter["search_cluster_computes"], 2)
        self.assertEqual(self.sdk.call_counter["create_cluster_compute"], 2)
        self.assertEqual(len(self.sdk.call_counter), 2)

    @patch("time.sleep", lambda *a, **kw: None)
    def testFindCreateClusterComputeCreateFailSucceed(self):
        # No existing compute, create new, fail once, succeed afterwards
        self.cluster_manager.set_cluster_compute(self.cluster_compute)
        self.assertTrue(self.cluster_manager.cluster_compute_name)
        self.assertFalse(self.cluster_manager.cluster_compute_id)

        self.sdk.returns["search_cluster_computes"] = APIDict(
            metadata=APIDict(
                next_paging_token=None,
            ),
            results=[
                APIDict(
                    name="no_match",
                    id="wrong",
                ),
            ],
        )
        self.sdk.returns["create_cluster_compute"] = fail_once(
            result=APIDict(
                result=APIDict(
                    id="correct",
                )
            )
        )
        self.cluster_manager.create_cluster_compute()
        # Both APIs were called twice (retry after fail)
        self.assertEqual(self.cluster_manager.cluster_compute_id, "correct")
        self.assertEqual(self.sdk.call_counter["search_cluster_computes"], 2)
        self.assertEqual(self.sdk.call_counter["create_cluster_compute"], 2)
        self.assertEqual(len(self.sdk.call_counter), 2)

    @patch("time.sleep", lambda *a, **kw: None)
    def testFindCreateClusterComputeCreateSucceed(self):
        # No existing compute, create new, and succeed
        self.cluster_manager.set_cluster_compute(self.cluster_compute)
        self.assertTrue(self.cluster_manager.cluster_compute_name)
        self.assertFalse(self.cluster_manager.cluster_compute_id)

        self.sdk.returns["search_cluster_computes"] = APIDict(
            metadata=APIDict(
                next_paging_token=None,
            ),
            results=[
                APIDict(
                    name="no_match",
                    id="wrong",
                ),
            ],
        )
        self.sdk.returns["create_cluster_compute"] = APIDict(
            result=APIDict(
                id="correct",
            )
        )
        self.cluster_manager.create_cluster_compute()
        # Both APIs were called twice (retry after fail)
        self.assertEqual(self.cluster_manager.cluster_compute_id, "correct")
        self.assertEqual(self.sdk.call_counter["search_cluster_computes"], 1)
        self.assertEqual(self.sdk.call_counter["create_cluster_compute"], 1)
        self.assertEqual(len(self.sdk.call_counter), 2)

        # Test automatic fields
        self.assertEqual(
            self.cluster_manager.cluster_compute["idle_termination_minutes"],
            self.cluster_manager.autosuspend_minutes,
        )
        self.assertEqual(
            self.cluster_manager.cluster_compute["maximum_uptime_minutes"],
            self.cluster_manager.maximum_uptime_minutes,
        )

    def testClusterComputeExtraTags(self):
        sdk = MockSDK()
        sdk.returns["get_cloud"] = APIDict(result=APIDict(provider="AWS"))
        sdk.returns["get_project"] = APIDict(result=APIDict(name="release_unit_tests"))
        cluster_manager = self.cls(
            test_name="test", project_id=UNIT_TEST_PROJECT_ID, smoke_test=False, sdk=sdk
        )
        cluster_manager.set_cluster_compute(self.cluster_compute)

        # No extra tags specified
        self.assertEqual(cluster_manager.cluster_compute, self.cluster_compute)

        # Extra tags specified
        cluster_manager.set_cluster_compute(
            self.cluster_compute, extra_tags={"foo": "bar"}
        )

        # All ResourceTypes as in
        # ray_release.aws.RELEASE_AWS_RESOURCE_TYPES_TO_TRACK_FOR_BILLING
        target_cluster_compute = TEST_CLUSTER_COMPUTE.copy()
        target_cluster_compute["aws"] = {
            "TagSpecifications": [
                {"ResourceType": "instance", "Tags": [{"Key": "foo", "Value": "bar"}]},
                {"ResourceType": "volume", "Tags": [{"Key": "foo", "Value": "bar"}]},
                {
                    "ResourceType": "host-reservation",
                    "Tags": [{"Key": "foo", "Value": "bar"}],
                },
            ]
        }
        self.assertEqual(
            cluster_manager.cluster_compute["aws"], target_cluster_compute["aws"]
        )

        # Test merging with already existing tags
        cluster_compute_with_tags = TEST_CLUSTER_COMPUTE.copy()
        cluster_compute_with_tags["aws"] = {
            "TagSpecifications": [
                {"ResourceType": "fake", "Tags": []},
                {"ResourceType": "instance", "Tags": [{"Key": "key", "Value": "val"}]},
            ]
        }
        cluster_manager.set_cluster_compute(
            cluster_compute_with_tags, extra_tags={"foo": "bar"}
        )

        # All ResourceTypes as in RELEASE_AWS_RESOURCE_TYPES_TO_TRACK_FOR_BILLING
        target_cluster_compute = TEST_CLUSTER_COMPUTE.copy()
        target_cluster_compute["aws"] = {
            "TagSpecifications": [
                {"ResourceType": "fake", "Tags": []},
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": "key", "Value": "val"},
                        {"Key": "foo", "Value": "bar"},
                    ],
                },
                {"ResourceType": "volume", "Tags": [{"Key": "foo", "Value": "bar"}]},
                {
                    "ResourceType": "host-reservation",
                    "Tags": [{"Key": "foo", "Value": "bar"}],
                },
            ]
        }
        self.assertEqual(
            cluster_manager.cluster_compute["aws"], target_cluster_compute["aws"]
        )

    @patch("time.sleep", lambda *a, **kw: None)
    def testFindCreateClusterEnvExisting(self):
        # Find existing env and succeed
        self.cluster_manager.set_cluster_env(self.cluster_env)
        self.assertTrue(self.cluster_manager.cluster_env_name)
        self.assertFalse(self.cluster_manager.cluster_env_id)

        self.sdk.returns["search_cluster_environments"] = APIDict(
            metadata=APIDict(
                next_paging_token=None,
            ),
            results=[
                APIDict(
                    name="no_match",
                    id="wrong",
                ),
                APIDict(name=self.cluster_manager.cluster_env_name, id="correct"),
            ],
        )
        self.cluster_manager.create_cluster_env()
        self.assertEqual(self.cluster_manager.cluster_env_id, "correct")
        self.assertEqual(self.sdk.call_counter["search_cluster_environments"], 1)
        self.assertEqual(len(self.sdk.call_counter), 1)

    @patch("time.sleep", lambda *a, **kw: None)
    def testFindCreateClusterEnvFailFail(self):
        # No existing compute, create new, but fail both times
        self.cluster_manager.set_cluster_env(self.cluster_env)
        self.assertTrue(self.cluster_manager.cluster_env_name)
        self.assertFalse(self.cluster_manager.cluster_env_id)

        self.sdk.returns["search_cluster_environments"] = APIDict(
            metadata=APIDict(
                next_paging_token=None,
            ),
            results=[
                APIDict(
                    name="no_match",
                    id="wrong",
                ),
            ],
        )
        self.sdk.returns["create_cluster_environment"] = fail_always
        with self.assertRaises(ClusterEnvCreateError):
            self.cluster_manager.create_cluster_env()
        # No cluster ID found or created
        self.assertFalse(self.cluster_manager.cluster_env_id)
        # Both APIs were called twice (retry after fail)
        self.assertEqual(self.sdk.call_counter["search_cluster_environments"], 2)
        self.assertEqual(self.sdk.call_counter["create_cluster_environment"], 2)
        self.assertEqual(len(self.sdk.call_counter), 2)

    @patch("time.sleep", lambda *a, **kw: None)
    def testFindCreateClusterEnvFailSucceed(self):
        # No existing compute, create new, fail once, succeed afterwards
        self.cluster_manager.set_cluster_env(self.cluster_env)
        self.assertTrue(self.cluster_manager.cluster_env_name)
        self.assertFalse(self.cluster_manager.cluster_env_id)

        self.cluster_manager.cluster_env_id = None
        self.sdk.reset()
        self.sdk.returns["search_cluster_environments"] = APIDict(
            metadata=APIDict(
                next_paging_token=None,
            ),
            results=[
                APIDict(
                    name="no_match",
                    id="wrong",
                ),
            ],
        )
        self.sdk.returns["create_cluster_environment"] = fail_once(
            result=APIDict(
                result=APIDict(
                    id="correct",
                )
            )
        )
        self.cluster_manager.create_cluster_env()
        # Both APIs were called twice (retry after fail)
        self.assertEqual(self.cluster_manager.cluster_env_id, "correct")
        self.assertEqual(self.sdk.call_counter["search_cluster_environments"], 2)
        self.assertEqual(self.sdk.call_counter["create_cluster_environment"], 2)
        self.assertEqual(len(self.sdk.call_counter), 2)

    @patch("time.sleep", lambda *a, **kw: None)
    def testFindCreateClusterEnvSucceed(self):
        # No existing compute, create new, and succeed
        self.cluster_manager.set_cluster_env(self.cluster_env)
        self.assertTrue(self.cluster_manager.cluster_env_name)
        self.assertFalse(self.cluster_manager.cluster_env_id)

        self.sdk.returns["search_cluster_environments"] = APIDict(
            metadata=APIDict(
                next_paging_token=None,
            ),
            results=[
                APIDict(
                    name="no_match",
                    id="wrong",
                ),
            ],
        )
        self.sdk.returns["create_cluster_environment"] = APIDict(
            result=APIDict(
                id="correct",
            )
        )
        self.cluster_manager.create_cluster_env()
        # Both APIs were called twice (retry after fail)
        self.assertEqual(self.cluster_manager.cluster_env_id, "correct")
        self.assertEqual(self.sdk.call_counter["search_cluster_environments"], 1)
        self.assertEqual(self.sdk.call_counter["create_cluster_environment"], 1)
        self.assertEqual(len(self.sdk.call_counter), 2)

    @patch("time.sleep", lambda *a, **kw: None)
    def testBuildClusterEnvNotFound(self):
        self.cluster_manager.set_cluster_env(self.cluster_env)
        self.cluster_manager.cluster_env_id = "correct"

        # Environment build not found
        self.sdk.returns["list_cluster_environment_builds"] = APIDict(results=[])
        with self.assertRaisesRegex(ClusterEnvBuildError, "No build found"):
            self.cluster_manager.build_cluster_env(timeout=600)

    @patch("time.sleep", lambda *a, **kw: None)
    def testBuildClusterEnvPreBuildFailed(self):
        """Pre-build fails, but is kicked off again."""
        self.cluster_manager.set_cluster_env(self.cluster_env)
        self.cluster_manager.cluster_env_id = "correct"

        # Build failed on first lookup
        self.cluster_manager.cluster_env_build_id = None
        self.sdk.reset()
        self.sdk.returns["list_cluster_environment_builds"] = APIDict(
            results=[
                APIDict(
                    id="build_failed",
                    status="failed",
                    created_at=0,
                    error_message=None,
                    config_json={},
                )
            ]
        )
        self.sdk.returns["create_cluster_environment_build"] = APIDict(
            result=APIDict(id="new_build_id")
        )
        self.sdk.returns["get_build"] = APIDict(
            result=APIDict(
                id="build_now_succeeded",
                status="failed",
                created_at=0,
                error_message=None,
                config_json={},
            )
        )
        with self.assertRaisesRegex(ClusterEnvBuildError, "Cluster env build failed"):
            self.cluster_manager.build_cluster_env(timeout=600)
        self.assertFalse(self.cluster_manager.cluster_env_build_id)
        self.assertEqual(self.sdk.call_counter["list_cluster_environment_builds"], 1)
        self.assertEqual(self.sdk.call_counter["create_cluster_environment_build"], 1)
        self.assertEqual(len(self.sdk.call_counter), 3)

    @patch("time.sleep", lambda *a, **kw: None)
    def testBuildClusterEnvPreBuildSucceeded(self):
        self.cluster_manager.set_cluster_env(self.cluster_env)
        self.cluster_manager.cluster_env_id = "correct"
        # (Second) build succeeded
        self.cluster_manager.cluster_env_build_id = None
        self.sdk.reset()
        self.sdk.returns["list_cluster_environment_builds"] = APIDict(
            results=[
                APIDict(
                    id="build_failed",
                    status="failed",
                    created_at=0,
                    error_message=None,
                    config_json={},
                ),
                APIDict(
                    id="build_succeeded",
                    status="succeeded",
                    created_at=1,
                    error_message=None,
                    config_json={},
                ),
            ]
        )
        self.cluster_manager.build_cluster_env(timeout=600)
        self.assertTrue(self.cluster_manager.cluster_env_build_id)
        self.assertEqual(self.cluster_manager.cluster_env_build_id, "build_succeeded")
        self.assertEqual(self.sdk.call_counter["list_cluster_environment_builds"], 1)
        self.assertEqual(len(self.sdk.call_counter), 1)

    @patch("time.sleep", lambda *a, **kw: None)
    def testBuildClusterEnvSelectLastBuild(self):
        self.cluster_manager.set_cluster_env(self.cluster_env)
        self.cluster_manager.cluster_env_id = "correct"
        # (Second) build succeeded
        self.cluster_manager.cluster_env_build_id = None
        self.sdk.reset()
        self.sdk.returns["list_cluster_environment_builds"] = APIDict(
            results=[
                APIDict(
                    id="build_succeeded",
                    status="succeeded",
                    created_at=0,
                    error_message=None,
                    config_json={},
                ),
                APIDict(
                    id="build_succeeded_2",
                    status="succeeded",
                    created_at=1,
                    error_message=None,
                    config_json={},
                ),
            ]
        )
        self.cluster_manager.build_cluster_env(timeout=600)
        self.assertTrue(self.cluster_manager.cluster_env_build_id)
        self.assertEqual(self.cluster_manager.cluster_env_build_id, "build_succeeded_2")
        self.assertEqual(self.sdk.call_counter["list_cluster_environment_builds"], 1)
        self.assertEqual(len(self.sdk.call_counter), 1)

    @patch("time.sleep", lambda *a, **kw: None)
    def testBuildClusterBuildFails(self):
        self.cluster_manager.set_cluster_env(self.cluster_env)
        self.cluster_manager.cluster_env_id = "correct"

        # Build, but fails after 300 seconds
        self.cluster_manager.cluster_env_build_id = None
        self.sdk.reset()
        self.sdk.returns["list_cluster_environment_builds"] = APIDict(
            results=[
                APIDict(
                    id="build_failed",
                    status="failed",
                    created_at=0,
                    error_message=None,
                    config_json={},
                ),
                APIDict(
                    id="build_succeeded",
                    status="pending",
                    created_at=1,
                    error_message=None,
                    config_json={},
                ),
            ]
        )
        with freeze_time() as frozen_time, self.assertRaisesRegex(
            ClusterEnvBuildError, "Cluster env build failed"
        ):
            self.sdk.returns["get_build"] = _DelayedResponse(
                lambda: frozen_time.tick(delta=10),
                finish_after=300,
                before=APIDict(
                    result=APIDict(
                        status="in_progress", error_message=None, config_json={}
                    )
                ),
                after=APIDict(
                    result=APIDict(status="failed", error_message=None, config_json={})
                ),
            )
            self.cluster_manager.build_cluster_env(timeout=600)

        self.assertFalse(self.cluster_manager.cluster_env_build_id)
        self.assertEqual(self.sdk.call_counter["list_cluster_environment_builds"], 1)
        self.assertGreaterEqual(self.sdk.call_counter["get_build"], 9)
        self.assertEqual(len(self.sdk.call_counter), 2)

    @patch("time.sleep", lambda *a, **kw: None)
    def testBuildClusterEnvBuildTimeout(self):
        self.cluster_manager.set_cluster_env(self.cluster_env)
        self.cluster_manager.cluster_env_id = "correct"

        # Build, but timeout after 100 seconds
        self.cluster_manager.cluster_env_build_id = None
        self.sdk.reset()
        self.sdk.returns["list_cluster_environment_builds"] = APIDict(
            results=[
                APIDict(
                    id="build_failed",
                    status="failed",
                    created_at=0,
                    error_message=None,
                    config_json={},
                ),
                APIDict(
                    id="build_succeeded",
                    status="pending",
                    created_at=1,
                    error_message=None,
                    config_json={},
                ),
            ]
        )
        with freeze_time() as frozen_time, self.assertRaisesRegex(
            ClusterEnvBuildTimeout, "Time out when building cluster env"
        ):
            self.sdk.returns["get_build"] = _DelayedResponse(
                lambda: frozen_time.tick(delta=10),
                finish_after=300,
                before=APIDict(
                    result=APIDict(
                        status="in_progress", error_message=None, config_json={}
                    )
                ),
                after=APIDict(
                    result=APIDict(
                        status="succeeded", error_message=None, config_json={}
                    )
                ),
            )
            self.cluster_manager.build_cluster_env(timeout=100)

        self.assertFalse(self.cluster_manager.cluster_env_build_id)
        self.assertEqual(self.sdk.call_counter["list_cluster_environment_builds"], 1)
        self.assertGreaterEqual(self.sdk.call_counter["get_build"], 9)
        self.assertEqual(len(self.sdk.call_counter), 2)

    @patch("time.sleep", lambda *a, **kw: None)
    def testBuildClusterBuildSucceed(self):
        self.cluster_manager.set_cluster_env(self.cluster_env)
        self.cluster_manager.cluster_env_id = "correct"
        # Build, succeed after 300 seconds
        self.cluster_manager.cluster_env_build_id = None
        self.sdk.reset()
        self.sdk.returns["list_cluster_environment_builds"] = APIDict(
            results=[
                APIDict(
                    id="build_failed",
                    status="failed",
                    created_at=0,
                    error_message=None,
                    config_json={},
                ),
                APIDict(
                    id="build_succeeded",
                    status="pending",
                    created_at=1,
                    error_message=None,
                    config_json={},
                ),
            ]
        )
        with freeze_time() as frozen_time:
            self.sdk.returns["get_build"] = _DelayedResponse(
                lambda: frozen_time.tick(delta=10),
                finish_after=300,
                before=APIDict(
                    result=APIDict(
                        status="in_progress", error_message=None, config_json={}
                    )
                ),
                after=APIDict(
                    result=APIDict(
                        status="succeeded", error_message=None, config_json={}
                    )
                ),
            )
            self.cluster_manager.build_cluster_env(timeout=600)

        self.assertTrue(self.cluster_manager.cluster_env_build_id)
        self.assertEqual(self.sdk.call_counter["list_cluster_environment_builds"], 1)
        self.assertGreaterEqual(self.sdk.call_counter["get_build"], 9)
        self.assertEqual(len(self.sdk.call_counter), 2)


class FullSessionManagerTest(MinimalSessionManagerTest):
    cls = FullClusterManager

    def testSessionStartCreationError(self):
        self.cluster_manager.cluster_env_id = "correct"
        self.cluster_manager.cluster_compute_id = "correct"

        self.sdk.returns["create_cluster"] = _fail

        with self.assertRaises(ClusterCreationError):
            self.cluster_manager.start_cluster()

    def testSessionStartStartupError(self):
        self.cluster_manager.cluster_env_id = "correct"
        self.cluster_manager.cluster_compute_id = "correct"

        self.sdk.returns["create_cluster"] = APIDict(result=APIDict(id="success"))
        self.sdk.returns["start_cluster"] = _fail

        with self.assertRaises(ClusterStartupError):
            self.cluster_manager.start_cluster()

    @patch("time.sleep", lambda *a, **kw: None)
    def testSessionStartStartupTimeout(self):
        self.cluster_manager.cluster_env_id = "correct"
        self.cluster_manager.cluster_compute_id = "correct"

        self.sdk.returns["create_cluster"] = APIDict(result=APIDict(id="success"))
        self.sdk.returns["start_cluster"] = APIDict(
            result=APIDict(id="cop_id", completed=False)
        )

        with freeze_time() as frozen_time, self.assertRaises(ClusterStartupTimeout):
            self.sdk.returns["get_cluster_operation"] = _DelayedResponse(
                lambda: frozen_time.tick(delta=10),
                finish_after=300,
                before=APIDict(result=APIDict(completed=False)),
                after=APIDict(result=APIDict(completed=True)),
            )

            # Timeout before startup finishes
            self.cluster_manager.start_cluster(timeout=200)

    @patch("time.sleep", lambda *a, **kw: None)
    def testSessionStartStartupFailed(self):
        self.cluster_manager.cluster_env_id = "correct"
        self.cluster_manager.cluster_compute_id = "correct"

        self.sdk.returns["create_cluster"] = APIDict(result=APIDict(id="success"))
        self.sdk.returns["start_cluster"] = APIDict(
            result=APIDict(id="cop_id", completed=False)
        )

        with freeze_time() as frozen_time, self.assertRaises(ClusterStartupFailed):
            frozen_time.tick(delta=0.1)
            self.sdk.returns["get_cluster_operation"] = _DelayedResponse(
                lambda: frozen_time.tick(delta=10),
                finish_after=300,
                before=APIDict(result=APIDict(completed=False)),
                after=APIDict(result=APIDict(completed=True)),
            )

            self.sdk.returns["get_cluster"] = APIDict(
                result=APIDict(state="Terminated")
            )

            # Timeout is long enough
            self.cluster_manager.start_cluster(timeout=400)

    @patch("time.sleep", lambda *a, **kw: None)
    def testSessionStartStartupSuccess(self):
        self.cluster_manager.cluster_env_id = "correct"
        self.cluster_manager.cluster_compute_id = "correct"

        self.sdk.returns["create_cluster"] = APIDict(result=APIDict(id="success"))
        self.sdk.returns["start_cluster"] = APIDict(
            result=APIDict(id="cop_id", completed=False)
        )

        with freeze_time() as frozen_time:
            frozen_time.tick(delta=0.1)
            self.sdk.returns["get_cluster_operation"] = _DelayedResponse(
                lambda: frozen_time.tick(delta=10),
                finish_after=300,
                before=APIDict(result=APIDict(completed=False)),
                after=APIDict(result=APIDict(completed=True)),
            )

            self.sdk.returns["get_cluster"] = APIDict(result=APIDict(state="Running"))

            # Timeout is long enough
            self.cluster_manager.start_cluster(timeout=400)


@unittest.skipUnless(
    os.environ.get("RELEASE_UNIT_TEST_NO_ANYSCALE", "0") == "1",
    reason="RELEASE_UNIT_TEST_NO_ANYSCALE is set to 1",
)
class LiveSessionManagerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.sdk = get_anyscale_sdk()

        self.cluster_env = TEST_CLUSTER_ENV
        self.cluster_compute = TEST_CLUSTER_COMPUTE

        self.cluster_manager = FullClusterManager(
            project_id=UNIT_TEST_PROJECT_ID,
            sdk=self.sdk,
            test_name=f"unit_test__{self.__class__.__name__}__endToEnd",
        )

    def tearDown(self) -> None:
        self.cluster_manager.terminate_cluster()
        self.cluster_manager.delete_configs()

    def testSessionEndToEnd(self):
        self.cluster_manager.set_cluster_env(self.cluster_env)
        self.cluster_manager.set_cluster_compute(self.cluster_compute)
        self.cluster_manager.build_configs(timeout=1200)

        # Reset, so that we fetch them again and test that code path
        self.cluster_manager.cluster_compute_id = None
        self.cluster_manager.cluster_env_id = None
        self.cluster_manager.cluster_env_build_id = None
        self.cluster_manager.build_configs(timeout=1200)

        # Start cluster
        self.cluster_manager.start_cluster(timeout=1200)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
