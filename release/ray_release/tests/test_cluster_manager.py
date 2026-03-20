import copy
import sys
import time
import unittest
from typing import Callable
from unittest.mock import patch

from ray_release.anyscale_util import create_cluster_env_from_image
from ray_release.cluster_manager.minimal import DefaultApi, MinimalClusterManager
from ray_release.exception import (
    ClusterComputeCreateError,
    ClusterEnvBuildError,
    ClusterEnvBuildTimeout,
    ClusterEnvCreateError,
)
from ray_release.test import Test
from ray_release.tests.utils import (
    UNIT_TEST_CLOUD_ID,
    UNIT_TEST_PROJECT_ID,
    APIDict,
    MockSDK,
    fail_always,
    fail_once,
)

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

TEST_CLUSTER_COMPUTE_NEW_SCHEMA = {
    "cloud": "test_cloud",
    "head_node": {
        "instance_type": "m5.4xlarge",
    },
    "worker_nodes": [
        {
            "instance_type": "m5.xlarge",
            "min_nodes": 0,
            "max_nodes": 4,
        }
    ],
}


def _fail(*args, **kwargs):
    raise RuntimeError()


class MockTest(Test):
    def get_anyscale_byod_image(self) -> str:
        return "anyscale"


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


def _make_default_api_proxy(sdk):
    """Return patchers that make DefaultApi methods delegate to MockSDK.

    DefaultApi.method(self.sdk, ...) is used in production code to bypass
    AnyscaleSDK deprecation overrides. These patches intercept the unbound
    method calls and route them to the MockSDK instance.
    """

    def _proxy(method_name):
        def proxy_fn(self_arg, *args, **kwargs):
            return getattr(sdk, method_name)(*args, **kwargs)

        return proxy_fn

    methods = [
        "search_cluster_computes",
        "create_cluster_compute",
        "list_cluster_environment_builds",
        "create_cluster_environment_build",
        "get_build",
        "search_cluster_environments",
        "create_byod_cluster_environment",
    ]
    return tuple(patch.object(DefaultApi, name, _proxy(name)) for name in methods)


class MinimalSessionManagerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.sdk = MockSDK()
        self.sdk.returns["get_project"] = APIDict(
            result=APIDict(name="release_unit_tests")
        )

        self.cluster_compute = TEST_CLUSTER_COMPUTE

        self.cluster_manager = MinimalClusterManager(
            project_id=UNIT_TEST_PROJECT_ID,
            sdk=self.sdk,
            test=MockTest(
                {
                    "name": f"unit_test__{self.__class__.__name__}",
                    "cluster": {"byod": {}},
                }
            ),
        )
        self.sdk.reset()

        # Patch DefaultApi methods to delegate to MockSDK
        self._api_patchers = _make_default_api_proxy(self.sdk)
        for p in self._api_patchers:
            p.start()

    def tearDown(self) -> None:
        for p in self._api_patchers:
            p.stop()

    def testClusterName(self):
        sdk = MockSDK()
        sdk.returns["get_project"] = APIDict(result=APIDict(name="release_unit_tests"))
        cluster_manager = MinimalClusterManager(
            test=MockTest({"name": "test"}),
            project_id=UNIT_TEST_PROJECT_ID,
            smoke_test=False,
            sdk=sdk,
        )
        self.assertRegex(cluster_manager.cluster_name, r"^test_\d+$")
        cluster_manager = MinimalClusterManager(
            test=MockTest({"name": "test"}),
            project_id=UNIT_TEST_PROJECT_ID,
            smoke_test=True,
            sdk=sdk,
        )
        self.assertRegex(cluster_manager.cluster_name, r"^test-smoke-test_\d+$")

    def testSetClusterEnv(self):
        sdk = MockSDK()
        sdk.returns["get_project"] = APIDict(result=APIDict(name="release_unit_tests"))
        cluster_manager = MinimalClusterManager(
            test=MockTest({"name": "test", "cluster": {"byod": {}}}),
            project_id=UNIT_TEST_PROJECT_ID,
            smoke_test=False,
            sdk=sdk,
        )
        cluster_manager.set_cluster_env()
        self.assertEqual(cluster_manager.cluster_env_name, "anyscale")

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

    @patch("time.sleep", lambda *a, **kw: None)
    @patch("ray_release.cluster_manager.minimal.anyscale.compute_config")
    def testNewSdkFindExisting(self, mock_cc):
        """New SDK path: find existing compute config."""
        sdk = MockSDK()
        sdk.returns["get_project"] = APIDict(result=APIDict(name="release_unit_tests"))
        cm = MinimalClusterManager(
            project_id=UNIT_TEST_PROJECT_ID,
            sdk=sdk,
            test=MockTest(
                {
                    "name": "unit_test_new_sdk",
                    "cluster": {"byod": {}, "anyscale_sdk_2026": True},
                }
            ),
        )
        cm.set_cluster_compute(copy.deepcopy(TEST_CLUSTER_COMPUTE_NEW_SCHEMA))

        mock_cc.list.return_value = APIDict(
            results=[
                APIDict(name=cm.cluster_compute_name + ":1", id="existing_id"),
            ]
        )
        cm.create_cluster_compute()
        self.assertEqual(cm.cluster_compute_id, "existing_id")
        mock_cc.list.assert_called_once_with(name=cm.cluster_compute_name)
        mock_cc.create.assert_not_called()

    @patch("time.sleep", lambda *a, **kw: None)
    @patch("ray_release.cluster_manager.minimal.anyscale.compute_config")
    def testNewSdkCreateSucceed(self, mock_cc):
        """New SDK path: no existing, create succeeds."""
        sdk = MockSDK()
        sdk.returns["get_project"] = APIDict(result=APIDict(name="release_unit_tests"))
        cm = MinimalClusterManager(
            project_id=UNIT_TEST_PROJECT_ID,
            sdk=sdk,
            test=MockTest(
                {
                    "name": "unit_test_new_sdk",
                    "cluster": {"byod": {}, "anyscale_sdk_2026": True},
                }
            ),
        )
        cm.set_cluster_compute(copy.deepcopy(TEST_CLUSTER_COMPUTE_NEW_SCHEMA))

        mock_cc.list.return_value = APIDict(results=[])
        mock_cc.create.return_value = cm.cluster_compute_name + ":1"
        mock_cc.get.return_value = APIDict(id="new_id")

        cm.create_cluster_compute()
        self.assertEqual(cm.cluster_compute_id, "new_id")
        mock_cc.create.assert_called_once()
        mock_cc.get.assert_called_once_with(cm.cluster_compute_name + ":1")

        # Verify that COMPUTE_CONFIG_FIELDS filtering works: set_cluster_compute
        # adds idle_termination_minutes/maximum_uptime_minutes to the dict, but
        # these must be excluded before passing to ComputeConfig (a frozen
        # dataclass that rejects unknown kwargs). The fact that create_cluster_compute
        # succeeded without TypeError proves filtering is working.
        self.assertIn("idle_termination_minutes", cm.cluster_compute)
        self.assertIn("maximum_uptime_minutes", cm.cluster_compute)

    @patch("time.sleep", lambda *a, **kw: None)
    @patch("ray_release.cluster_manager.minimal.anyscale.compute_config")
    def testNewSdkCreateFailFail(self, mock_cc):
        """New SDK path: create fails both times."""
        sdk = MockSDK()
        sdk.returns["get_project"] = APIDict(result=APIDict(name="release_unit_tests"))
        cm = MinimalClusterManager(
            project_id=UNIT_TEST_PROJECT_ID,
            sdk=sdk,
            test=MockTest(
                {
                    "name": "unit_test_new_sdk",
                    "cluster": {"byod": {}, "anyscale_sdk_2026": True},
                }
            ),
        )
        cm.set_cluster_compute(copy.deepcopy(TEST_CLUSTER_COMPUTE_NEW_SCHEMA))

        mock_cc.list.return_value = APIDict(results=[])
        mock_cc.create.side_effect = RuntimeError("API error")

        with self.assertRaises(ClusterComputeCreateError):
            cm.create_cluster_compute()
        self.assertIsNone(cm.cluster_compute_id)
        # Both list and create are called twice (retry after fail)
        self.assertEqual(mock_cc.list.call_count, 2)
        self.assertEqual(mock_cc.create.call_count, 2)

    def testClusterComputeExtraTags(self):
        self.cluster_manager.set_cluster_compute(self.cluster_compute)

        # No extra tags specified
        self.assertEqual(self.cluster_manager.cluster_compute, self.cluster_compute)

        # Extra tags specified
        self.cluster_manager.set_cluster_compute(
            self.cluster_compute, extra_tags={"foo": "bar"}
        )

        # All ResourceTypes as in
        # ray_release.aws.RELEASE_AWS_RESOURCE_TYPES_TO_TRACK_FOR_BILLING
        target_cluster_compute = TEST_CLUSTER_COMPUTE.copy()
        target_cluster_compute["advanced_configurations_json"] = {
            "TagSpecifications": [
                {"ResourceType": "instance", "Tags": [{"Key": "foo", "Value": "bar"}]},
                {"ResourceType": "volume", "Tags": [{"Key": "foo", "Value": "bar"}]},
            ]
        }
        self.assertEqual(
            self.cluster_manager.cluster_compute["advanced_configurations_json"],
            target_cluster_compute["advanced_configurations_json"],
        )

        # Test merging with already existing tags
        cluster_compute_with_tags = TEST_CLUSTER_COMPUTE.copy()
        cluster_compute_with_tags["advanced_configurations_json"] = {
            "TagSpecifications": [
                {"ResourceType": "fake", "Tags": []},
                {"ResourceType": "instance", "Tags": [{"Key": "key", "Value": "val"}]},
            ]
        }
        self.cluster_manager.set_cluster_compute(
            cluster_compute_with_tags, extra_tags={"foo": "bar"}
        )

        # All ResourceTypes as in RELEASE_AWS_RESOURCE_TYPES_TO_TRACK_FOR_BILLING
        target_cluster_compute = TEST_CLUSTER_COMPUTE.copy()
        target_cluster_compute["advanced_configurations_json"] = {
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
            ]
        }
        self.assertEqual(
            self.cluster_manager.cluster_compute["advanced_configurations_json"],
            target_cluster_compute["advanced_configurations_json"],
        )

    def testClusterComputeExtraTagsNewSchema(self):
        sdk = MockSDK()
        sdk.returns["get_project"] = APIDict(result=APIDict(name="release_unit_tests"))
        cluster_manager = MinimalClusterManager(
            project_id=UNIT_TEST_PROJECT_ID,
            sdk=sdk,
            test=MockTest(
                {
                    "name": "unit_test_new_schema",
                    "cluster": {"byod": {}, "anyscale_sdk_2026": True},
                }
            ),
        )

        cluster_compute = copy.deepcopy(TEST_CLUSTER_COMPUTE_NEW_SCHEMA)
        cluster_manager.set_cluster_compute(cluster_compute, extra_tags={"foo": "bar"})

        top_level_aic = cluster_manager.cluster_compute["advanced_instance_config"]
        self.assertIn("TagSpecifications", top_level_aic)

        head_aic = cluster_manager.cluster_compute["head_node"][
            "advanced_instance_config"
        ]
        self.assertIn("TagSpecifications", head_aic)

        worker_aic = cluster_manager.cluster_compute["worker_nodes"][0][
            "advanced_instance_config"
        ]
        self.assertIn("TagSpecifications", worker_aic)

        for aic in [top_level_aic, head_aic, worker_aic]:
            tag_specs = aic["TagSpecifications"]
            instance_tags = [ts for ts in tag_specs if ts["ResourceType"] == "instance"]
            self.assertEqual(len(instance_tags), 1)
            self.assertIn({"Key": "foo", "Value": "bar"}, instance_tags[0]["Tags"])


class NewSdkBuildClusterEnvTest(unittest.TestCase):
    """Tests for build_cluster_env() which polls anyscale.image.get()."""

    def _make_new_sdk_cm(self):
        sdk = MockSDK()
        sdk.returns["get_project"] = APIDict(result=APIDict(name="release_unit_tests"))
        cm = MinimalClusterManager(
            project_id=UNIT_TEST_PROJECT_ID,
            sdk=sdk,
            test=MockTest(
                {
                    "name": "unit_test_new_sdk_build",
                    "cluster": {"byod": {}, "anyscale_sdk_2026": True},
                }
            ),
        )
        cm.set_cluster_env()
        cm.cluster_env_id = "env_id"
        cm.cluster_env_build_id = None
        return cm

    @patch("time.sleep", lambda *a, **kw: None)
    @patch("ray_release.cluster_manager.minimal.anyscale.image")
    def testNewSdkBuildAlreadySucceeded(self, mock_image):
        cm = self._make_new_sdk_cm()
        mock_image.get.return_value = APIDict(
            name="test_image",
            latest_build_id="bld_123",
            latest_build_revision=3,
            latest_build_status="SUCCEEDED",
        )
        cm.build_cluster_env(timeout=600)
        self.assertEqual(cm.cluster_env_build_id, "anyscale/image/test_image:3")
        mock_image.get.assert_called_once_with(name=cm.cluster_env_name)

    @patch("time.sleep", lambda *a, **kw: None)
    @patch("ray_release.cluster_manager.minimal.anyscale.image")
    def testNewSdkBuildAlreadyFailed(self, mock_image):
        cm = self._make_new_sdk_cm()
        mock_image.get.return_value = APIDict(
            latest_build_id="bld_123",
            latest_build_status="FAILED",
            latest_image_uri=None,
        )
        with self.assertRaisesRegex(ClusterEnvBuildError, "build failed"):
            cm.build_cluster_env(timeout=600)
        self.assertIsNone(cm.cluster_env_build_id)

    @patch("time.sleep", lambda *a, **kw: None)
    @patch("ray_release.cluster_manager.minimal.anyscale.image")
    def testNewSdkBuildNoBuild(self, mock_image):
        cm = self._make_new_sdk_cm()
        mock_image.get.return_value = APIDict(
            latest_build_id=None,
            latest_build_status=None,
            latest_image_uri=None,
        )
        with self.assertRaisesRegex(ClusterEnvBuildError, "No build found"):
            cm.build_cluster_env(timeout=600)

    @patch("time.sleep", lambda *a, **kw: None)
    @patch("ray_release.cluster_manager.minimal.anyscale.image")
    def testNewSdkBuildInProgressThenSucceeds(self, mock_image):
        cm = self._make_new_sdk_cm()
        call_count = [0]

        def fake_get(name=None):
            call_count[0] += 1
            if call_count[0] <= 3:
                return APIDict(
                    name="test_image",
                    latest_build_id="bld_123",
                    latest_build_revision=5,
                    latest_build_status="IN_PROGRESS",
                )
            return APIDict(
                name="test_image",
                latest_build_id="bld_123",
                latest_build_revision=5,
                latest_build_status="SUCCEEDED",
            )

        mock_image.get.side_effect = fake_get
        cm.build_cluster_env(timeout=600)
        self.assertEqual(cm.cluster_env_build_id, "anyscale/image/test_image:5")
        self.assertGreaterEqual(mock_image.get.call_count, 4)

    @patch("time.sleep", lambda *a, **kw: None)
    @patch("ray_release.cluster_manager.minimal.anyscale.image")
    def testNewSdkBuildInProgressThenFails(self, mock_image):
        cm = self._make_new_sdk_cm()
        call_count = [0]

        def fake_get(name=None):
            call_count[0] += 1
            if call_count[0] <= 3:
                return APIDict(
                    latest_build_id="bld_123",
                    latest_build_status="IN_PROGRESS",
                    latest_image_uri=None,
                )
            return APIDict(
                latest_build_id="bld_123",
                latest_build_status="FAILED",
                latest_image_uri=None,
            )

        mock_image.get.side_effect = fake_get
        with self.assertRaisesRegex(ClusterEnvBuildError, "build failed"):
            cm.build_cluster_env(timeout=600)
        self.assertIsNone(cm.cluster_env_build_id)

    @patch("time.sleep", lambda *a, **kw: None)
    @patch("ray_release.cluster_manager.minimal.anyscale.image")
    def testNewSdkBuildTimeout(self, mock_image):
        cm = self._make_new_sdk_cm()
        mock_image.get.return_value = APIDict(
            latest_build_id="bld_123",
            latest_build_status="IN_PROGRESS",
            latest_image_uri=None,
        )
        with self.assertRaisesRegex(ClusterEnvBuildTimeout, "Time out when building"):
            cm.build_cluster_env(timeout=0)
        self.assertIsNone(cm.cluster_env_build_id)


class NewSdkCreateClusterEnvTest(unittest.TestCase):
    """Tests for _create_cluster_env_new_sdk() in anyscale_util."""

    @patch("ray_release.anyscale_util.anyscale.image")
    def testNewSdkFindExistingEnv(self, mock_image):
        mock_image.list.return_value = [
            APIDict(name="my_env", id="env_123"),
        ]
        result = create_cluster_env_from_image(
            image="docker/image:tag",
            test_name="test",
            runtime_env={},
            cluster_env_name="my_env",
            use_new_sdk=True,
        )
        self.assertEqual(result, "env_123")
        mock_image.list.assert_called_once_with(name="my_env")
        mock_image.register.assert_not_called()

    @patch("ray_release.anyscale_util.anyscale.image")
    def testNewSdkCreateEnvSucceed(self, mock_image):
        mock_image.list.return_value = []
        mock_image.get.return_value = APIDict(id="new_env_id")

        result = create_cluster_env_from_image(
            image="docker/image:tag",
            test_name="test",
            runtime_env={},
            cluster_env_name="my_env",
            use_new_sdk=True,
        )
        self.assertEqual(result, "new_env_id")
        mock_image.register.assert_called_once_with(
            "docker/image:tag", name="my_env", ray_version="nightly"
        )
        mock_image.get.assert_called_once_with(name="my_env")

    @patch("ray_release.anyscale_util.anyscale.image")
    def testNewSdkCreateEnvFail(self, mock_image):
        mock_image.list.return_value = []
        mock_image.register.side_effect = RuntimeError("API error")

        with self.assertRaises(ClusterEnvCreateError):
            create_cluster_env_from_image(
                image="docker/image:tag",
                test_name="test",
                runtime_env={},
                cluster_env_name="my_env",
                use_new_sdk=True,
            )

    @patch("ray_release.anyscale_util.anyscale.image")
    def testNewSdkExistingIdSkipsList(self, mock_image):
        """When cluster_env_id is already provided, skip listing."""
        result = create_cluster_env_from_image(
            image="docker/image:tag",
            test_name="test",
            runtime_env={},
            cluster_env_id="existing_id",
            cluster_env_name="my_env",
            use_new_sdk=True,
        )
        self.assertEqual(result, "existing_id")
        mock_image.list.assert_not_called()
        mock_image.register.assert_not_called()


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
