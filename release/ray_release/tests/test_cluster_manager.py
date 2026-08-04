import copy
import sys
import time
import unittest
from typing import Callable
from unittest.mock import MagicMock, patch

from freezegun import freeze_time

from ray_release.aws import RELEASE_AWS_RESOURCE_TYPES_TO_TRACK_FOR_BILLING
from ray_release.cluster_manager.minimal import MinimalClusterManager
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
        # Create a cluster manager with anyscale_sdk_2026=True
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

        # Top-level advanced_instance_config should have tags
        top_level_aic = cluster_manager.cluster_compute["advanced_instance_config"]
        self.assertIn("TagSpecifications", top_level_aic)

        # head_node and worker_nodes must NOT be auto-annotated when the
        # input compute config has no per-group advanced_instance_config.
        # Creating one (with TagSpecifications only) would cause Anyscale to
        # use the per-group spec and drop the cluster-level
        # IamInstanceProfile and other fields, since per-group spec replaces
        # (not merges with) the base.
        self.assertNotIn(
            "advanced_instance_config",
            cluster_manager.cluster_compute.get("head_node", {}),
        )
        for worker in cluster_manager.cluster_compute.get("worker_nodes", []):
            self.assertNotIn("advanced_instance_config", worker)

        # Verify tag values for all tracked resource types on the top-level spec
        tag_specs = top_level_aic["TagSpecifications"]
        self.assertEqual(
            len(tag_specs), len(RELEASE_AWS_RESOURCE_TYPES_TO_TRACK_FOR_BILLING)
        )
        for resource_type in RELEASE_AWS_RESOURCE_TYPES_TO_TRACK_FOR_BILLING:
            resource_tags = [
                ts for ts in tag_specs if ts["ResourceType"] == resource_type
            ]
            self.assertEqual(len(resource_tags), 1)
            self.assertIn({"Key": "foo", "Value": "bar"}, resource_tags[0]["Tags"])

        # When the input compute config already has per-group
        # advanced_instance_config, those per-group specs MUST be annotated
        # too. Anyscale replaces the base spec with the per-group spec when
        # one is present, so without annotation the billing tags would be
        # lost on that node group.
        cluster_compute_with_per_group = copy.deepcopy(TEST_CLUSTER_COMPUTE_NEW_SCHEMA)
        cluster_compute_with_per_group["head_node"]["advanced_instance_config"] = {
            "IamInstanceProfile": {"Name": "head-profile"},
        }
        cluster_compute_with_per_group["worker_nodes"][0][
            "advanced_instance_config"
        ] = {"IamInstanceProfile": {"Name": "worker-profile"}}
        cluster_manager.set_cluster_compute(
            cluster_compute_with_per_group, extra_tags={"foo": "bar"}
        )
        # No base advanced_instance_config in the input: do not auto-create
        # one. Only the per-group specs should carry the billing tags.
        self.assertNotIn("advanced_instance_config", cluster_manager.cluster_compute)
        head_aic = cluster_manager.cluster_compute["head_node"][
            "advanced_instance_config"
        ]
        self.assertEqual(head_aic["IamInstanceProfile"], {"Name": "head-profile"})
        self.assertIn("TagSpecifications", head_aic)
        worker_aic = cluster_manager.cluster_compute["worker_nodes"][0][
            "advanced_instance_config"
        ]
        self.assertEqual(worker_aic["IamInstanceProfile"], {"Name": "worker-profile"})
        self.assertIn("TagSpecifications", worker_aic)

        # When the input compute config has BOTH a base
        # advanced_instance_config and per-group ones, tags must land in
        # both places. Anyscale uses the base for any node group that
        # doesn't supply its own override.
        cluster_compute_with_base_and_per_group = copy.deepcopy(
            TEST_CLUSTER_COMPUTE_NEW_SCHEMA
        )
        cluster_compute_with_base_and_per_group["advanced_instance_config"] = {
            "IamInstanceProfile": {"Name": "base-profile"},
        }
        cluster_compute_with_base_and_per_group["head_node"][
            "advanced_instance_config"
        ] = {"IamInstanceProfile": {"Name": "head-profile"}}
        cluster_compute_with_base_and_per_group["worker_nodes"][0][
            "advanced_instance_config"
        ] = {"IamInstanceProfile": {"Name": "worker-profile"}}
        cluster_manager.set_cluster_compute(
            cluster_compute_with_base_and_per_group, extra_tags={"foo": "bar"}
        )
        base_aic = cluster_manager.cluster_compute["advanced_instance_config"]
        self.assertEqual(base_aic["IamInstanceProfile"], {"Name": "base-profile"})
        self.assertIn("TagSpecifications", base_aic)
        head_aic = cluster_manager.cluster_compute["head_node"][
            "advanced_instance_config"
        ]
        self.assertEqual(head_aic["IamInstanceProfile"], {"Name": "head-profile"})
        self.assertIn("TagSpecifications", head_aic)
        worker_aic = cluster_manager.cluster_compute["worker_nodes"][0][
            "advanced_instance_config"
        ]
        self.assertEqual(worker_aic["IamInstanceProfile"], {"Name": "worker-profile"})
        self.assertIn("TagSpecifications", worker_aic)

        # Test merging with already existing tags
        cluster_compute_with_tags = copy.deepcopy(TEST_CLUSTER_COMPUTE_NEW_SCHEMA)
        cluster_compute_with_tags["advanced_instance_config"] = {
            "TagSpecifications": [
                {"ResourceType": "fake", "Tags": []},
                {"ResourceType": "instance", "Tags": [{"Key": "key", "Value": "val"}]},
            ]
        }
        cluster_manager.set_cluster_compute(
            cluster_compute_with_tags, extra_tags={"foo": "bar"}
        )
        merged_aic = cluster_manager.cluster_compute["advanced_instance_config"]
        merged_specs = merged_aic["TagSpecifications"]
        # Should have: fake (preserved), instance (merged), volume (new)
        fake_tags = [ts for ts in merged_specs if ts["ResourceType"] == "fake"]
        self.assertEqual(len(fake_tags), 1)
        self.assertEqual(fake_tags[0]["Tags"], [])
        instance_tags = [ts for ts in merged_specs if ts["ResourceType"] == "instance"]
        self.assertEqual(len(instance_tags), 1)
        self.assertIn({"Key": "key", "Value": "val"}, instance_tags[0]["Tags"])
        self.assertIn({"Key": "foo", "Value": "bar"}, instance_tags[0]["Tags"])
        volume_tags = [ts for ts in merged_specs if ts["ResourceType"] == "volume"]
        self.assertEqual(len(volume_tags), 1)
        self.assertIn({"Key": "foo", "Value": "bar"}, volume_tags[0]["Tags"])

    def _make_new_schema_cluster_manager(
        self, env: str = "aws"
    ) -> MinimalClusterManager:
        sdk = MockSDK()
        sdk.returns["get_project"] = APIDict(result=APIDict(name="release_unit_tests"))
        return MinimalClusterManager(
            project_id=UNIT_TEST_PROJECT_ID,
            sdk=sdk,
            test=MockTest(
                {
                    "name": "unit_test_new_schema",
                    "env": env,
                    "cluster": {"byod": {}, "anyscale_sdk_2026": True},
                }
            ),
        )

    def testClusterComputeNewSchemaNoAic(self):
        # No advanced_instance_config anywhere: auto-create the base spec to
        # hold the billing tags. Per-group specs stay absent.
        cluster_manager = self._make_new_schema_cluster_manager()
        cluster_compute = copy.deepcopy(TEST_CLUSTER_COMPUTE_NEW_SCHEMA)
        cluster_manager.set_cluster_compute(cluster_compute, extra_tags={"foo": "bar"})
        self.assertIn(
            "TagSpecifications",
            cluster_manager.cluster_compute["advanced_instance_config"],
        )
        self.assertNotIn(
            "advanced_instance_config",
            cluster_manager.cluster_compute["head_node"],
        )
        for worker in cluster_manager.cluster_compute["worker_nodes"]:
            self.assertNotIn("advanced_instance_config", worker)

    def testClusterComputeNewSchemaNonAws(self):
        # Non-AWS cloud (gce): annotation should be skipped entirely.
        cluster_manager = self._make_new_schema_cluster_manager(env="gce")
        cluster_compute = copy.deepcopy(TEST_CLUSTER_COMPUTE_NEW_SCHEMA)
        cluster_manager.set_cluster_compute(cluster_compute, extra_tags={"foo": "bar"})
        self.assertNotIn("advanced_instance_config", cluster_manager.cluster_compute)

    def testClusterComputeNewSchemaPerGroupHeadOnly(self):
        # advanced_instance_config defined only on head_node (no base, no
        # worker): tag head only, don't auto-create base.
        cluster_manager = self._make_new_schema_cluster_manager()
        cluster_compute = copy.deepcopy(TEST_CLUSTER_COMPUTE_NEW_SCHEMA)
        cluster_compute["head_node"]["advanced_instance_config"] = {
            "IamInstanceProfile": {"Name": "head-profile"},
        }
        cluster_manager.set_cluster_compute(cluster_compute, extra_tags={"foo": "bar"})
        self.assertNotIn("advanced_instance_config", cluster_manager.cluster_compute)
        head_aic = cluster_manager.cluster_compute["head_node"][
            "advanced_instance_config"
        ]
        self.assertEqual(head_aic["IamInstanceProfile"], {"Name": "head-profile"})
        self.assertIn("TagSpecifications", head_aic)
        for worker in cluster_manager.cluster_compute["worker_nodes"]:
            self.assertNotIn("advanced_instance_config", worker)

    def testClusterComputeNewSchemaPerGroupWorkerOnly(self):
        # advanced_instance_config defined only on a worker (no base, no
        # head): tag that worker only, don't auto-create base.
        cluster_manager = self._make_new_schema_cluster_manager()
        cluster_compute = copy.deepcopy(TEST_CLUSTER_COMPUTE_NEW_SCHEMA)
        cluster_compute["worker_nodes"][0]["advanced_instance_config"] = {
            "IamInstanceProfile": {"Name": "worker-profile"},
        }
        cluster_manager.set_cluster_compute(cluster_compute, extra_tags={"foo": "bar"})
        self.assertNotIn("advanced_instance_config", cluster_manager.cluster_compute)
        self.assertNotIn(
            "advanced_instance_config",
            cluster_manager.cluster_compute["head_node"],
        )
        worker_aic = cluster_manager.cluster_compute["worker_nodes"][0][
            "advanced_instance_config"
        ]
        self.assertEqual(worker_aic["IamInstanceProfile"], {"Name": "worker-profile"})
        self.assertIn("TagSpecifications", worker_aic)

    @patch("time.sleep", lambda *a, **kw: None)
    def testFindCreateClusterEnvExisting(self):
        # Find existing env and succeed
        self.cluster_manager.set_cluster_env()
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
        self.cluster_manager.set_cluster_env()
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
        self.sdk.returns["create_byod_cluster_environment"] = fail_always
        with self.assertRaises(ClusterEnvCreateError):
            self.cluster_manager.create_cluster_env()
        # No cluster ID found or created
        self.assertFalse(self.cluster_manager.cluster_env_id)
        # Both APIs were called twice (retry after fail)
        self.assertEqual(self.sdk.call_counter["search_cluster_environments"], 2)
        self.assertEqual(self.sdk.call_counter["create_byod_cluster_environment"], 2)
        self.assertEqual(len(self.sdk.call_counter), 2)

    @patch("time.sleep", lambda *a, **kw: None)
    def testFindCreateClusterEnvFailSucceed(self):
        # No existing compute, create new, fail once, succeed afterwards
        self.cluster_manager.set_cluster_env()
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
        self.sdk.returns["create_byod_cluster_environment"] = fail_once(
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
        self.assertEqual(self.sdk.call_counter["create_byod_cluster_environment"], 2)
        self.assertEqual(len(self.sdk.call_counter), 2)

    @patch("time.sleep", lambda *a, **kw: None)
    def testFindCreateClusterEnvSucceed(self):
        # No existing compute, create new, and succeed
        self.cluster_manager.set_cluster_env()
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
        self.sdk.returns["create_byod_cluster_environment"] = APIDict(
            result=APIDict(
                id="correct",
            )
        )
        self.cluster_manager.create_cluster_env()
        # Both APIs were called twice (retry after fail)
        self.assertEqual(self.cluster_manager.cluster_env_id, "correct")
        self.assertEqual(self.sdk.call_counter["search_cluster_environments"], 1)
        self.assertEqual(self.sdk.call_counter["create_byod_cluster_environment"], 1)
        self.assertEqual(len(self.sdk.call_counter), 2)

    @patch("time.sleep", lambda *a, **kw: None)
    def testBuildClusterEnvNotFound(self):
        self.cluster_manager.set_cluster_env()
        self.cluster_manager.cluster_env_id = "correct"

        # Environment build not found
        self.sdk.returns["list_cluster_environment_builds"] = APIDict(results=[])
        with self.assertRaisesRegex(ClusterEnvBuildError, "No build found"):
            self.cluster_manager.build_cluster_env(timeout=600)

    @patch("time.sleep", lambda *a, **kw: None)
    def testBuildClusterEnvPreBuildFailed(self):
        """Pre-build fails, but is kicked off again."""
        self.cluster_manager.set_cluster_env()
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
        self.cluster_manager.set_cluster_env()
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
        self.cluster_manager.set_cluster_env()
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
        self.cluster_manager.set_cluster_env()
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
        self.cluster_manager.set_cluster_env()
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
        self.cluster_manager.set_cluster_env()
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


class MinimalSessionManagerNewSchemaTest(unittest.TestCase):
    """Tests for compute config creation using the anyscale.compute_config API."""

    def setUp(self) -> None:
        self.sdk = MockSDK()
        self.sdk.returns["get_project"] = APIDict(
            result=APIDict(name="release_unit_tests")
        )

        self.cluster_compute = TEST_CLUSTER_COMPUTE_NEW_SCHEMA

        self.cluster_manager = MinimalClusterManager(
            project_id=UNIT_TEST_PROJECT_ID,
            sdk=self.sdk,
            test=MockTest(
                {
                    "name": f"unit_test__{self.__class__.__name__}",
                    "cluster": {"byod": {}, "anyscale_sdk_2026": True},
                }
            ),
        )
        self.sdk.reset()

    @patch("time.sleep", lambda *a, **kw: None)
    def testFindExistingComputeConfig(self):
        """Find existing compute config via anyscale.compute_config.get."""
        self.cluster_manager.set_cluster_compute(self.cluster_compute)
        self.assertTrue(self.cluster_manager.cluster_compute_name)
        self.assertIsNone(self.cluster_manager.cluster_compute_id)

        mock_anyscale = MagicMock()
        self.cluster_manager.test.anyscale = mock_anyscale
        mock_anyscale.compute_config.get.return_value = APIDict(
            id="existing_id", name="test:1", config=None
        )

        self.cluster_manager.create_cluster_compute()

        self.assertEqual(self.cluster_manager.cluster_compute_id, "existing_id")
        mock_anyscale.compute_config.get.assert_called_once_with(
            self.cluster_manager.cluster_compute_name
        )
        mock_anyscale.compute_config.create.assert_not_called()

    @patch("time.sleep", lambda *a, **kw: None)
    def testCreateComputeConfigSucceed(self):
        """Compute config not found, create new, and succeed."""
        self.cluster_manager.set_cluster_compute(self.cluster_compute)
        self.assertTrue(self.cluster_manager.cluster_compute_name)
        self.assertIsNone(self.cluster_manager.cluster_compute_id)

        mock_anyscale = MagicMock()
        self.cluster_manager.test.anyscale = mock_anyscale
        # get: first call raises (not found), second call returns version
        mock_anyscale.compute_config.get.side_effect = [
            RuntimeError("not found"),
            APIDict(id="new_id", name="test:1", config=None),
        ]
        mock_anyscale.compute_config.create.return_value = "test:1"

        self.cluster_manager.create_cluster_compute()

        self.assertEqual(self.cluster_manager.cluster_compute_id, "new_id")
        mock_anyscale.compute_config.create.assert_called_once()
        self.assertEqual(mock_anyscale.compute_config.get.call_count, 2)

    @patch("time.sleep", lambda *a, **kw: None)
    def testCreateComputeConfigCreateFailFail(self):
        """Compute config not found, create fails both times."""
        self.cluster_manager.set_cluster_compute(self.cluster_compute)
        self.assertTrue(self.cluster_manager.cluster_compute_name)
        self.assertIsNone(self.cluster_manager.cluster_compute_id)

        mock_anyscale = MagicMock()
        self.cluster_manager.test.anyscale = mock_anyscale
        # get always raises (not found)
        mock_anyscale.compute_config.get.side_effect = RuntimeError("not found")
        # create always fails
        mock_anyscale.compute_config.create.side_effect = Exception("create failed")

        with self.assertRaises(ClusterComputeCreateError):
            self.cluster_manager.create_cluster_compute()

        self.assertIsNone(self.cluster_manager.cluster_compute_id)
        # get called once per attempt (lookup), create called once per attempt
        self.assertEqual(mock_anyscale.compute_config.get.call_count, 2)
        self.assertEqual(mock_anyscale.compute_config.create.call_count, 2)

    @patch("time.sleep", lambda *a, **kw: None)
    def testCreateComputeConfigCreateFailSucceed(self):
        """Compute config not found, create fails once, succeeds on retry."""
        self.cluster_manager.set_cluster_compute(self.cluster_compute)
        self.assertTrue(self.cluster_manager.cluster_compute_name)
        self.assertIsNone(self.cluster_manager.cluster_compute_id)

        mock_anyscale = MagicMock()
        self.cluster_manager.test.anyscale = mock_anyscale
        # get: raises on lookups (attempts 1 and 2), succeeds on post-create fetch
        mock_anyscale.compute_config.get.side_effect = [
            RuntimeError("not found"),  # attempt 1: lookup
            RuntimeError("not found"),  # attempt 2: lookup
            APIDict(id="new_id", name="test:1", config=None),  # attempt 2: post-create
        ]
        # create: fails once, then succeeds
        mock_anyscale.compute_config.create.side_effect = [
            Exception("create failed"),
            "test:1",
        ]

        self.cluster_manager.create_cluster_compute()

        self.assertEqual(self.cluster_manager.cluster_compute_id, "new_id")
        self.assertEqual(mock_anyscale.compute_config.get.call_count, 3)
        self.assertEqual(mock_anyscale.compute_config.create.call_count, 2)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
