import pytest
import time
import yaml
import tempfile
import shutil
import unittest

import ray
from ray.tests.test_autoscaler import SMALL_CLUSTER, MockProvider, \
    MockProcessRunner
from ray.autoscaler.autoscaler import StandardAutoscaler
from ray.autoscaler.load_metrics import LoadMetrics
from ray.autoscaler.node_provider import NODE_PROVIDERS
from ray.autoscaler.commands import get_or_create_head_node
from ray.autoscaler.tags import TAG_RAY_USER_NODE_TYPE, TAG_RAY_NODE_KIND
from ray.autoscaler.resource_demand_scheduler import _utilization_score, \
    get_bin_pack_residual, get_nodes_for, ResourceDemandScheduler
from ray.test_utils import same_elements

from time import sleep

TYPES_A = {
    "empty_node": {
        "node_config": {
            "FooProperty": 42,
        },
        "resources": {},
        "max_workers": 0,
    },
    "m4.large": {
        "node_config": {},
        "resources": {
            "CPU": 2
        },
        "max_workers": 10,
    },
    "m4.4xlarge": {
        "node_config": {},
        "resources": {
            "CPU": 16
        },
        "max_workers": 8,
    },
    "m4.16xlarge": {
        "node_config": {},
        "resources": {
            "CPU": 64
        },
        "max_workers": 4,
    },
    "p2.xlarge": {
        "node_config": {},
        "resources": {
            "CPU": 16,
            "GPU": 1
        },
        "max_workers": 10,
    },
    "p2.8xlarge": {
        "node_config": {},
        "resources": {
            "CPU": 32,
            "GPU": 8
        },
        "max_workers": 4,
    },
}

MULTI_WORKER_CLUSTER = dict(
    SMALL_CLUSTER, **{
        "available_node_types": TYPES_A,
        "head_node_type": "empty_node",
        "worker_default_node_type": "m4.large",
    })


def test_util_score():
    assert _utilization_score({"CPU": 64}, [{"TPU": 16}]) is None
    assert _utilization_score({"GPU": 4}, [{"GPU": 2}]) == (0.5, 0.5)
    assert _utilization_score({"GPU": 4}, [{"GPU": 1}, {"GPU": 1}]) == \
        (0.5, 0.5)
    assert _utilization_score({"GPU": 2}, [{"GPU": 2}]) == (2, 2)
    assert _utilization_score({"GPU": 2}, [{"GPU": 1}, {"GPU": 1}]) == (2, 2)
    assert _utilization_score({"GPU": 2, "TPU": 1}, [{"GPU": 2}]) == (0, 1)
    assert _utilization_score({"CPU": 64}, [{"CPU": 64}]) == (64, 64)
    assert _utilization_score({"CPU": 64}, [{"CPU": 32}]) == (8, 8)
    assert _utilization_score({"CPU": 64}, [{"CPU": 16}, {"CPU": 16}]) == \
        (8, 8)


def test_bin_pack():
    assert get_bin_pack_residual([], [{"GPU": 2}, {"GPU": 2}]) == \
        [{"GPU": 2}, {"GPU": 2}]
    assert get_bin_pack_residual([{"GPU": 2}], [{"GPU": 2}, {"GPU": 2}]) == \
        [{"GPU": 2}]
    assert get_bin_pack_residual([{"GPU": 4}], [{"GPU": 2}, {"GPU": 2}]) == []
    arg = [{"GPU": 2}, {"GPU": 2, "CPU": 2}]
    assert get_bin_pack_residual(arg, [{"GPU": 2}, {"GPU": 2}]) == []
    arg = [{"CPU": 2}, {"GPU": 2}]
    assert get_bin_pack_residual(arg, [{"GPU": 2}, {"GPU": 2}]) == [{"GPU": 2}]


def test_get_nodes_packing_heuristic():
    assert get_nodes_for(TYPES_A, {}, 9999, [{"GPU": 8}]) == \
        [("p2.8xlarge", 1)]
    assert get_nodes_for(TYPES_A, {}, 9999, [{"GPU": 1}] * 6) == \
        [("p2.8xlarge", 1)]
    assert get_nodes_for(TYPES_A, {}, 9999, [{"GPU": 1}] * 4) == \
        [("p2.xlarge", 4)]
    assert get_nodes_for(TYPES_A, {}, 9999, [{"CPU": 32, "GPU": 1}] * 3) \
        == [("p2.8xlarge", 3)]
    assert get_nodes_for(TYPES_A, {}, 9999, [{"CPU": 64, "GPU": 1}] * 3) \
        == []
    assert get_nodes_for(TYPES_A, {}, 9999, [{"CPU": 64}] * 3) == \
        [("m4.16xlarge", 3)]
    assert get_nodes_for(TYPES_A, {}, 9999, [{"CPU": 64}, {"CPU": 1}]) \
        == [("m4.16xlarge", 1), ("m4.large", 1)]
    assert get_nodes_for(
        TYPES_A, {}, 9999, [{"CPU": 64}, {"CPU": 9}, {"CPU": 9}]) == \
        [("m4.16xlarge", 1), ("m4.4xlarge", 2)]
    assert get_nodes_for(TYPES_A, {}, 9999, [{"CPU": 16}] * 5) == \
        [("m4.16xlarge", 1), ("m4.4xlarge", 1)]
    assert get_nodes_for(TYPES_A, {}, 9999, [{"CPU": 8}] * 10) == \
        [("m4.16xlarge", 1), ("m4.4xlarge", 1)]
    assert get_nodes_for(TYPES_A, {}, 9999, [{"CPU": 1}] * 100) == \
        [("m4.16xlarge", 1), ("m4.4xlarge", 2), ("m4.large", 2)]
    assert get_nodes_for(
        TYPES_A, {}, 9999, [{"GPU": 1}] + ([{"CPU": 1}] * 64)) == \
        [("m4.16xlarge", 1), ("p2.xlarge", 1)]
    assert get_nodes_for(
        TYPES_A, {}, 9999, ([{"GPU": 1}] * 8) + ([{"CPU": 1}] * 64)) == \
        [("m4.16xlarge", 1), ("p2.8xlarge", 1)]


def test_get_nodes_respects_max_limit():
    types = {
        "m4.large": {
            "resources": {
                "CPU": 2
            },
            "max_workers": 10,
        },
        "gpu": {
            "resources": {
                "GPU": 1
            },
            "max_workers": 99999,
        },
    }
    assert get_nodes_for(types, {}, 2, [{"CPU": 1}] * 10) == \
        [("m4.large", 2)]
    assert get_nodes_for(types, {"m4.large": 9999}, 9999, [{
        "CPU": 1
    }] * 10) == []
    assert get_nodes_for(types, {"m4.large": 0}, 9999, [{
        "CPU": 1
    }] * 10) == [("m4.large", 5)]
    assert get_nodes_for(types, {"m4.large": 7}, 4, [{
        "CPU": 1
    }] * 10) == [("m4.large", 3)]
    assert get_nodes_for(types, {"m4.large": 7}, 2, [{
        "CPU": 1
    }] * 10) == [("m4.large", 2)]


def test_get_nodes_to_launch_limits():
    provider = MockProvider()
    scheduler = ResourceDemandScheduler(provider, TYPES_A, 3)

    provider.create_node({}, {TAG_RAY_USER_NODE_TYPE: "p2.8xlarge"}, 2)

    nodes = provider.non_terminated_nodes({})

    ips = provider.non_terminated_node_ips({})
    utilizations = {ip: {"GPU": 8} for ip in ips}

    to_launch = scheduler.get_nodes_to_launch(nodes, {"p2.8xlarge": 1}, [{
        "GPU": 8
    }] * 2, utilizations)
    assert to_launch == []


def test_calculate_node_resources():
    provider = MockProvider()
    scheduler = ResourceDemandScheduler(provider, TYPES_A, 10)

    provider.create_node({}, {TAG_RAY_USER_NODE_TYPE: "p2.8xlarge"}, 2)

    nodes = provider.non_terminated_nodes({})

    ips = provider.non_terminated_node_ips({})
    # 2 free p2.8xls
    utilizations = {ip: {"GPU": 8} for ip in ips}
    # 1 more on the way
    pending_nodes = {"p2.8xlarge": 1}
    # requires 4 p2.8xls (only 3 are in cluster/pending)
    demands = [{"GPU": 8}] * (len(utilizations) + 2)
    to_launch = scheduler.get_nodes_to_launch(nodes, pending_nodes, demands,
                                              utilizations)

    assert to_launch == [("p2.8xlarge", 1)]


class LoadMetricsTest(unittest.TestCase):
    def testResourceDemandVector(self):
        lm = LoadMetrics()
        lm.update(
            "1.1.1.1", {"CPU": 2}, {"CPU": 1}, {},
            waiting_bundles=[{
                "GPU": 1
            }],
            infeasible_bundles=[{
                "CPU": 16
            }])
        assert same_elements(lm.get_resource_demand_vector(), [{
            "CPU": 16
        }, {
            "GPU": 1
        }])


class AutoscalingTest(unittest.TestCase):
    def setUp(self):
        NODE_PROVIDERS["mock"] = \
            lambda config: self.create_provider
        self.provider = None
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        self.provider = None
        del NODE_PROVIDERS["mock"]
        shutil.rmtree(self.tmpdir)
        ray.shutdown()

    def waitForNodes(self, expected, comparison=None, tag_filters={}):
        MAX_ITER = 50
        for i in range(MAX_ITER):
            n = len(self.provider.non_terminated_nodes(tag_filters))
            if comparison is None:
                comparison = self.assertEqual
            try:
                comparison(n, expected)
                return
            except Exception:
                if i == MAX_ITER - 1:
                    raise
            time.sleep(.1)

    def create_provider(self, config, cluster_name):
        assert self.provider
        return self.provider

    def write_config(self, config):
        path = self.tmpdir + "/simple.yaml"
        with open(path, "w") as f:
            f.write(yaml.dump(config))
        return path

    def testGetOrCreateMultiNodeType(self):
        config_path = self.write_config(MULTI_WORKER_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        get_or_create_head_node(
            MULTI_WORKER_CLUSTER,
            config_path,
            no_restart=False,
            restart_only=False,
            yes=True,
            override_cluster_name=None,
            _provider=self.provider,
            _runner=runner)
        self.waitForNodes(1)
        runner.assert_has_call("1.2.3.4", "init_cmd")
        runner.assert_has_call("1.2.3.4", "setup_cmd")
        runner.assert_has_call("1.2.3.4", "start_ray_head")
        self.assertEqual(self.provider.mock_nodes[0].node_type, "empty_node")
        self.assertEqual(
            self.provider.mock_nodes[0].node_config.get("FooProperty"), 42)
        self.assertEqual(
            self.provider.mock_nodes[0].node_config.get("TestProp"), 1)
        self.assertEqual(
            self.provider.mock_nodes[0].tags.get(TAG_RAY_USER_NODE_TYPE),
            "empty_node")

    def testScaleUpMinSanity(self):
        config_path = self.write_config(MULTI_WORKER_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(2)
        autoscaler.update()
        self.waitForNodes(2)

    def testScaleUpIgnoreUsed(self):
        config = MULTI_WORKER_CLUSTER.copy()
        # Commenting out this line causes the test case to fail?!?!
        config["min_workers"] = 0
        config["target_utilization_fraction"] = 1.0
        config_path = self.write_config(config)
        self.provider = MockProvider()
        self.provider.create_node({}, {
            TAG_RAY_NODE_KIND: "head",
            TAG_RAY_USER_NODE_TYPE: "p2.xlarge"
        }, 1)
        head_ip = self.provider.non_terminated_node_ips({})[0]
        self.provider.finish_starting_nodes()
        runner = MockProcessRunner()
        lm = LoadMetrics(local_ip=head_ip)
        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        autoscaler.update()
        self.waitForNodes(1)
        lm.update(head_ip, {"CPU": 4, "GPU": 1}, {}, {})
        self.waitForNodes(1)

        lm.update(
            head_ip, {
                "CPU": 4,
                "GPU": 1
            }, {"GPU": 0}, {},
            waiting_bundles=[{
                "GPU": 1
            }])
        autoscaler.update()
        self.waitForNodes(2)
        assert self.provider.mock_nodes[1].node_type == "p2.xlarge"

    def testRequestBundlesAccountsForHeadNode(self):
        config = MULTI_WORKER_CLUSTER.copy()
        config["head_node_type"] = "p2.8xlarge"
        config["min_workers"] = 0
        config["max_workers"] = 50
        config_path = self.write_config(config)
        self.provider = MockProvider()
        self.provider.create_node({}, {
            TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
            TAG_RAY_NODE_KIND: "head"
        }, 1)
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 1

        # These requests fit on the head node.
        autoscaler.update()
        self.waitForNodes(1)
        autoscaler.request_resources([{"CPU": 1}])
        autoscaler.update()
        self.waitForNodes(1)
        assert len(self.provider.mock_nodes) == 1
        autoscaler.request_resources([{"GPU": 8}])
        autoscaler.update()
        self.waitForNodes(1)

        # This request requires an additional worker node.
        autoscaler.request_resources([{"GPU": 8}] * 2)
        autoscaler.update()
        self.waitForNodes(2)
        assert self.provider.mock_nodes[1].node_type == "p2.8xlarge"

    def testRequestBundles(self):
        config = MULTI_WORKER_CLUSTER.copy()
        config["min_workers"] = 0
        config["max_workers"] = 50
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(0)
        autoscaler.request_resources([{"CPU": 1}])
        autoscaler.update()
        self.waitForNodes(1)
        assert self.provider.mock_nodes[0].node_type == "m4.large"
        autoscaler.request_resources([{"GPU": 8}])
        autoscaler.update()
        self.waitForNodes(2)
        assert self.provider.mock_nodes[1].node_type == "p2.8xlarge"
        autoscaler.request_resources([{"CPU": 32}] * 4)
        autoscaler.update()
        self.waitForNodes(4)
        assert self.provider.mock_nodes[2].node_type == "m4.16xlarge"
        assert self.provider.mock_nodes[3].node_type == "m4.16xlarge"

    def testResourcePassing(self):
        config = MULTI_WORKER_CLUSTER.copy()
        config["min_workers"] = 0
        config["max_workers"] = 50
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(0)
        autoscaler.request_resources([{"CPU": 1}])
        autoscaler.update()
        self.waitForNodes(1)
        assert self.provider.mock_nodes[0].node_type == "m4.large"
        autoscaler.request_resources([{"GPU": 8}])
        autoscaler.update()
        self.waitForNodes(2)
        assert self.provider.mock_nodes[1].node_type == "p2.8xlarge"

        # TODO (Alex): Autoscaler creates the node during one update then
        # starts the updater in the enxt update. The sleep is largely
        # unavoidable because the updater runs in its own thread and we have no
        # good way of ensuring that the commands are sent in time.
        autoscaler.update()
        sleep(0.1)

        # These checks are done separately because we have no guarantees on the
        # order the dict is serialized in.
        runner.assert_has_call("172.0.0.0", "RAY_OVERRIDE_RESOURCES=")
        runner.assert_has_call("172.0.0.0", "\"CPU\":2")
        runner.assert_has_call("172.0.0.1", "RAY_OVERRIDE_RESOURCES=")
        runner.assert_has_call("172.0.0.1", "\"CPU\":32")
        runner.assert_has_call("172.0.0.1", "\"GPU\":8")

    def testScaleUpLoadMetrics(self):
        config = MULTI_WORKER_CLUSTER.copy()
        config["min_workers"] = 0
        config["max_workers"] = 50
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        lm = LoadMetrics()
        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(0)
        autoscaler.update()
        lm.update(
            "1.2.3.4", {}, {}, {},
            waiting_bundles=[{
                "GPU": 1
            }],
            infeasible_bundles=[{
                "CPU": 16
            }])
        autoscaler.update()
        self.waitForNodes(2)
        nodes = {
            self.provider.mock_nodes[0].node_type,
            self.provider.mock_nodes[1].node_type
        }
        assert nodes == {"p2.xlarge", "m4.4xlarge"}

    def testCommandPassing(self):
        t = "custom"
        config = MULTI_WORKER_CLUSTER.copy()
        config["available_node_types"]["p2.8xlarge"][
            "worker_setup_commands"] = ["new_worker_setup_command"]
        config["available_node_types"]["p2.xlarge"][
            "initialization_commands"] = ["new_worker_initialization_cmd"]
        config["available_node_types"]["p2.xlarge"]["resources"][t] = 1
        # Commenting out this line causes the test case to fail?!?!
        config["min_workers"] = 0
        config["max_workers"] = 10
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(0)
        autoscaler.request_resources([{"CPU": 1}])
        autoscaler.update()
        self.waitForNodes(1)
        assert self.provider.mock_nodes[0].node_type == "m4.large"
        autoscaler.request_resources([{"GPU": 8}])
        autoscaler.update()
        self.waitForNodes(2)
        assert self.provider.mock_nodes[1].node_type == "p2.8xlarge"
        autoscaler.request_resources([{"GPU": 1}] * 9)
        autoscaler.update()
        self.waitForNodes(3)
        assert self.provider.mock_nodes[2].node_type == "p2.xlarge"
        autoscaler.update()
        sleep(0.1)
        runner.assert_has_call(self.provider.mock_nodes[1].internal_ip,
                               "new_worker_setup_command")
        runner.assert_not_has_call(self.provider.mock_nodes[1].internal_ip,
                                   "setup_cmd")
        runner.assert_not_has_call(self.provider.mock_nodes[1].internal_ip,
                                   "worker_setup_cmd")
        runner.assert_has_call(self.provider.mock_nodes[2].internal_ip,
                               "new_worker_initialization_cmd")
        runner.assert_not_has_call(self.provider.mock_nodes[2].internal_ip,
                                   "init_cmd")

    def testDockerWorkers(self):
        config = MULTI_WORKER_CLUSTER.copy()
        config["available_node_types"]["p2.8xlarge"]["docker"] = {
            "worker_image": "p2.8x_image:latest",
            "worker_run_options": ["p2.8x-run-options"]
        }
        config["available_node_types"]["p2.xlarge"]["docker"] = {
            "worker_image": "p2x_image:nightly"
        }
        config["docker"]["worker_run_options"] = ["standard-run-options"]
        config["docker"]["image"] = "default-image:nightly"
        config["docker"]["worker_image"] = "default-image:nightly"
        # Commenting out this line causes the test case to fail?!?!
        config["min_workers"] = 0
        config["max_workers"] = 10
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(0)
        autoscaler.request_resources([{"CPU": 1}])
        autoscaler.update()
        self.waitForNodes(1)
        assert self.provider.mock_nodes[0].node_type == "m4.large"
        autoscaler.request_resources([{"GPU": 8}])
        autoscaler.update()
        self.waitForNodes(2)
        assert self.provider.mock_nodes[1].node_type == "p2.8xlarge"
        autoscaler.request_resources([{"GPU": 1}] * 9)
        autoscaler.update()
        self.waitForNodes(3)
        assert self.provider.mock_nodes[2].node_type == "p2.xlarge"
        autoscaler.update()
        # Fill up m4, p2.8, p2 and request 2 more CPUs
        autoscaler.request_resources([{
            "CPU": 2
        }, {
            "CPU": 16
        }, {
            "CPU": 32
        }, {
            "CPU": 2
        }])
        autoscaler.update()
        self.waitForNodes(4)
        assert self.provider.mock_nodes[3].node_type == "m4.16xlarge"
        autoscaler.update()
        sleep(0.1)
        runner.assert_has_call(self.provider.mock_nodes[1].internal_ip,
                               "p2.8x-run-options")
        runner.assert_has_call(self.provider.mock_nodes[1].internal_ip,
                               "p2.8x_image:latest")
        runner.assert_not_has_call(self.provider.mock_nodes[1].internal_ip,
                                   "default-image:nightly")
        runner.assert_not_has_call(self.provider.mock_nodes[1].internal_ip,
                                   "standard-run-options")

        runner.assert_has_call(self.provider.mock_nodes[2].internal_ip,
                               "p2x_image:nightly")
        runner.assert_has_call(self.provider.mock_nodes[2].internal_ip,
                               "standard-run-options")
        runner.assert_not_has_call(self.provider.mock_nodes[2].internal_ip,
                                   "p2.8x-run-options")

        runner.assert_has_call(self.provider.mock_nodes[3].internal_ip,
                               "default-image:nightly")
        runner.assert_has_call(self.provider.mock_nodes[3].internal_ip,
                               "standard-run-options")
        runner.assert_not_has_call(self.provider.mock_nodes[3].internal_ip,
                                   "p2.8x-run-options")
        runner.assert_not_has_call(self.provider.mock_nodes[3].internal_ip,
                                   "p2x_image:nightly")

    def testUpdateConfig(self):
        config = MULTI_WORKER_CLUSTER.copy()
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(2)
        config["min_workers"] = 0
        config["available_node_types"]["m4.large"]["node_config"][
            "field_changed"] = 1
        config_path = self.write_config(config)
        autoscaler.update()
        self.waitForNodes(0)

    def testEmptyDocker(self):
        config = MULTI_WORKER_CLUSTER.copy()
        del config["docker"]
        config["min_workers"] = 0
        config["max_workers"] = 10
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(0)
        autoscaler.request_resources([{"CPU": 1}])
        autoscaler.update()
        self.waitForNodes(1)
        assert self.provider.mock_nodes[0].node_type == "m4.large"
        autoscaler.request_resources([{"GPU": 8}])
        autoscaler.update()
        self.waitForNodes(2)
        assert self.provider.mock_nodes[1].node_type == "p2.8xlarge"


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
