from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from flaky import flaky
import shutil
import tempfile
import threading
import time
import unittest
import yaml
import copy

import ray
import ray.services as services
from ray.autoscaler.autoscaler import StandardAutoscaler, LoadMetrics, \
    fillout_defaults, validate_config
from ray.autoscaler.tags import TAG_RAY_NODE_TYPE, TAG_RAY_NODE_STATUS
from ray.autoscaler.node_provider import NODE_PROVIDERS, NodeProvider
import pytest


class MockNode(object):
    def __init__(self, node_id, tags):
        self.node_id = node_id
        self.state = "pending"
        self.tags = tags
        self.external_ip = "1.2.3.4"
        self.internal_ip = "172.0.0.{}".format(self.node_id)

    def matches(self, tags):
        for k, v in tags.items():
            if k not in self.tags or self.tags[k] != v:
                return False
        return True


class MockProcessRunner(object):
    def __init__(self, fail_cmds=[]):
        self.calls = []
        self.fail_cmds = fail_cmds

    def check_call(self, cmd, *args, **kwargs):
        for token in self.fail_cmds:
            if token in str(cmd):
                raise Exception("Failing command on purpose")
        self.calls.append(cmd)


class MockProvider(NodeProvider):
    def __init__(self):
        self.mock_nodes = {}
        self.next_id = 0
        self.throw = False
        self.fail_creates = False
        self.ready_to_create = threading.Event()
        self.ready_to_create.set()

    def non_terminated_nodes(self, tag_filters):
        if self.throw:
            raise Exception("oops")
        return [
            n.node_id for n in self.mock_nodes.values()
            if n.matches(tag_filters) and n.state != "terminated"
        ]

    def is_running(self, node_id):
        return self.mock_nodes[node_id].state == "running"

    def is_terminated(self, node_id):
        return self.mock_nodes[node_id].state == "terminated"

    def node_tags(self, node_id):
        return self.mock_nodes[node_id].tags

    def internal_ip(self, node_id):
        return self.mock_nodes[node_id].internal_ip

    def external_ip(self, node_id):
        return self.mock_nodes[node_id].external_ip

    def create_node(self, node_config, tags, count):
        self.ready_to_create.wait()
        if self.fail_creates:
            return
        for _ in range(count):
            self.mock_nodes[self.next_id] = MockNode(self.next_id, tags)
            self.next_id += 1

    def set_node_tags(self, node_id, tags):
        self.mock_nodes[node_id].tags.update(tags)

    def terminate_node(self, node_id):
        self.mock_nodes[node_id].state = "terminated"


SMALL_CLUSTER = {
    "cluster_name": "default",
    "min_workers": 2,
    "max_workers": 2,
    "initial_workers": 0,
    "target_utilization_fraction": 0.8,
    "idle_timeout_minutes": 5,
    "provider": {
        "type": "mock",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
    },
    "docker": {
        "image": "example",
        "container_name": "mock",
    },
    "auth": {
        "ssh_user": "ubuntu",
        "ssh_private_key": "/dev/null",
    },
    "head_node": {
        "TestProp": 1,
    },
    "worker_nodes": {
        "TestProp": 2,
    },
    "file_mounts": {},
    "initialization_commands": ["cmd0"],
    "setup_commands": ["cmd1"],
    "head_setup_commands": ["cmd2"],
    "worker_setup_commands": ["cmd3"],
    "head_start_ray_commands": ["start_ray_head"],
    "worker_start_ray_commands": ["start_ray_worker"],
}


class LoadMetricsTest(unittest.TestCase):
    def testUpdate(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 2}, {"CPU": 1})
        assert lm.approx_workers_used() == 0.5
        lm.update("1.1.1.1", {"CPU": 2}, {"CPU": 0})
        assert lm.approx_workers_used() == 1.0
        lm.update("2.2.2.2", {"CPU": 2}, {"CPU": 0})
        assert lm.approx_workers_used() == 2.0

    def testPruneByNodeIp(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 1}, {"CPU": 0})
        lm.update("2.2.2.2", {"CPU": 1}, {"CPU": 0})
        lm.prune_active_ips({"1.1.1.1", "4.4.4.4"})
        assert lm.approx_workers_used() == 1.0

    def testBottleneckResource(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 2}, {"CPU": 0})
        lm.update("2.2.2.2", {"CPU": 2, "GPU": 16}, {"CPU": 2, "GPU": 2})
        assert lm.approx_workers_used() == 1.88

    def testHeartbeat(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 2}, {"CPU": 1})
        lm.mark_active("2.2.2.2")
        assert "1.1.1.1" in lm.last_heartbeat_time_by_ip
        assert "2.2.2.2" in lm.last_heartbeat_time_by_ip
        assert "3.3.3.3" not in lm.last_heartbeat_time_by_ip

    def testDebugString(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 2}, {"CPU": 0})
        lm.update("2.2.2.2", {"CPU": 2, "GPU": 16}, {"CPU": 2, "GPU": 2})
        debug = lm.info_string()
        assert "ResourceUsage=2.0/4.0 CPU, 14.0/16.0 GPU" in debug
        assert "NumNodesConnected=2" in debug
        assert "NumNodesUsed=1.88" in debug


class AutoscalingTest(unittest.TestCase):
    def setUp(self):
        NODE_PROVIDERS["mock"] = \
            lambda: (None, self.create_provider)
        self.provider = None
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        del NODE_PROVIDERS["mock"]
        shutil.rmtree(self.tmpdir)
        ray.shutdown()

    def waitFor(self, condition, num_retries=50):
        for _ in range(num_retries):
            if condition():
                return
            time.sleep(.1)
        raise Exception("Timed out waiting for {}".format(condition))

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

    def testInvalidConfig(self):
        invalid_config = "/dev/null"
        with pytest.raises(ValueError):
            StandardAutoscaler(
                invalid_config, LoadMetrics(), update_interval_s=0)

    def testValidation(self):
        """Ensures that schema validation is working."""
        config = copy.deepcopy(SMALL_CLUSTER)
        try:
            validate_config(config)
        except Exception:
            self.fail("Test config did not pass validation test!")

        config["blah"] = "blah"
        with pytest.raises(ValueError):
            validate_config(config)
        del config["blah"]

        config["provider"]["blah"] = "blah"
        with pytest.raises(ValueError):
            validate_config(config)
        del config["provider"]["blah"]

        del config["provider"]
        with pytest.raises(ValueError):
            validate_config(config)

    def testValidateDefaultConfig(self):
        config = {}
        config["provider"] = {
            "type": "aws",
            "region": "us-east-1",
            "availability_zone": "us-east-1a",
        }
        config = fillout_defaults(config)
        try:
            validate_config(config)
        except Exception:
            self.fail("Default config did not pass validation test!")

    def testScaleUp(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_failures=0, update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(2)
        autoscaler.update()
        self.waitForNodes(2)

    def testTerminateOutdatedNodesGracefully(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 5
        config["max_workers"] = 5
        config_path = self.write_config(config)
        self.provider = MockProvider()
        self.provider.create_node({}, {TAG_RAY_NODE_TYPE: "worker"}, 10)
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_failures=0, update_interval_s=0)
        self.waitForNodes(10)

        # Gradually scales down to meet target size, never going too low
        for _ in range(10):
            autoscaler.update()
            self.waitForNodes(5, comparison=self.assertLessEqual)
            self.waitForNodes(4, comparison=self.assertGreaterEqual)

        # Eventually reaches steady state
        self.waitForNodes(5)

    def testDynamicScaling(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            update_interval_s=0)
        self.waitForNodes(0)
        autoscaler.update()
        self.waitForNodes(2)

        # Update the config to reduce the cluster size
        new_config = SMALL_CLUSTER.copy()
        new_config["max_workers"] = 1
        self.write_config(new_config)
        autoscaler.update()
        self.waitForNodes(1)

        # Update the config to reduce the cluster size
        new_config["min_workers"] = 10
        new_config["max_workers"] = 10
        self.write_config(new_config)
        autoscaler.update()
        self.waitForNodes(6)
        autoscaler.update()
        self.waitForNodes(10)

    def testInitialWorkers(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 0
        config["max_workers"] = 20
        config["initial_workers"] = 10
        config_path = self.write_config(config)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            update_interval_s=0)
        self.waitForNodes(0)
        autoscaler.update()
        self.waitForNodes(5)  # expected due to batch sizes and concurrency
        autoscaler.update()
        self.waitForNodes(10)
        autoscaler.update()

    def testDelayedLaunch(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0

        # Update will try to create, but will block until we set the flag
        self.provider.ready_to_create.clear()
        autoscaler.update()
        assert autoscaler.num_launches_pending.value == 2
        assert len(self.provider.non_terminated_nodes({})) == 0

        # Set the flag, check it updates
        self.provider.ready_to_create.set()
        self.waitForNodes(2)
        assert autoscaler.num_launches_pending.value == 0

        # Update the config to reduce the cluster size
        new_config = SMALL_CLUSTER.copy()
        new_config["max_workers"] = 1
        self.write_config(new_config)
        autoscaler.update()
        assert len(self.provider.non_terminated_nodes({})) == 1

    def testDelayedLaunchWithFailure(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 10
        config["max_workers"] = 10
        config_path = self.write_config(config)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_launch_batch=5,
            max_concurrent_launches=8,
            max_failures=0,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0

        # update() should launch a wave of 5 nodes (max_launch_batch)
        # Force this first wave to block.
        rtc1 = self.provider.ready_to_create
        rtc1.clear()
        autoscaler.update()
        # Synchronization: wait for launchy thread to be blocked on rtc1
        if hasattr(rtc1, '_cond'):  # Python 3.5
            waiters = rtc1._cond._waiters
        else:  # Python 2.7
            waiters = rtc1._Event__cond._Condition__waiters
        self.waitFor(lambda: len(waiters) == 1)
        assert autoscaler.num_launches_pending.value == 5
        assert len(self.provider.non_terminated_nodes({})) == 0

        # Call update() to launch a second wave of 3 nodes,
        # as 5 + 3 = 8 = max_concurrent_launches.
        # Make this wave complete immediately.
        rtc2 = threading.Event()
        self.provider.ready_to_create = rtc2
        rtc2.set()
        autoscaler.update()
        self.waitForNodes(3)
        assert autoscaler.num_launches_pending.value == 5

        # The first wave of 5 will now tragically fail
        self.provider.fail_creates = True
        rtc1.set()
        self.waitFor(lambda: autoscaler.num_launches_pending.value == 0)
        assert len(self.provider.non_terminated_nodes({})) == 3

        # Retry the first wave, allowing it to succeed this time
        self.provider.fail_creates = False
        autoscaler.update()
        self.waitForNodes(8)
        assert autoscaler.num_launches_pending.value == 0

        # Final wave of 2 nodes
        autoscaler.update()
        self.waitForNodes(10)
        assert autoscaler.num_launches_pending.value == 0

    def testUpdateThrottling(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            update_interval_s=10)
        autoscaler.update()
        self.waitForNodes(2)
        assert autoscaler.num_launches_pending.value == 0
        new_config = SMALL_CLUSTER.copy()
        new_config["max_workers"] = 1
        self.write_config(new_config)
        autoscaler.update()
        # not updated yet
        # note that node termination happens in the main thread, so
        # we do not need to add any delay here before checking
        assert len(self.provider.non_terminated_nodes({})) == 2
        assert autoscaler.num_launches_pending.value == 0

    def testLaunchConfigChange(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_failures=0, update_interval_s=0)
        autoscaler.update()
        self.waitForNodes(2)

        # Update the config to change the node type
        new_config = SMALL_CLUSTER.copy()
        new_config["worker_nodes"]["InstanceType"] = "updated"
        self.write_config(new_config)
        self.provider.ready_to_create.clear()
        for _ in range(5):
            autoscaler.update()
        self.waitForNodes(0)
        self.provider.ready_to_create.set()
        self.waitForNodes(2)

    def testIgnoresCorruptedConfig(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_launch_batch=10,
            max_concurrent_launches=10,
            max_failures=0,
            update_interval_s=0)
        autoscaler.update()
        self.waitForNodes(2)

        # Write a corrupted config
        self.write_config("asdf")
        for _ in range(10):
            autoscaler.update()
        time.sleep(0.1)
        assert autoscaler.num_launches_pending.value == 0
        assert len(self.provider.non_terminated_nodes({})) == 2

        # New a good config again
        new_config = SMALL_CLUSTER.copy()
        new_config["min_workers"] = 10
        new_config["max_workers"] = 10
        self.write_config(new_config)
        autoscaler.update()
        self.waitForNodes(10)

    def testMaxFailures(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        self.provider.throw = True
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_failures=2, update_interval_s=0)
        autoscaler.update()
        autoscaler.update()
        with pytest.raises(Exception):
            autoscaler.update()

    def testLaunchNewNodeOnOutOfBandTerminate(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_failures=0, update_interval_s=0)
        autoscaler.update()
        autoscaler.update()
        self.waitForNodes(2)
        for node in self.provider.mock_nodes.values():
            node.state = "terminated"
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(2)

    def testConfiguresNewNodes(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        autoscaler.update()
        autoscaler.update()
        self.waitForNodes(2)
        for node in self.provider.mock_nodes.values():
            node.state = "running"
        autoscaler.update()
        self.waitForNodes(2, tag_filters={TAG_RAY_NODE_STATUS: "up-to-date"})

    def testReportsConfigFailures(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner(fail_cmds=["cmd1"])
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        autoscaler.update()
        autoscaler.update()
        self.waitForNodes(2)
        for node in self.provider.mock_nodes.values():
            node.state = "running"
        autoscaler.update()
        self.waitForNodes(
            2, tag_filters={TAG_RAY_NODE_STATUS: "update-failed"})

    def testConfiguresOutdatedNodes(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        autoscaler.update()
        autoscaler.update()
        self.waitForNodes(2)
        for node in self.provider.mock_nodes.values():
            node.state = "running"
        autoscaler.update()
        self.waitForNodes(2, tag_filters={TAG_RAY_NODE_STATUS: "up-to-date"})
        runner.calls = []
        new_config = SMALL_CLUSTER.copy()
        new_config["worker_setup_commands"] = ["cmdX", "cmdY"]
        self.write_config(new_config)
        autoscaler.update()
        autoscaler.update()
        self.waitFor(lambda: len(runner.calls) > 0)

    def testScaleUpBasedOnLoad(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 1
        config["max_workers"] = 10
        config["target_utilization_fraction"] = 0.5
        config_path = self.write_config(config)
        self.provider = MockProvider()
        lm = LoadMetrics()
        autoscaler = StandardAutoscaler(
            config_path, lm, max_failures=0, update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(1)
        autoscaler.update()
        assert autoscaler.num_launches_pending.value == 0
        assert len(self.provider.non_terminated_nodes({})) == 1

        # Scales up as nodes are reported as used
        local_ip = services.get_node_ip_address()
        lm.update(local_ip, {"CPU": 2}, {"CPU": 0})  # head
        lm.update("172.0.0.0", {"CPU": 2}, {"CPU": 0})  # worker 1
        autoscaler.update()
        self.waitForNodes(3)
        lm.update("172.0.0.1", {"CPU": 2}, {"CPU": 0})
        autoscaler.update()
        self.waitForNodes(5)

        # Holds steady when load is removed
        lm.update("172.0.0.0", {"CPU": 2}, {"CPU": 2})
        lm.update("172.0.0.1", {"CPU": 2}, {"CPU": 2})
        autoscaler.update()
        assert autoscaler.num_launches_pending.value == 0
        assert len(self.provider.non_terminated_nodes({})) == 5

        # Scales down as nodes become unused
        lm.last_used_time_by_ip["172.0.0.0"] = 0
        lm.last_used_time_by_ip["172.0.0.1"] = 0
        autoscaler.update()
        assert autoscaler.num_launches_pending.value == 0
        assert len(self.provider.non_terminated_nodes({})) == 3
        lm.last_used_time_by_ip["172.0.0.2"] = 0
        lm.last_used_time_by_ip["172.0.0.3"] = 0
        autoscaler.update()
        assert autoscaler.num_launches_pending.value == 0
        assert len(self.provider.non_terminated_nodes({})) == 1

    def testDontScaleBelowTarget(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 0
        config["max_workers"] = 2
        config["target_utilization_fraction"] = 0.5
        config_path = self.write_config(config)
        self.provider = MockProvider()
        lm = LoadMetrics()
        autoscaler = StandardAutoscaler(
            config_path, lm, max_failures=0, update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        assert autoscaler.num_launches_pending.value == 0
        assert len(self.provider.non_terminated_nodes({})) == 0

        # Scales up as nodes are reported as used
        local_ip = services.get_node_ip_address()
        lm.update(local_ip, {"CPU": 2}, {"CPU": 0})  # head
        # 1.0 nodes used => target nodes = 2 => target workers = 1
        autoscaler.update()
        self.waitForNodes(1)

        # Make new node idle, and never used.
        # Should hold steady as target is still 2.
        lm.update("172.0.0.0", {"CPU": 0}, {"CPU": 0})
        lm.last_used_time_by_ip["172.0.0.0"] = 0
        autoscaler.update()
        assert len(self.provider.non_terminated_nodes({})) == 1

        # Reduce load on head => target nodes = 1 => target workers = 0
        lm.update(local_ip, {"CPU": 2}, {"CPU": 1})
        autoscaler.update()
        assert len(self.provider.non_terminated_nodes({})) == 0

    @flaky(max_runs=4)
    def testRecoverUnhealthyWorkers(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        lm = LoadMetrics()
        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        autoscaler.update()
        self.waitForNodes(2)
        for node in self.provider.mock_nodes.values():
            node.state = "running"
        autoscaler.update()
        self.waitForNodes(2, tag_filters={TAG_RAY_NODE_STATUS: "up-to-date"})

        # Mark a node as unhealthy
        lm.last_heartbeat_time_by_ip["172.0.0.0"] = 0
        num_calls = len(runner.calls)
        autoscaler.update()
        self.waitFor(lambda: len(runner.calls) > num_calls, num_retries=150)

    def testExternalNodeScaler(self):
        config = SMALL_CLUSTER.copy()
        config["provider"] = {
            "type": "external",
            "module": "ray.autoscaler.node_provider.NodeProvider",
        }
        config_path = self.write_config(config)
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_failures=0, update_interval_s=0)
        assert isinstance(autoscaler.provider, NodeProvider)

    def testExternalNodeScalerWrongImport(self):
        config = SMALL_CLUSTER.copy()
        config["provider"] = {
            "type": "external",
            "module": "mymodule.provider_class",
        }
        invalid_provider = self.write_config(config)
        with pytest.raises(ImportError):
            StandardAutoscaler(
                invalid_provider, LoadMetrics(), update_interval_s=0)

    def testExternalNodeScalerWrongModuleFormat(self):
        config = SMALL_CLUSTER.copy()
        config["provider"] = {
            "type": "external",
            "module": "does-not-exist",
        }
        invalid_provider = self.write_config(config)
        with pytest.raises(ValueError):
            StandardAutoscaler(
                invalid_provider, LoadMetrics(), update_interval_s=0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
