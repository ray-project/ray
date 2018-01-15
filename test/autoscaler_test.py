from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import shutil
import tempfile
import time
import unittest
import yaml

import ray
from ray.autoscaler.autoscaler import StandardAutoscaler, LoadMetrics
from ray.autoscaler.tags import TAG_RAY_NODE_TYPE, TAG_RAY_NODE_STATUS
from ray.autoscaler.node_provider import NODE_PROVIDERS, NodeProvider
from ray.autoscaler.updater import NodeUpdaterThread


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

    def nodes(self, tag_filters):
        if self.throw:
            raise Exception("oops")
        return [
            n.node_id for n in self.mock_nodes.values()
            if n.matches(tag_filters) and n.state != "terminated"]

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
    "target_utilization_fraction": 0.8,
    "idle_timeout_minutes": 5,
    "provider": {
        "type": "mock",
        "region": "us-east-1",
        "availability_zone": "us-east-1a",
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
        self.assertEqual(lm.approx_workers_used(), 0.5)
        lm.update("1.1.1.1", {"CPU": 2}, {"CPU": 0})
        self.assertEqual(lm.approx_workers_used(), 1.0)
        lm.update("2.2.2.2", {"CPU": 2}, {"CPU": 0})
        self.assertEqual(lm.approx_workers_used(), 2.0)

    def testPruneByNodeIp(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 1}, {"CPU": 0})
        lm.update("2.2.2.2", {"CPU": 1}, {"CPU": 0})
        lm.prune_active_ips({"1.1.1.1", "4.4.4.4"})
        self.assertEqual(lm.approx_workers_used(), 1.0)

    def testBottleneckResource(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 2}, {"CPU": 0})
        lm.update("2.2.2.2", {"CPU": 2, "GPU": 16}, {"CPU": 2, "GPU": 2})
        self.assertEqual(lm.approx_workers_used(), 1.88)

    def testHeartbeat(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 2}, {"CPU": 1})
        lm.mark_active("2.2.2.2")
        self.assertIn("1.1.1.1", lm.last_heartbeat_time_by_ip)
        self.assertIn("2.2.2.2", lm.last_heartbeat_time_by_ip)
        self.assertNotIn("3.3.3.3", lm.last_heartbeat_time_by_ip)

    def testDebugString(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 2}, {"CPU": 0})
        lm.update("2.2.2.2", {"CPU": 2, "GPU": 16}, {"CPU": 2, "GPU": 2})
        debug = lm.debug_string()
        self.assertIn("ResourceUsage: 2.0/4.0 CPU, 14.0/16.0 GPU", debug)
        self.assertIn("NumNodesConnected: 2", debug)
        self.assertIn("NumNodesUsed: 1.88", debug)


class AutoscalingTest(unittest.TestCase):
    def setUp(self):
        NODE_PROVIDERS["mock"] = \
            lambda: (None, self.create_provider)
        self.provider = None
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        del NODE_PROVIDERS["mock"]
        shutil.rmtree(self.tmpdir)
        ray.worker.cleanup()

    def waitFor(self, condition):
        for _ in range(50):
            if condition():
                return
            time.sleep(.1)
        raise Exception("Timed out waiting for {}".format(condition))

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
        self.assertRaises(
            ValueError,
            lambda: StandardAutoscaler(
                invalid_config, LoadMetrics(), update_interval_s=0))

    def testScaleUp(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_failures=0, update_interval_s=0)
        self.assertEqual(len(self.provider.nodes({})), 0)
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 2)
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 2)

    def testTerminateOutdatedNodesGracefully(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 5
        config["max_workers"] = 5
        config_path = self.write_config(config)
        self.provider = MockProvider()
        self.provider.create_node({}, {TAG_RAY_NODE_TYPE: "Worker"}, 10)
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_failures=0, update_interval_s=0)
        self.assertEqual(len(self.provider.nodes({})), 10)

        # Gradually scales down to meet target size, never going too low
        for _ in range(10):
            autoscaler.update()
            self.assertLessEqual(len(self.provider.nodes({})), 5)
            self.assertGreaterEqual(len(self.provider.nodes({})), 4)

        # Eventually reaches steady state
        self.assertEqual(len(self.provider.nodes({})), 5)

    def testDynamicScaling(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_concurrent_launches=5,
            max_failures=0, update_interval_s=0)
        self.assertEqual(len(self.provider.nodes({})), 0)
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 2)

        # Update the config to reduce the cluster size
        new_config = SMALL_CLUSTER.copy()
        new_config["max_workers"] = 1
        self.write_config(new_config)
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 1)

        # Update the config to reduce the cluster size
        new_config["min_workers"] = 10
        new_config["max_workers"] = 10
        self.write_config(new_config)
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 6)
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 10)

    def testUpdateThrottling(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_concurrent_launches=5,
            max_failures=0, update_interval_s=10)
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 2)
        new_config = SMALL_CLUSTER.copy()
        new_config["max_workers"] = 1
        self.write_config(new_config)
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 2)  # not updated yet

    def testLaunchConfigChange(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_failures=0, update_interval_s=0)
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 2)

        # Update the config to change the node type
        new_config = SMALL_CLUSTER.copy()
        new_config["worker_nodes"]["InstanceType"] = "updated"
        self.write_config(new_config)
        existing_nodes = set(self.provider.nodes({}))
        for _ in range(5):
            autoscaler.update()
        new_nodes = set(self.provider.nodes({}))
        self.assertEqual(len(new_nodes), 2)
        self.assertEqual(len(new_nodes.intersection(existing_nodes)), 0)

    def testIgnoresCorruptedConfig(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_concurrent_launches=10,
            max_failures=0, update_interval_s=0)
        autoscaler.update()

        # Write a corrupted config
        self.write_config("asdf")
        for _ in range(10):
            autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 2)

        # New a good config again
        new_config = SMALL_CLUSTER.copy()
        new_config["min_workers"] = 10
        new_config["max_workers"] = 10
        self.write_config(new_config)
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 10)

    def testMaxFailures(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        self.provider.throw = True
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_failures=2, update_interval_s=0)
        autoscaler.update()
        autoscaler.update()
        self.assertRaises(Exception, autoscaler.update)

    def testAbortOnCreationFailures(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        self.provider.fail_creates = True
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_failures=0, update_interval_s=0)
        self.assertRaises(AssertionError, autoscaler.update)

    def testLaunchNewNodeOnOutOfBandTerminate(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_failures=0, update_interval_s=0)
        autoscaler.update()
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 2)
        for node in self.provider.mock_nodes.values():
            node.state = "terminated"
        self.assertEqual(len(self.provider.nodes({})), 0)
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 2)

    def testConfiguresNewNodes(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_failures=0, process_runner=runner,
            verbose_updates=True, node_updater_cls=NodeUpdaterThread,
            update_interval_s=0)
        autoscaler.update()
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 2)
        for node in self.provider.mock_nodes.values():
            node.state = "running"
        assert len(self.provider.nodes(
            {TAG_RAY_NODE_STATUS: "Uninitialized"})) == 2
        autoscaler.update()
        self.waitFor(
            lambda: len(self.provider.nodes(
                {TAG_RAY_NODE_STATUS: "Up-to-date"})) == 2)

    def testReportsConfigFailures(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner(fail_cmds=["cmd1"])
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_failures=0, process_runner=runner,
            verbose_updates=True, node_updater_cls=NodeUpdaterThread,
            update_interval_s=0)
        autoscaler.update()
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 2)
        for node in self.provider.mock_nodes.values():
            node.state = "running"
        assert len(self.provider.nodes(
            {TAG_RAY_NODE_STATUS: "Uninitialized"})) == 2
        autoscaler.update()
        self.waitFor(
            lambda: len(self.provider.nodes(
                {TAG_RAY_NODE_STATUS: "UpdateFailed"})) == 2)

    def testConfiguresOutdatedNodes(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path, LoadMetrics(), max_failures=0, process_runner=runner,
            verbose_updates=True, node_updater_cls=NodeUpdaterThread,
            update_interval_s=0)
        autoscaler.update()
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 2)
        for node in self.provider.mock_nodes.values():
            node.state = "running"
        autoscaler.update()
        self.waitFor(
            lambda: len(self.provider.nodes(
                {TAG_RAY_NODE_STATUS: "Up-to-date"})) == 2)
        runner.calls = []
        new_config = SMALL_CLUSTER.copy()
        new_config["worker_setup_commands"] = ["cmdX", "cmdY"]
        self.write_config(new_config)
        autoscaler.update()
        autoscaler.update()
        self.waitFor(lambda: len(runner.calls) > 0)

    def testScaleUpBasedOnLoad(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 2
        config["max_workers"] = 10
        config["target_utilization_fraction"] = 0.5
        config_path = self.write_config(config)
        self.provider = MockProvider()
        lm = LoadMetrics()
        autoscaler = StandardAutoscaler(
            config_path, lm, max_failures=0, update_interval_s=0)
        self.assertEqual(len(self.provider.nodes({})), 0)
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 2)
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 2)

        # Scales up as nodes are reported as used
        lm.update("172.0.0.0", {"CPU": 2}, {"CPU": 0})
        lm.update("172.0.0.1", {"CPU": 2}, {"CPU": 0})
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 4)
        lm.update("172.0.0.2", {"CPU": 2}, {"CPU": 0})
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 6)

        # Holds steady when load is removed
        lm.update("172.0.0.0", {"CPU": 2}, {"CPU": 2})
        lm.update("172.0.0.1", {"CPU": 2}, {"CPU": 2})
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 6)

        # Scales down as nodes become unused
        lm.last_used_time_by_ip["172.0.0.0"] = 0
        lm.last_used_time_by_ip["172.0.0.1"] = 0
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 4)
        lm.last_used_time_by_ip["172.0.0.2"] = 0
        lm.last_used_time_by_ip["172.0.0.3"] = 0
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 2)

    def testRecoverUnhealthyWorkers(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        lm = LoadMetrics()
        autoscaler = StandardAutoscaler(
            config_path, lm, max_failures=0, process_runner=runner,
            verbose_updates=True, node_updater_cls=NodeUpdaterThread,
            update_interval_s=0)
        autoscaler.update()
        for node in self.provider.mock_nodes.values():
            node.state = "running"
        autoscaler.update()
        self.waitFor(
            lambda: len(self.provider.nodes(
                {TAG_RAY_NODE_STATUS: "Up-to-date"})) == 2)

        # Mark a node as unhealthy
        lm.last_heartbeat_time_by_ip["172.0.0.0"] = 0
        num_calls = len(runner.calls)
        autoscaler.update()
        self.waitFor(lambda: len(runner.calls) > num_calls)


if __name__ == "__main__":
    unittest.main(verbosity=2)
