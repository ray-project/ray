from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import shutil
import tempfile
import time
import unittest
import yaml

import ray
from ray.autoscaler.autoscaler import StandardAutoscaler
from ray.autoscaler.tags import TAG_RAY_NODE_TYPE, TAG_RAY_NODE_STATUS
from ray.autoscaler.node_provider import NODE_PROVIDERS, NodeProvider
from ray.autoscaler.updater import NodeUpdaterThread


class MockNode(object):
    def __init__(self, node_id, tags):
        self.node_id = node_id
        self.state = "pending"
        self.tags = tags
        self.external_ip = "1.2.3.4"

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
    "provider": {
        "type": "mock",
        "region": "us-east-1",
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
    "head_init_commands": ["cmd1", "cmd2"],
    "worker_init_commands": ["cmd1"],
}


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
            ValueError, lambda: StandardAutoscaler(invalid_config))

    def testScaleUp(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(config_path, max_failures=0)
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
        autoscaler = StandardAutoscaler(config_path, max_failures=0)
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
            config_path, max_concurrent_launches=5, max_failures=0)
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
        new_config["max_workers"] = 10
        self.write_config(new_config)
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 6)
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 10)

    def testLaunchConfigChange(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(config_path, max_failures=0)
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
            config_path, max_concurrent_launches=10, max_failures=0)
        autoscaler.update()

        # Write a corrupted config
        self.write_config("asdf")
        for _ in range(10):
            autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 2)

        # New a good config again
        new_config = SMALL_CLUSTER.copy()
        new_config["max_workers"] = 10
        self.write_config(new_config)
        autoscaler.update()
        self.assertEqual(len(self.provider.nodes({})), 10)

    def testMaxFailures(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        self.provider.throw = True
        autoscaler = StandardAutoscaler(config_path, max_failures=2)
        autoscaler.update()
        autoscaler.update()
        self.assertRaises(Exception, autoscaler.update)

    def testAbortOnCreationFailures(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        self.provider.fail_creates = True
        autoscaler = StandardAutoscaler(config_path, max_failures=0)
        self.assertRaises(AssertionError, autoscaler.update)

    def testLaunchNewNodeOnOutOfBandTerminate(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        autoscaler = StandardAutoscaler(config_path, max_failures=0)
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
            config_path, max_failures=0, process_runner=runner,
            verbose_updates=True, node_updater_cls=NodeUpdaterThread)
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
            config_path, max_failures=0, process_runner=runner,
            verbose_updates=True, node_updater_cls=NodeUpdaterThread)
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
            config_path, max_failures=0, process_runner=runner,
            verbose_updates=True, node_updater_cls=NodeUpdaterThread)
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
        new_config["worker_init_commands"] = ["cmdX", "cmdY"]
        self.write_config(new_config)
        autoscaler.update()
        autoscaler.update()
        self.waitFor(lambda: len(runner.calls) > 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
