import os
import shutil
from subprocess import CalledProcessError
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock
import yaml
import copy
import sys
from jsonschema.exceptions import ValidationError

import ray
import ray._private.services as services
from ray.autoscaler._private.util import prepare_config, validate_config
from ray.autoscaler._private import commands
from ray.autoscaler.sdk import get_docker_host_mount_location
from ray.autoscaler._private.load_metrics import LoadMetrics
from ray.autoscaler._private.autoscaler import StandardAutoscaler
from ray.autoscaler._private.providers import (_NODE_PROVIDERS,
                                               _clear_provider_cache)
from ray.autoscaler.tags import TAG_RAY_NODE_KIND, TAG_RAY_NODE_STATUS, \
    STATUS_UP_TO_DATE, STATUS_UPDATE_FAILED, TAG_RAY_USER_NODE_TYPE
from ray.autoscaler.node_provider import NodeProvider
from ray.test_utils import RayTestTimeoutException
import pytest


class MockNode:
    def __init__(self, node_id, tags, node_config, node_type,
                 unique_ips=False):
        self.node_id = node_id
        self.state = "pending"
        self.tags = tags
        self.external_ip = "1.2.3.4"
        self.internal_ip = "172.0.0.{}".format(self.node_id)
        if unique_ips:
            self.external_ip = f"1.2.3.{self.node_id}"

        self.node_config = node_config
        self.node_type = node_type

    def matches(self, tags):
        for k, v in tags.items():
            if k not in self.tags or self.tags[k] != v:
                return False
        return True


class MockProcessRunner:
    def __init__(self, fail_cmds=None):
        self.calls = []
        self.fail_cmds = fail_cmds or []
        self.call_response = {}

    def check_call(self, cmd, *args, **kwargs):
        for token in self.fail_cmds:
            if token in str(cmd):
                raise CalledProcessError(1, token,
                                         "Failing command on purpose")
        self.calls.append(cmd)

    def check_output(self, cmd):
        self.check_call(cmd)
        return_string = "command-output"
        key_to_shrink = None
        for pattern, response_list in self.call_response.items():
            if pattern in str(cmd):
                return_string = response_list[0]
                key_to_shrink = pattern
                break
        if key_to_shrink:
            self.call_response[key_to_shrink] = self.call_response[
                key_to_shrink][1:]
            if len(self.call_response[key_to_shrink]) == 0:
                del self.call_response[key_to_shrink]

        return return_string.encode()

    def assert_has_call(self, ip, pattern=None, exact=None):
        assert pattern or exact, \
            "Must specify either a pattern or exact match."
        out = ""
        if pattern is not None:
            for cmd in self.command_history():
                if ip in cmd:
                    out += cmd
                    out += "\n"
            if pattern in out:
                return True
            else:
                raise Exception(
                    f"Did not find [{pattern}] in [{out}] for ip={ip}."
                    f"\n\nFull output: {self.command_history()}")
        elif exact is not None:
            exact_cmd = " ".join(exact)
            for cmd in self.command_history():
                if ip in cmd:
                    out += cmd
                    out += "\n"
                if cmd == exact_cmd:
                    return True
            raise Exception(
                f"Did not find [{exact_cmd}] in [{out}] for ip={ip}."
                f"\n\nFull output: {self.command_history()}")

    def assert_not_has_call(self, ip, pattern):
        out = ""
        for cmd in self.command_history():
            if ip in cmd:
                out += cmd
                out += "\n"
        if pattern in out:
            raise Exception("Found [{}] in [{}] for {}".format(
                pattern, out, ip))
        else:
            return True

    def clear_history(self):
        self.calls = []

    def command_history(self):
        return [" ".join(cmd) for cmd in self.calls]

    def respond_to_call(self, pattern, response_list):
        self.call_response[pattern] = response_list


class MockProvider(NodeProvider):
    def __init__(self, cache_stopped=False, unique_ips=False):
        self.mock_nodes = {}
        self.next_id = 0
        self.throw = False
        self.fail_creates = False
        self.ready_to_create = threading.Event()
        self.ready_to_create.set()
        self.cache_stopped = cache_stopped
        self.unique_ips = unique_ips
        # Many of these functions are called by node_launcher or updater in
        # different threads. This can be treated as a global lock for
        # everything.
        self.lock = threading.Lock()
        super().__init__(None, None)

    def non_terminated_nodes(self, tag_filters):
        with self.lock:
            if self.throw:
                raise Exception("oops")
            return [
                n.node_id for n in self.mock_nodes.values()
                if n.matches(tag_filters)
                and n.state not in ["stopped", "terminated"]
            ]

    def non_terminated_node_ips(self, tag_filters):
        with self.lock:
            if self.throw:
                raise Exception("oops")
            return [
                n.internal_ip for n in self.mock_nodes.values()
                if n.matches(tag_filters)
                and n.state not in ["stopped", "terminated"]
            ]

    def is_running(self, node_id):
        return self.mock_nodes[node_id].state == "running"

    def is_terminated(self, node_id):
        return self.mock_nodes[node_id].state in ["stopped", "terminated"]

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
        with self.lock:
            if self.cache_stopped:
                for node in self.mock_nodes.values():
                    if node.state == "stopped" and count > 0:
                        count -= 1
                        node.state = "pending"
                        node.tags.update(tags)
            for _ in range(count):
                self.mock_nodes[self.next_id] = MockNode(
                    self.next_id,
                    tags.copy(),
                    node_config,
                    tags.get(TAG_RAY_USER_NODE_TYPE),
                    unique_ips=self.unique_ips)
                self.next_id += 1

    def set_node_tags(self, node_id, tags):
        self.mock_nodes[node_id].tags.update(tags)

    def terminate_node(self, node_id):
        with self.lock:
            if self.cache_stopped:
                self.mock_nodes[node_id].state = "stopped"
            else:
                self.mock_nodes[node_id].state = "terminated"

    def finish_starting_nodes(self):
        with self.lock:
            for node in self.mock_nodes.values():
                if node.state == "pending":
                    node.state = "running"


SMALL_CLUSTER = {
    "cluster_name": "default",
    "min_workers": 2,
    "max_workers": 2,
    "initial_workers": 0,
    "autoscaling_mode": "default",
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
        "ssh_private_key": os.devnull,
    },
    "head_node": {
        "TestProp": 1,
    },
    "worker_nodes": {
        "TestProp": 2,
    },
    "file_mounts": {},
    "cluster_synced_files": [],
    "initialization_commands": ["init_cmd"],
    "setup_commands": ["setup_cmd"],
    "head_setup_commands": ["head_setup_cmd"],
    "worker_setup_commands": ["worker_setup_cmd"],
    "head_start_ray_commands": ["start_ray_head"],
    "worker_start_ray_commands": ["start_ray_worker"],
}


class LoadMetricsTest(unittest.TestCase):
    def testUpdate(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 2}, True, {"CPU": 1}, True, {})
        assert lm.approx_workers_used() == 0.5
        lm.update("1.1.1.1", {"CPU": 2}, True, {"CPU": 0}, True, {})
        assert lm.approx_workers_used() == 1.0
        lm.update("2.2.2.2", {"CPU": 2}, True, {"CPU": 0}, True, {})
        assert lm.approx_workers_used() == 2.0

    def testLoadMessages(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 2}, True, {"CPU": 1}, True, {})
        self.assertEqual(lm.approx_workers_used(), 0.5)
        lm.update("1.1.1.1", {"CPU": 2}, True, {"CPU": 1}, True, {"CPU": 1})
        self.assertEqual(lm.approx_workers_used(), 1.0)

        # Both nodes count as busy since there is a queue on one.
        lm.update("2.2.2.2", {"CPU": 2}, True, {"CPU": 2}, True, {})
        self.assertEqual(lm.approx_workers_used(), 2.0)
        lm.update("2.2.2.2", {"CPU": 2}, True, {"CPU": 0}, True, {})
        self.assertEqual(lm.approx_workers_used(), 2.0)
        lm.update("2.2.2.2", {"CPU": 2}, True, {"CPU": 1}, True, {})
        self.assertEqual(lm.approx_workers_used(), 2.0)

        # No queue anymore, so we're back to exact accounting.
        lm.update("1.1.1.1", {"CPU": 2}, True, {"CPU": 0}, True, {})
        self.assertEqual(lm.approx_workers_used(), 1.5)
        lm.update("2.2.2.2", {"CPU": 2}, True, {"CPU": 1}, True, {"GPU": 1})
        self.assertEqual(lm.approx_workers_used(), 2.0)

        lm.update("3.3.3.3", {"CPU": 2}, True, {"CPU": 1}, True, {})
        lm.update("4.3.3.3", {"CPU": 2}, True, {"CPU": 1}, True, {})
        lm.update("5.3.3.3", {"CPU": 2}, True, {"CPU": 1}, True, {})
        lm.update("6.3.3.3", {"CPU": 2}, True, {"CPU": 1}, True, {})
        lm.update("7.3.3.3", {"CPU": 2}, True, {"CPU": 1}, True, {})
        lm.update("8.3.3.3", {"CPU": 2}, True, {"CPU": 1}, True, {})
        self.assertEqual(lm.approx_workers_used(), 8.0)

        lm.update("2.2.2.2", {"CPU": 2}, True, {"CPU": 1}, True,
                  {})  # no queue anymore
        self.assertEqual(lm.approx_workers_used(), 4.5)

    def testLoadMessagesWithLightHeartbeat(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 2}, True, {"CPU": 1}, True, {})
        self.assertEqual(lm.approx_workers_used(), 0.5)
        lm.update("1.1.1.1", {}, False, {}, True, {"CPU": 1})
        self.assertEqual(lm.approx_workers_used(), 1.0)

        # Both nodes count as busy since there is a queue on one.
        lm.update("2.2.2.2", {"CPU": 2}, True, {"CPU": 2}, True, {})
        self.assertEqual(lm.approx_workers_used(), 2.0)
        lm.update("2.2.2.2", {}, True, {"CPU": 0}, False, {})
        self.assertEqual(lm.approx_workers_used(), 2.0)
        lm.update("2.2.2.2", {}, True, {"CPU": 1}, False, {})
        self.assertEqual(lm.approx_workers_used(), 2.0)

        # No queue anymore, so we're back to exact accounting.
        lm.update("1.1.1.1", {}, True, {"CPU": 0}, True, {})
        self.assertEqual(lm.approx_workers_used(), 1.5)
        lm.update("2.2.2.2", {}, False, {}, True, {"GPU": 1})
        self.assertEqual(lm.approx_workers_used(), 2.0)

        lm.update("3.3.3.3", {"CPU": 2}, True, {"CPU": 1}, True, {})
        lm.update("4.3.3.3", {"CPU": 2}, True, {"CPU": 1}, True, {})
        lm.update("5.3.3.3", {"CPU": 2}, True, {"CPU": 1}, True, {})
        lm.update("6.3.3.3", {"CPU": 2}, True, {"CPU": 1}, True, {})
        lm.update("7.3.3.3", {"CPU": 2}, True, {"CPU": 1}, True, {})
        lm.update("8.3.3.3", {"CPU": 2}, True, {"CPU": 1}, True, {})
        self.assertEqual(lm.approx_workers_used(), 8.0)

        lm.update("2.2.2.2", {}, False, {"CPU": 1}, True,
                  {})  # no queue anymore
        self.assertEqual(lm.approx_workers_used(), 4.5)

    def testPruneByNodeIp(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 1}, True, {"CPU": 0}, True, {})
        lm.update("2.2.2.2", {"CPU": 1}, True, {"CPU": 0}, True, {})
        lm.prune_active_ips({"1.1.1.1", "4.4.4.4"})
        assert lm.approx_workers_used() == 1.0

    def testBottleneckResource(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 2}, True, {"CPU": 0}, True, {})
        lm.update("2.2.2.2", {
            "CPU": 2,
            "GPU": 16
        }, True, {
            "CPU": 2,
            "GPU": 2
        }, True, {})
        assert lm.approx_workers_used() == 1.88

    def testHeartbeat(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 2}, True, {"CPU": 1}, True, {})
        lm.mark_active("2.2.2.2")
        assert "1.1.1.1" in lm.last_heartbeat_time_by_ip
        assert "2.2.2.2" in lm.last_heartbeat_time_by_ip
        assert "3.3.3.3" not in lm.last_heartbeat_time_by_ip

    def testDebugString(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 2}, True, {"CPU": 0}, True, {})
        lm.update("2.2.2.2", {
            "CPU": 2,
            "GPU": 16
        }, True, {
            "CPU": 2,
            "GPU": 2
        }, True, {})
        lm.update("3.3.3.3", {
            "memory": 20,
            "object_store_memory": 40
        }, True, {
            "memory": 0,
            "object_store_memory": 20
        }, True, {})
        debug = lm.info_string()
        assert ("ResourceUsage: 2.0/4.0 CPU, 14.0/16.0 GPU, "
                "1.05 GiB/1.05 GiB memory, "
                "1.05 GiB/2.1 GiB object_store_memory") in debug
        assert "NumNodesConnected: 3" in debug
        assert "NumNodesUsed: 2.88" in debug


class AutoscalingTest(unittest.TestCase):
    def setUp(self):
        _NODE_PROVIDERS["mock"] = \
            lambda config: self.create_provider
        self.provider = None
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        self.provider = None
        del _NODE_PROVIDERS["mock"]
        _clear_provider_cache()
        shutil.rmtree(self.tmpdir)
        ray.shutdown()

    def waitFor(self, condition, num_retries=50):
        for _ in range(num_retries):
            if condition():
                return
            time.sleep(.1)
        raise RayTestTimeoutException(
            "Timed out waiting for {}".format(condition))

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
        path = os.path.join(self.tmpdir, "simple.yaml")
        with open(path, "w") as f:
            f.write(yaml.dump(config))
        return path

    def testInvalidConfig(self):
        invalid_config = os.devnull
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
        with pytest.raises(ValidationError):
            validate_config(config)
        del config["blah"]

        del config["provider"]
        with pytest.raises(ValidationError):
            validate_config(config)

    def testValidateDefaultConfig(self):
        config = {}
        config["provider"] = {
            "type": "aws",
            "region": "us-east-1",
            "availability_zone": "us-east-1a",
        }
        config = prepare_config(config)
        try:
            validate_config(config)
        except ValidationError:
            self.fail("Default config did not pass validation test!")

    @unittest.skipIf(sys.platform == "win32", "Failing on Windows.")
    def testGetOrCreateHeadNode(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Mounts", ["[]"])
        # Two initial calls to docker cp, one before run, two final calls to cp
        runner.respond_to_call(".State.Running",
                               ["false", "false", "false", "true", "true"])
        commands.get_or_create_head_node(
            SMALL_CLUSTER,
            config_path,
            no_restart=False,
            restart_only=False,
            yes=True,
            override_cluster_name=None,
            _provider=self.provider,
            _runner=runner)
        self.waitForNodes(1)
        runner.assert_has_call("1.2.3.4", "init_cmd")
        runner.assert_has_call("1.2.3.4", "head_setup_cmd")
        runner.assert_has_call("1.2.3.4", "start_ray_head")
        self.assertEqual(self.provider.mock_nodes[0].node_type, None)
        runner.assert_has_call("1.2.3.4", pattern="docker run")

        docker_mount_prefix = get_docker_host_mount_location(
            SMALL_CLUSTER["cluster_name"])
        runner.assert_not_has_call(
            "1.2.3.4",
            pattern=f"-v {docker_mount_prefix}/~/ray_bootstrap_config")
        runner.assert_has_call(
            "1.2.3.4",
            pattern=f"docker cp {docker_mount_prefix}/~/ray_bootstrap_key.pem")
        pattern_to_assert = \
            f"docker cp {docker_mount_prefix}/~/ray_bootstrap_config.yaml"
        runner.assert_has_call("1.2.3.4", pattern=pattern_to_assert)

    @unittest.skipIf(sys.platform == "win32", "Failing on Windows.")
    def testRsyncCommandWithDocker(self):
        assert SMALL_CLUSTER["docker"]["container_name"]
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider(unique_ips=True)
        self.provider.create_node({}, {TAG_RAY_NODE_KIND: "head"}, 1)
        self.provider.create_node({}, {TAG_RAY_NODE_KIND: "worker"}, 10)
        self.provider.finish_starting_nodes()
        ray.autoscaler.node_provider._get_node_provider = Mock(
            return_value=self.provider)
        ray.autoscaler._private.commands._bootstrap_config = Mock(
            return_value=SMALL_CLUSTER)
        runner = MockProcessRunner()
        commands.rsync(
            config_path,
            source=config_path,
            target="/tmp/test_path",
            override_cluster_name=None,
            down=True,
            _runner=runner)
        runner.assert_has_call("1.2.3.0", pattern="docker cp")
        runner.assert_has_call("1.2.3.0", pattern="rsync")
        runner.clear_history()

        commands.rsync(
            config_path,
            source=config_path,
            target="/tmp/test_path",
            override_cluster_name=None,
            down=True,
            ip_address="1.2.3.5",
            _runner=runner)
        runner.assert_has_call("1.2.3.5", pattern="docker cp")
        runner.assert_has_call("1.2.3.5", pattern="rsync")
        runner.clear_history()

        commands.rsync(
            config_path,
            source=config_path,
            target="/tmp/test_path",
            ip_address="172.0.0.4",
            override_cluster_name=None,
            down=True,
            use_internal_ip=True,
            _runner=runner)
        runner.assert_has_call("172.0.0.4", pattern="docker cp")
        runner.assert_has_call("172.0.0.4", pattern="rsync")

    @unittest.skipIf(sys.platform == "win32", "Failing on Windows.")
    def testRsyncCommandWithoutDocker(self):
        cluster_cfg = SMALL_CLUSTER.copy()
        cluster_cfg["docker"] = {}
        config_path = self.write_config(cluster_cfg)
        self.provider = MockProvider(unique_ips=True)
        self.provider.create_node({}, {TAG_RAY_NODE_KIND: "head"}, 1)
        self.provider.create_node({}, {TAG_RAY_NODE_KIND: "worker"}, 10)
        self.provider.finish_starting_nodes()
        runner = MockProcessRunner()
        ray.autoscaler.node_provider._get_node_provider = Mock(
            return_value=self.provider)
        ray.autoscaler._private.commands._bootstrap_config = Mock(
            return_value=SMALL_CLUSTER)
        commands.rsync(
            config_path,
            source=config_path,
            target="/tmp/test_path",
            override_cluster_name=None,
            down=True,
            _runner=runner)
        runner.assert_has_call("1.2.3.0", pattern="rsync")

        commands.rsync(
            config_path,
            source=config_path,
            target="/tmp/test_path",
            override_cluster_name=None,
            down=True,
            ip_address="1.2.3.5",
            _runner=runner)
        runner.assert_has_call("1.2.3.5", pattern="rsync")
        runner.clear_history()

        commands.rsync(
            config_path,
            source=config_path,
            target="/tmp/test_path",
            override_cluster_name=None,
            down=True,
            ip_address="172.0.0.4",
            use_internal_ip=True,
            _runner=runner)
        runner.assert_has_call("172.0.0.4", pattern="rsync")
        runner.clear_history()

    def testScaleUp(self):
        config_path = self.write_config(SMALL_CLUSTER)
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

    def testManualAutoscaling(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 0
        config["max_workers"] = 50
        cores_per_node = 2
        config["worker_nodes"] = {"Resources": {"CPU": cores_per_node}}
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(0)
        autoscaler.request_resources({"CPU": cores_per_node * 10})
        for _ in range(5):  # Maximum launch batch is 5
            time.sleep(0.01)
            autoscaler.update()
        self.waitForNodes(10)
        autoscaler.request_resources({"CPU": cores_per_node * 30})
        for _ in range(4):  # Maximum launch batch is 5
            time.sleep(0.01)
            autoscaler.update()
        self.waitForNodes(30)

    def testTerminateOutdatedNodesGracefully(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 5
        config["max_workers"] = 5
        config_path = self.write_config(config)
        self.provider = MockProvider()
        self.provider.create_node({}, {TAG_RAY_NODE_KIND: "worker"}, 10)
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
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
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            process_runner=runner,
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
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        self.waitForNodes(0)
        autoscaler.update()
        self.waitForNodes(5)  # expected due to batch sizes and concurrency
        autoscaler.update()
        self.waitForNodes(10)
        autoscaler.update()

    def testAggressiveAutoscaling(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 0
        config["max_workers"] = 20
        config["initial_workers"] = 10
        config["idle_timeout_minutes"] = 0
        config["autoscaling_mode"] = "aggressive"
        config_path = self.write_config(config)

        self.provider = MockProvider()
        self.provider.create_node({}, {TAG_RAY_NODE_KIND: "head"}, 1)
        head_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: "head"}, )[0]
        runner = MockProcessRunner()

        lm = LoadMetrics()
        lm.local_ip = head_ip

        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)

        self.waitForNodes(1)
        autoscaler.update()
        self.waitForNodes(6)  # expected due to batch sizes and concurrency
        autoscaler.update()
        self.waitForNodes(11)

        # Connect the head and workers to end the bringup phase
        addrs = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: "worker"}, )
        addrs += head_ip
        for addr in addrs:
            lm.update(addr, {"CPU": 2}, True, {"CPU": 0}, True, {})
            lm.update(addr, {"CPU": 2}, True, {"CPU": 2}, True, {})
        assert autoscaler.bringup
        autoscaler.update()

        assert not autoscaler.bringup
        autoscaler.update()
        self.waitForNodes(1)

        # All of the nodes are down. Simulate some load on the head node
        lm.update(head_ip, {"CPU": 2}, True, {"CPU": 0}, True, {})

        autoscaler.update()
        self.waitForNodes(6)  # expected due to batch sizes and concurrency
        autoscaler.update()
        self.waitForNodes(11)

    def testUnmanagedNodes(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 0
        config["max_workers"] = 20
        config["initial_workers"] = 0
        config["idle_timeout_minutes"] = 0
        config["autoscaling_mode"] = "aggressive"
        config["target_utilization_fraction"] = 0.8
        config_path = self.write_config(config)

        self.provider = MockProvider()
        self.provider.create_node({}, {TAG_RAY_NODE_KIND: "head"}, 1)
        head_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: "head"}, )[0]

        self.provider.create_node({}, {TAG_RAY_NODE_KIND: "unmanaged"}, 1)
        unmanaged_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: "unmanaged"}, )[0]

        runner = MockProcessRunner()

        lm = LoadMetrics()
        lm.local_ip = head_ip

        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)

        autoscaler.update()
        self.waitForNodes(2)
        # This node has num_cpus=0
        lm.update(head_ip, {"CPU": 1}, True, {"CPU": 0}, True, {})
        lm.update(unmanaged_ip, {"CPU": 0}, True, {"CPU": 0}, True, {})
        autoscaler.update()
        self.waitForNodes(2)
        # 1 CPU task cannot be scheduled.
        lm.update(unmanaged_ip, {"CPU": 0}, True, {"CPU": 0}, True, {"CPU": 1})
        autoscaler.update()
        self.waitForNodes(3)

    def testUnmanagedNodes2(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 0
        config["max_workers"] = 20
        config["initial_workers"] = 0
        config["idle_timeout_minutes"] = 0
        config["autoscaling_mode"] = "aggressive"
        config["target_utilization_fraction"] = 1.0
        config_path = self.write_config(config)

        self.provider = MockProvider()
        self.provider.create_node({}, {TAG_RAY_NODE_KIND: "head"}, 1)
        head_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: "head"}, )[0]

        self.provider.create_node({}, {TAG_RAY_NODE_KIND: "unmanaged"}, 1)
        unmanaged_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: "unmanaged"}, )[0]
        unmanaged_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: "unmanaged"}, )[0]

        runner = MockProcessRunner()

        lm = LoadMetrics()
        lm.local_ip = head_ip

        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)

        lm.update(head_ip, {"CPU": 1}, True, {"CPU": 0}, True, {"CPU": 1})
        lm.update(unmanaged_ip, {"CPU": 0}, True, {"CPU": 0}, True, {})

        # Note that we shouldn't autoscale here because the resource demand
        # vector is not set and target utilization fraction = 1.
        autoscaler.update()
        # If the autoscaler was behaving incorrectly, it needs time to start
        # the new node, otherwise it could scale up after this check.
        time.sleep(0.2)
        self.waitForNodes(2)

    def testDelayedLaunch(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0

        # Update will try to create, but will block until we set the flag
        self.provider.ready_to_create.clear()
        autoscaler.update()
        assert autoscaler.pending_launches.value == 2
        assert len(self.provider.non_terminated_nodes({})) == 0

        # Set the flag, check it updates
        self.provider.ready_to_create.set()
        self.waitForNodes(2)
        assert autoscaler.pending_launches.value == 0

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
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_launch_batch=5,
            max_concurrent_launches=8,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0

        # update() should launch a wave of 5 nodes (max_launch_batch)
        # Force this first wave to block.
        rtc1 = self.provider.ready_to_create
        rtc1.clear()
        autoscaler.update()
        # Synchronization: wait for launchy thread to be blocked on rtc1
        waiters = rtc1._cond._waiters
        self.waitFor(lambda: len(waiters) == 1)
        assert autoscaler.pending_launches.value == 5
        assert len(self.provider.non_terminated_nodes({})) == 0

        # Call update() to launch a second wave of 3 nodes,
        # as 5 + 3 = 8 = max_concurrent_launches.
        # Make this wave complete immediately.
        rtc2 = threading.Event()
        self.provider.ready_to_create = rtc2
        rtc2.set()
        autoscaler.update()
        self.waitForNodes(3)
        assert autoscaler.pending_launches.value == 5

        # The first wave of 5 will now tragically fail
        self.provider.fail_creates = True
        rtc1.set()
        self.waitFor(lambda: autoscaler.pending_launches.value == 0)
        assert len(self.provider.non_terminated_nodes({})) == 3

        # Retry the first wave, allowing it to succeed this time
        self.provider.fail_creates = False
        autoscaler.update()
        self.waitForNodes(8)
        assert autoscaler.pending_launches.value == 0

        # Final wave of 2 nodes
        autoscaler.update()
        self.waitForNodes(10)
        assert autoscaler.pending_launches.value == 0

    def testUpdateThrottling(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            process_runner=runner,
            update_interval_s=10)
        autoscaler.update()
        self.waitForNodes(2)
        assert autoscaler.pending_launches.value == 0
        new_config = SMALL_CLUSTER.copy()
        new_config["max_workers"] = 1
        self.write_config(new_config)
        autoscaler.update()
        # not updated yet
        # note that node termination happens in the main thread, so
        # we do not need to add any delay here before checking
        assert len(self.provider.non_terminated_nodes({})) == 2
        assert autoscaler.pending_launches.value == 0

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
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_launch_batch=10,
            max_concurrent_launches=10,
            process_runner=runner,
            max_failures=0,
            update_interval_s=0)
        autoscaler.update()
        self.waitForNodes(2)

        # Write a corrupted config
        self.write_config("asdf")
        for _ in range(10):
            autoscaler.update()
        time.sleep(0.1)
        assert autoscaler.pending_launches.value == 0
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
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=2,
            process_runner=runner,
            update_interval_s=0)
        autoscaler.update()
        autoscaler.update()
        with pytest.raises(Exception):
            autoscaler.update()

    def testLaunchNewNodeOnOutOfBandTerminate(self):
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
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})

    def testReportsConfigFailures(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["provider"]["type"] = "external"
        config = prepare_config(config)
        config["provider"]["type"] = "mock"
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner(fail_cmds=["setup_cmd"])
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        autoscaler.update()
        autoscaler.update()
        self.waitForNodes(2)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UPDATE_FAILED})

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
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})
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
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(1)
        autoscaler.update()
        assert autoscaler.pending_launches.value == 0
        assert len(self.provider.non_terminated_nodes({})) == 1

        # Scales up as nodes are reported as used
        local_ip = services.get_node_ip_address()
        lm.update(local_ip, {"CPU": 2}, True, {"CPU": 0}, True, {})  # head
        lm.update("172.0.0.0", {"CPU": 2}, True, {"CPU": 0}, True,
                  {})  # worker 1
        autoscaler.update()
        self.waitForNodes(3)
        lm.update("172.0.0.1", {"CPU": 2}, True, {"CPU": 0}, True, {})
        autoscaler.update()
        self.waitForNodes(5)

        # Holds steady when load is removed
        lm.update("172.0.0.0", {"CPU": 2}, True, {"CPU": 2}, True, {})
        lm.update("172.0.0.1", {"CPU": 2}, True, {"CPU": 2}, True, {})
        autoscaler.update()
        assert autoscaler.pending_launches.value == 0
        assert len(self.provider.non_terminated_nodes({})) == 5

        # Scales down as nodes become unused
        lm.last_used_time_by_ip["172.0.0.0"] = 0
        lm.last_used_time_by_ip["172.0.0.1"] = 0
        autoscaler.update()
        assert autoscaler.pending_launches.value == 0
        assert len(self.provider.non_terminated_nodes({})) == 3
        lm.last_used_time_by_ip["172.0.0.2"] = 0
        lm.last_used_time_by_ip["172.0.0.3"] = 0
        autoscaler.update()
        assert autoscaler.pending_launches.value == 0
        assert len(self.provider.non_terminated_nodes({})) == 1

    def testDontScaleBelowTarget(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 0
        config["max_workers"] = 2
        config["target_utilization_fraction"] = 0.5
        config_path = self.write_config(config)
        self.provider = MockProvider()
        lm = LoadMetrics()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        assert autoscaler.pending_launches.value == 0
        assert len(self.provider.non_terminated_nodes({})) == 0

        # Scales up as nodes are reported as used
        local_ip = services.get_node_ip_address()
        lm.update(local_ip, {"CPU": 2}, True, {"CPU": 0}, True, {})  # head
        # 1.0 nodes used => target nodes = 2 => target workers = 1
        autoscaler.update()
        self.waitForNodes(1)

        # Make new node idle, and never used.
        # Should hold steady as target is still 2.
        lm.update("172.0.0.0", {"CPU": 0}, True, {"CPU": 0}, True, {})
        lm.last_used_time_by_ip["172.0.0.0"] = 0
        autoscaler.update()
        assert len(self.provider.non_terminated_nodes({})) == 1

        # Reduce load on head => target nodes = 1 => target workers = 0
        lm.update(local_ip, {"CPU": 2}, True, {"CPU": 1}, True, {})
        autoscaler.update()
        assert len(self.provider.non_terminated_nodes({})) == 0

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
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})

        # Mark a node as unhealthy
        for _ in range(5):
            if autoscaler.updaters:
                time.sleep(0.05)
                autoscaler.update()
        assert not autoscaler.updaters
        num_calls = len(runner.calls)
        lm.last_heartbeat_time_by_ip["172.0.0.0"] = 0
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

    def testSetupCommandsWithNoNodeCaching(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 1
        config["max_workers"] = 1
        config_path = self.write_config(config)
        self.provider = MockProvider(cache_stopped=False)
        runner = MockProcessRunner()
        lm = LoadMetrics()
        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        autoscaler.update()
        self.waitForNodes(1)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            1, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})
        runner.assert_has_call("172.0.0.0", "init_cmd")
        runner.assert_has_call("172.0.0.0", "setup_cmd")
        runner.assert_has_call("172.0.0.0", "worker_setup_cmd")
        runner.assert_has_call("172.0.0.0", "start_ray_worker")

        # Check the node was not reused
        self.provider.terminate_node(0)
        autoscaler.update()
        self.waitForNodes(1)
        runner.clear_history()
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            1, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})
        runner.assert_has_call("172.0.0.1", "init_cmd")
        runner.assert_has_call("172.0.0.1", "setup_cmd")
        runner.assert_has_call("172.0.0.1", "worker_setup_cmd")
        runner.assert_has_call("172.0.0.1", "start_ray_worker")

    def testSetupCommandsWithStoppedNodeCaching(self):
        file_mount_dir = tempfile.mkdtemp()
        config = SMALL_CLUSTER.copy()
        config["file_mounts"] = {"/root/test-folder": file_mount_dir}
        config["file_mounts_sync_continuously"] = True
        config["min_workers"] = 1
        config["max_workers"] = 1
        config_path = self.write_config(config)
        self.provider = MockProvider(cache_stopped=True)
        runner = MockProcessRunner()
        lm = LoadMetrics()
        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        autoscaler.update()
        self.waitForNodes(1)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            1, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})
        runner.assert_has_call("172.0.0.0", "init_cmd")
        runner.assert_has_call("172.0.0.0", "setup_cmd")
        runner.assert_has_call("172.0.0.0", "worker_setup_cmd")
        runner.assert_has_call("172.0.0.0", "start_ray_worker")
        runner.assert_has_call("172.0.0.0", "docker run")

        # Check the node was indeed reused
        self.provider.terminate_node(0)
        autoscaler.update()
        self.waitForNodes(1)
        runner.clear_history()
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            1, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})
        runner.assert_not_has_call("172.0.0.0", "init_cmd")
        runner.assert_not_has_call("172.0.0.0", "setup_cmd")
        runner.assert_not_has_call("172.0.0.0", "worker_setup_cmd")
        runner.assert_has_call("172.0.0.0", "start_ray_worker")
        runner.assert_has_call("172.0.0.0", "docker run")

        with open(f"{file_mount_dir}/new_file", "w") as f:
            f.write("abcdefgh")

        # Check that run_init happens when file_mounts have updated
        self.provider.terminate_node(0)
        autoscaler.update()
        self.waitForNodes(1)
        runner.clear_history()
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            1, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})
        runner.assert_not_has_call("172.0.0.0", "init_cmd")
        runner.assert_not_has_call("172.0.0.0", "setup_cmd")
        runner.assert_not_has_call("172.0.0.0", "worker_setup_cmd")
        runner.assert_has_call("172.0.0.0", "start_ray_worker")
        runner.assert_has_call("172.0.0.0", "docker run")

        runner.clear_history()
        autoscaler.update()
        runner.assert_not_has_call("172.0.0.0", "setup_cmd")

        # We did not start any other nodes
        runner.assert_not_has_call("172.0.0.1", " ")

    def testMultiNodeReuse(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 3
        config["max_workers"] = 3
        config_path = self.write_config(config)
        self.provider = MockProvider(cache_stopped=True)
        runner = MockProcessRunner()
        lm = LoadMetrics()
        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        autoscaler.update()
        self.waitForNodes(3)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            3, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})

        self.provider.terminate_node(0)
        self.provider.terminate_node(1)
        self.provider.terminate_node(2)
        runner.clear_history()

        # Scale up to 10 nodes, check we reuse the first 3 and add 7 more.
        config["min_workers"] = 10
        config["max_workers"] = 10
        self.write_config(config)
        autoscaler.update()
        autoscaler.update()
        self.waitForNodes(10)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            10, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})
        autoscaler.update()
        for i in [0, 1, 2]:
            runner.assert_not_has_call("172.0.0.{}".format(i), "setup_cmd")
            runner.assert_has_call("172.0.0.{}".format(i), "start_ray_worker")
        for i in [3, 4, 5, 6, 7, 8, 9]:
            runner.assert_has_call("172.0.0.{}".format(i), "setup_cmd")
            runner.assert_has_call("172.0.0.{}".format(i), "start_ray_worker")

    @unittest.skipIf(sys.platform == "win32", "Failing on Windows.")
    def testContinuousFileMounts(self):
        file_mount_dir = tempfile.mkdtemp()

        self.provider = MockProvider()
        config = SMALL_CLUSTER.copy()
        config["file_mounts"] = {"/home/test-folder": file_mount_dir}
        config["file_mounts_sync_continuously"] = True
        config["min_workers"] = 2
        config["max_workers"] = 2
        config_path = self.write_config(config)
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
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})
        autoscaler.update()
        docker_mount_prefix = get_docker_host_mount_location(
            config["cluster_name"])
        for i in [0, 1]:
            runner.assert_has_call(f"172.0.0.{i}", "setup_cmd")
            runner.assert_has_call(
                f"172.0.0.{i}", f"{file_mount_dir}/ ubuntu@172.0.0.{i}:"
                f"{docker_mount_prefix}/home/test-folder/")

        runner.clear_history()

        with open(os.path.join(file_mount_dir, "test.txt"), "wb") as temp_file:
            temp_file.write("hello".encode())

        autoscaler.update()
        self.waitForNodes(2)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})
        autoscaler.update()

        for i in [0, 1]:
            runner.assert_not_has_call(f"172.0.0.{i}", "setup_cmd")
            runner.assert_has_call(
                f"172.0.0.{i}", f"172.0.0.{i}",
                f"{file_mount_dir}/ ubuntu@172.0.0.{i}:"
                f"{docker_mount_prefix}/home/test-folder/")

    def testFileMountsNonContinuous(self):
        file_mount_dir = tempfile.mkdtemp()

        self.provider = MockProvider()
        config = SMALL_CLUSTER.copy()
        config["file_mounts"] = {"/home/test-folder": file_mount_dir}
        config["min_workers"] = 2
        config["max_workers"] = 2
        config_path = self.write_config(config)
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
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})
        autoscaler.update()
        docker_mount_prefix = get_docker_host_mount_location(
            config["cluster_name"])

        for i in [0, 1]:
            runner.assert_has_call(f"172.0.0.{i}", "setup_cmd")
            runner.assert_has_call(
                f"172.0.0.{i}", f"172.0.0.{i}",
                f"{file_mount_dir}/ ubuntu@172.0.0.{i}:"
                f"{docker_mount_prefix}/home/test-folder/")

        runner.clear_history()

        with open(os.path.join(file_mount_dir, "test.txt"), "wb") as temp_file:
            temp_file.write("hello".encode())

        autoscaler.update()
        self.waitForNodes(2)
        self.provider.finish_starting_nodes()
        self.waitForNodes(
            2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})

        for i in [0, 1]:
            runner.assert_not_has_call(f"172.0.0.{i}", "setup_cmd")
            runner.assert_not_has_call(
                f"172.0.0.{i}", f"{file_mount_dir}/ ubuntu@172.0.0.{i}:"
                f"{docker_mount_prefix}/home/test-folder/")

        # Simulate a second `ray up` call
        from ray.autoscaler._private import util
        util._hash_cache = {}
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
        self.provider.finish_starting_nodes()
        self.waitForNodes(
            2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})
        autoscaler.update()

        for i in [0, 1]:
            runner.assert_has_call(f"172.0.0.{i}", "setup_cmd")
            runner.assert_has_call(
                f"172.0.0.{i}", f"172.0.0.{i}",
                f"{file_mount_dir}/ ubuntu@172.0.0.{i}:"
                f"{docker_mount_prefix}/home/test-folder/")


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
