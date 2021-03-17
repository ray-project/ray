import json
import jsonschema
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
from ray.autoscaler._private.util import prepare_config, validate_config
from ray.autoscaler._private import commands
from ray.autoscaler.sdk import get_docker_host_mount_location
from ray.autoscaler._private.load_metrics import LoadMetrics
from ray.autoscaler._private.autoscaler import StandardAutoscaler
from ray.autoscaler._private.providers import (
    _NODE_PROVIDERS, _clear_provider_cache, _DEFAULT_CONFIGS)
from ray.autoscaler.tags import TAG_RAY_NODE_KIND, TAG_RAY_NODE_STATUS, \
    STATUS_UP_TO_DATE, STATUS_UPDATE_FAILED, TAG_RAY_USER_NODE_TYPE, \
    NODE_TYPE_LEGACY_HEAD, NODE_TYPE_LEGACY_WORKER, NODE_KIND_HEAD, \
    NODE_KIND_WORKER, STATUS_UNINITIALIZED, TAG_RAY_CLUSTER_NAME
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
        self.ready_to_run = threading.Event()
        self.ready_to_run.set()

    def check_call(self, cmd, *args, **kwargs):
        self.ready_to_run.wait()
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
        with self.lock:
            return self.mock_nodes[node_id].state == "running"

    def is_terminated(self, node_id):
        with self.lock:
            return self.mock_nodes[node_id].state in ["stopped", "terminated"]

    def node_tags(self, node_id):
        with self.lock:
            return self.mock_nodes[node_id].tags

    def internal_ip(self, node_id):
        with self.lock:
            return self.mock_nodes[node_id].internal_ip

    def external_ip(self, node_id):
        with self.lock:
            return self.mock_nodes[node_id].external_ip

    def create_node(self, node_config, tags, count, _skip_wait=False):
        if not _skip_wait:
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
        with self.lock:
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

MOCK_DEFAULT_CONFIG = {
    "cluster_name": "default",
    "max_workers": 2,
    "upscaling_speed": 1.0,
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
    "available_node_types": {
        "ray.head.default": {
            "min_workers": 0,
            "max_workers": 0,
            "resources": {},
            "node_config": {
                "head_default_prop": 4
            }
        },
        "ray.worker.default": {
            "min_workers": 0,
            "max_workers": 2,
            "resources": {},
            "node_config": {
                "worker_default_prop": 7
            }
        }
    },
    "head_node_type": "ray.head.default",
    "head_node": {},
    "worker_nodes": {},
    "file_mounts": {},
    "cluster_synced_files": [],
    "initialization_commands": [],
    "setup_commands": [],
    "head_setup_commands": [],
    "worker_setup_commands": [],
    "head_start_ray_commands": [],
    "worker_start_ray_commands": [],
}


class LoadMetricsTest(unittest.TestCase):
    def testHeartbeat(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 2}, {"CPU": 1}, {})
        lm.mark_active("2.2.2.2")
        assert "1.1.1.1" in lm.last_heartbeat_time_by_ip
        assert "2.2.2.2" in lm.last_heartbeat_time_by_ip
        assert "3.3.3.3" not in lm.last_heartbeat_time_by_ip

    def testDebugString(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", {"CPU": 2}, {"CPU": 0}, {})
        lm.update("2.2.2.2", {"CPU": 2, "GPU": 16}, {"CPU": 2, "GPU": 2}, {})
        lm.update(
            "3.3.3.3", {
                "memory": 1.05 * 1024 * 1024 * 1024,
                "object_store_memory": 2.1 * 1024 * 1024 * 1024,
            }, {
                "memory": 0,
                "object_store_memory": 1.05 * 1024 * 1024 * 1024,
            }, {})
        debug = lm.info_string()
        assert ("ResourceUsage: 2.0/4.0 CPU, 14.0/16.0 GPU, "
                "1.05 GiB/1.05 GiB memory, "
                "1.05 GiB/2.1 GiB object_store_memory") in debug


class AutoscalingTest(unittest.TestCase):
    def setUp(self):
        _NODE_PROVIDERS["mock"] = \
            lambda config: self.create_provider
        _DEFAULT_CONFIGS["mock"] = _DEFAULT_CONFIGS["local"]
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

    def write_config(self, config, call_prepare_config=True):
        new_config = copy.deepcopy(config)
        if call_prepare_config:
            new_config = prepare_config(new_config)
        path = os.path.join(self.tmpdir, "simple.yaml")
        with open(path, "w") as f:
            f.write(yaml.dump(new_config))
        return path

    def testAutoscalerConfigValidationFailNotFatal(self):
        invalid_config = {**SMALL_CLUSTER, "invalid_property_12345": "test"}
        # First check that this config is actually invalid
        with pytest.raises(ValidationError):
            validate_config(invalid_config)
        config_path = self.write_config(invalid_config)
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
        config = copy.deepcopy(SMALL_CLUSTER)
        head_run_option = "--kernel-memory=10g"
        standard_run_option = "--memory-swap=5g"
        config["docker"]["head_run_options"] = [head_run_option]
        config["docker"]["run_options"] = [standard_run_option]
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Mounts", ["[]"])
        # Two initial calls to docker cp, + 2 more calls during run_init
        runner.respond_to_call(".State.Running",
                               ["false", "false", "false", "false"])
        runner.respond_to_call("json .Config.Env", ["[]"])

        def _create_node(node_config, tags, count, _skip_wait=False):
            assert tags[TAG_RAY_NODE_STATUS] == STATUS_UNINITIALIZED
            if not _skip_wait:
                self.provider.ready_to_create.wait()
            if self.provider.fail_creates:
                return
            with self.provider.lock:
                if self.provider.cache_stopped:
                    for node in self.provider.mock_nodes.values():
                        if node.state == "stopped" and count > 0:
                            count -= 1
                            node.state = "pending"
                            node.tags.update(tags)
                for _ in range(count):
                    self.provider.mock_nodes[self.provider.next_id] = MockNode(
                        self.provider.next_id,
                        tags.copy(),
                        node_config,
                        tags.get(TAG_RAY_USER_NODE_TYPE),
                        unique_ips=self.provider.unique_ips)
                    self.provider.next_id += 1

        self.provider.create_node = _create_node
        commands.get_or_create_head_node(
            config,
            printable_config_file=config_path,
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
        runner.assert_has_call("1.2.3.4", pattern=head_run_option)
        runner.assert_has_call("1.2.3.4", pattern=standard_run_option)

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
        return config

    @unittest.skipIf(sys.platform == "win32", "Failing on Windows.")
    def testGetOrCreateHeadNodePodman(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["docker"]["use_podman"] = True
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Mounts", ["[]"])
        # Two initial calls to docker cp, + 2 more calls during run_init
        runner.respond_to_call(".State.Running",
                               ["false", "false", "false", "false"])
        runner.respond_to_call("json .Config.Env", ["[]"])
        commands.get_or_create_head_node(
            config,
            printable_config_file=config_path,
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
        runner.assert_has_call("1.2.3.4", pattern="podman run")

        docker_mount_prefix = get_docker_host_mount_location(
            SMALL_CLUSTER["cluster_name"])
        runner.assert_not_has_call(
            "1.2.3.4",
            pattern=f"-v {docker_mount_prefix}/~/ray_bootstrap_config")
        runner.assert_has_call(
            "1.2.3.4",
            pattern=f"podman cp {docker_mount_prefix}/~/ray_bootstrap_key.pem")
        pattern_to_assert = \
            f"podman cp {docker_mount_prefix}/~/ray_bootstrap_config.yaml"
        runner.assert_has_call("1.2.3.4", pattern=pattern_to_assert)

        for cmd in runner.command_history():
            assert "docker" not in cmd, ("Docker (not podman) found in call: "
                                         f"{cmd}")

        runner.assert_has_call("1.2.3.4", "podman inspect")
        runner.assert_has_call("1.2.3.4", "podman exec")

    @unittest.skipIf(sys.platform == "win32", "Failing on Windows.")
    def testGetOrCreateHeadNodeFromStopped(self):
        config = self.testGetOrCreateHeadNode()
        self.provider.cache_stopped = True
        existing_nodes = self.provider.non_terminated_nodes({})
        assert len(existing_nodes) == 1
        self.provider.terminate_node(existing_nodes[0])
        config_path = self.write_config(config)
        runner = MockProcessRunner()
        runner.respond_to_call("json .Mounts", ["[]"])
        # Two initial calls to docker cp, + 2 more calls during run_init
        runner.respond_to_call(".State.Running",
                               ["false", "false", "false", "false"])
        runner.respond_to_call("json .Config.Env", ["[]"])
        commands.get_or_create_head_node(
            config,
            printable_config_file=config_path,
            no_restart=False,
            restart_only=False,
            yes=True,
            override_cluster_name=None,
            _provider=self.provider,
            _runner=runner)
        self.waitForNodes(1)
        # Init & Setup commands must be run for Docker!
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

        # This next section of code ensures that the following order of
        # commands are executed:
        # 1. mkdir -p {docker_mount_prefix}
        # 2. rsync bootstrap files
        # 3. docker cp bootstrap files into container
        commands_with_mount = [
            (i, cmd) for i, cmd in enumerate(runner.command_history())
            if docker_mount_prefix in cmd
        ]
        rsync_commands = [x for x in commands_with_mount if "rsync" in x[1]]
        docker_cp_commands = [
            x for x in commands_with_mount if "docker cp" in x[1]
        ]
        first_mkdir = min(x[0] for x in commands_with_mount if "mkdir" in x[1])
        for file_to_check in [
                "ray_bootstrap_config.yaml", "ray_bootstrap_key.pem"
        ]:
            first_rsync = min(x[0] for x in rsync_commands
                              if "ray_bootstrap_config.yaml" in x[1])
            first_cp = min(
                x[0] for x in docker_cp_commands if file_to_check in x[1])
            assert first_mkdir < first_rsync
            assert first_rsync < first_cp

    def testGetOrCreateHeadNodeFromStoppedRestartOnly(self):
        config = self.testGetOrCreateHeadNode()
        self.provider.cache_stopped = True
        existing_nodes = self.provider.non_terminated_nodes({})
        assert len(existing_nodes) == 1
        self.provider.terminate_node(existing_nodes[0])
        config_path = self.write_config(config)
        runner = MockProcessRunner()
        runner.respond_to_call("json .Mounts", ["[]"])
        # Two initial calls to docker cp, + 2 more calls during run_init
        runner.respond_to_call(".State.Running",
                               ["false", "false", "false", "false"])
        runner.respond_to_call("json .Config.Env", ["[]"])
        commands.get_or_create_head_node(
            config,
            printable_config_file=config_path,
            no_restart=False,
            restart_only=True,
            yes=True,
            override_cluster_name=None,
            _provider=self.provider,
            _runner=runner)
        self.waitForNodes(1)
        # Init & Setup commands must be run for Docker!
        runner.assert_has_call("1.2.3.4", "init_cmd")
        runner.assert_has_call("1.2.3.4", "head_setup_cmd")
        runner.assert_has_call("1.2.3.4", "start_ray_head")

    @unittest.skipIf(sys.platform == "win32", "Failing on Windows.")
    def testDockerFileMountsAdded(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["file_mounts"] = {"source": "/dev/null"}
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        mounts = [{
            "Type": "bind",
            "Source": "/sys",
            "Destination": "/sys",
            "Mode": "ro",
            "RW": False,
            "Propagation": "rprivate"
        }]
        runner.respond_to_call("json .Mounts", [json.dumps(mounts)])
        # Two initial calls to docker cp, +1 more call during run_init
        runner.respond_to_call(".State.Running",
                               ["false", "false", "true", "true"])
        runner.respond_to_call("json .Config.Env", ["[]"])
        commands.get_or_create_head_node(
            config,
            printable_config_file=config_path,
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
        runner.assert_has_call("1.2.3.4", pattern="docker stop")
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
    def testDockerFileMountsRemoved(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["file_mounts"] = {}
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        mounts = [{
            "Type": "bind",
            "Source": "/sys",
            "Destination": "/sys",
            "Mode": "ro",
            "RW": False,
            "Propagation": "rprivate"
        }]
        runner.respond_to_call("json .Mounts", [json.dumps(mounts)])
        # Two initial calls to docker cp, +1 more call during run_init
        runner.respond_to_call(".State.Running",
                               ["false", "false", "true", "true"])
        runner.respond_to_call("json .Config.Env", ["[]"])
        commands.get_or_create_head_node(
            config,
            printable_config_file=config_path,
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
        # We only removed amount from the YAML, no changes should happen.
        runner.assert_not_has_call("1.2.3.4", pattern="docker stop")
        runner.assert_not_has_call("1.2.3.4", pattern="docker run")

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
        self.provider.create_node({}, {
            TAG_RAY_NODE_KIND: "head",
            TAG_RAY_NODE_STATUS: "up-to-date"
        }, 1)
        self.provider.create_node({}, {
            TAG_RAY_NODE_KIND: "worker",
            TAG_RAY_NODE_STATUS: "up-to-date"
        }, 10)
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
        self.provider.create_node({}, {
            TAG_RAY_NODE_KIND: "head",
            TAG_RAY_NODE_STATUS: "up-to-date"
        }, 1)
        self.provider.create_node({}, {
            TAG_RAY_NODE_KIND: "worker",
            TAG_RAY_NODE_STATUS: "up-to-date"
        }, 10)
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

    def testTerminateOutdatedNodesGracefully(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 5
        config["max_workers"] = 5
        config_path = self.write_config(config)
        self.provider = MockProvider()
        self.provider.create_node({}, {
            TAG_RAY_NODE_KIND: "worker",
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            TAG_RAY_USER_NODE_TYPE: NODE_TYPE_LEGACY_WORKER
        }, 10)
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(10)])
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

        # Check the outdated node removal event is generated.
        autoscaler.update()
        events = autoscaler.event_summarizer.summary()
        assert ("Removing 10 nodes of type "
                "ray-legacy-worker-node-type (outdated)." in events), events

    def testDynamicScaling(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(12)])
        lm = LoadMetrics()
        self.provider.create_node({}, {
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            TAG_RAY_USER_NODE_TYPE: NODE_TYPE_LEGACY_HEAD
        }, 1)
        lm.update("172.0.0.0", {"CPU": 1}, {"CPU": 0}, {})
        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        self.waitForNodes(0, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})
        autoscaler.update()
        self.waitForNodes(2, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})

        # Update the config to reduce the cluster size
        new_config = SMALL_CLUSTER.copy()
        new_config["max_workers"] = 1
        self.write_config(new_config)
        autoscaler.update()
        self.waitForNodes(1, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})

        # Update the config to reduce the cluster size
        new_config["min_workers"] = 10
        new_config["max_workers"] = 10
        self.write_config(new_config)
        autoscaler.update()
        # Because one worker already started, the scheduler waits for its
        # resources to be updated before it launches the remaining min_workers.
        self.waitForNodes(1, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})
        worker_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER}, )[0]
        lm.update(worker_ip, {"CPU": 1}, {"CPU": 1}, {})
        autoscaler.update()
        self.waitForNodes(
            10, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})

        # Check the launch failure event is generated.
        autoscaler.update()
        events = autoscaler.event_summarizer.summary()
        assert ("Removing 1 nodes of type "
                "ray-legacy-worker-node-type (max workers)." in events), events

    def testInitialWorkers(self):
        """initial_workers is deprecated, this tests that it is ignored."""
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
        self.waitForNodes(0)

    def testLegacyYamlWithRequestResources(self):
        """Test when using legacy yamls request_resources() adds workers.

        Makes sure that requested resources are added for legacy yamls when
        necessary. So if requested resources for instance fit on the headnode
        we don't add more nodes. But we add more nodes when they don't fit.
        """
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 0
        config["max_workers"] = 100
        config["idle_timeout_minutes"] = 0
        config["upscaling_speed"] = 1
        config_path = self.write_config(config)

        self.provider = MockProvider()
        self.provider.create_node({}, {
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
            TAG_RAY_USER_NODE_TYPE: NODE_TYPE_LEGACY_HEAD
        }, 1)
        head_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_HEAD}, )[0]
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(10)])

        lm = LoadMetrics()
        lm.local_ip = head_ip
        lm.update(head_ip, {"CPU": 1}, {"CPU": 1}, {})
        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        autoscaler.update()
        # 1 head node.
        self.waitForNodes(1)
        autoscaler.load_metrics.set_resource_requests([{"CPU": 1}])
        autoscaler.update()
        # still 1 head node because request_resources fits in the headnode.
        self.waitForNodes(1)
        autoscaler.load_metrics.set_resource_requests([{
            "CPU": 1
        }] + [{
            "CPU": 2
        }] * 9)
        autoscaler.update()
        self.waitForNodes(2)  # Adds a single worker to get its resources.
        autoscaler.update()
        self.waitForNodes(2)  # Still 1 worker because its resources
        # aren't known.
        lm.update("172.0.0.1", {"CPU": 2}, {"CPU": 2}, {})
        autoscaler.update()
        self.waitForNodes(10)  # 9 workers and 1 head node, scaled immediately.
        lm.update(
            "172.0.0.1", {"CPU": 2}, {"CPU": 2}, {},
            waiting_bundles=[{
                "CPU": 2
            }] * 9,
            infeasible_bundles=[{
                "CPU": 1
            }] * 1)
        autoscaler.update()
        # Make sure that if all the resources fit on the exising nodes not
        # to add any more.
        self.waitForNodes(10)

    def testAggressiveAutoscaling(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 0
        config["max_workers"] = 10
        config["idle_timeout_minutes"] = 0
        config["upscaling_speed"] = config["max_workers"]
        config_path = self.write_config(config)

        self.provider = MockProvider()
        self.provider.create_node({}, {
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
            TAG_RAY_USER_NODE_TYPE: NODE_TYPE_LEGACY_HEAD
        }, 1)
        head_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_HEAD}, )[0]
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(11)])
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
        lm.update(
            head_ip, {"CPU": 1}, {"CPU": 0}, {},
            waiting_bundles=[{
                "CPU": 1
            }] * 7,
            infeasible_bundles=[{
                "CPU": 1
            }] * 3)
        autoscaler.update()
        self.waitForNodes(2)  # launches a single node to get its resources
        worker_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER}, )[0]
        lm.update(
            worker_ip, {"CPU": 1}, {"CPU": 1}, {},
            waiting_bundles=[{
                "CPU": 1
            }] * 7,
            infeasible_bundles=[{
                "CPU": 1
            }] * 3)
        # Otherwise the worker is immediately terminated due to being idle.
        lm.last_used_time_by_ip[worker_ip] = time.time() + 5
        autoscaler.update()
        self.waitForNodes(11)
        worker_ips = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER}, )
        for ip in worker_ips:
            lm.last_used_time_by_ip[ip] = 0
        autoscaler.update()
        self.waitForNodes(1)  # only the head node
        # Make sure they don't get overwritten.
        assert autoscaler.resource_demand_scheduler.node_types[
            NODE_TYPE_LEGACY_HEAD]["resources"] == {
                "CPU": 1
            }
        assert autoscaler.resource_demand_scheduler.node_types[
            NODE_TYPE_LEGACY_WORKER]["resources"] == {
                "CPU": 1
            }

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
        self.provider.create_node({}, {
            TAG_RAY_NODE_KIND: "head",
            TAG_RAY_USER_NODE_TYPE: NODE_TYPE_LEGACY_HEAD,
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE
        }, 1)
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
        lm.update(head_ip, {"CPU": 1}, {"CPU": 0}, {})
        lm.update(unmanaged_ip, {"CPU": 0}, {"CPU": 0}, {})
        autoscaler.update()
        self.waitForNodes(2)
        # 1 CPU task cannot be scheduled.
        lm.update(
            unmanaged_ip, {"CPU": 0}, {"CPU": 0}, {},
            waiting_bundles=[{
                "CPU": 1
            }])
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
        self.provider.create_node({}, {
            TAG_RAY_NODE_KIND: "head",
            TAG_RAY_USER_NODE_TYPE: NODE_TYPE_LEGACY_HEAD,
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE
        }, 1)
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

        lm.update(head_ip, {"CPU": 1}, {"CPU": 0}, {"CPU": 1})
        lm.update(unmanaged_ip, {"CPU": 0}, {"CPU": 0}, {})

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
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(2)])
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

    def testDelayedLaunchWithMinWorkers(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 10
        config["max_workers"] = 10
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(10)])
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
        self.waitFor(lambda: len(waiters) == 2)
        assert autoscaler.pending_launches.value == 10
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(0)  # Nodes are not added on top of pending.
        rtc1.set()
        self.waitFor(lambda: autoscaler.pending_launches.value == 0)
        assert len(self.provider.non_terminated_nodes({})) == 10
        self.waitForNodes(10)
        assert autoscaler.pending_launches.value == 0
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
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(11)])
        self.provider.create_node({}, {
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            TAG_RAY_USER_NODE_TYPE: NODE_TYPE_LEGACY_HEAD
        }, 1)
        lm = LoadMetrics()
        lm.update("172.0.0.0", {"CPU": 1}, {"CPU": 0}, {})
        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_launch_batch=10,
            max_concurrent_launches=10,
            process_runner=runner,
            max_failures=0,
            update_interval_s=0)
        autoscaler.update()
        self.waitForNodes(2, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})

        # Write a corrupted config
        self.write_config("asdf", call_prepare_config=False)
        for _ in range(10):
            autoscaler.update()
        time.sleep(0.1)
        assert autoscaler.pending_launches.value == 0
        assert len(
            self.provider.non_terminated_nodes({
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER
            })) == 2

        # New a good config again
        new_config = SMALL_CLUSTER.copy()
        new_config["min_workers"] = 10
        new_config["max_workers"] = 10
        self.write_config(new_config)
        worker_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER}, )[0]
        # Because one worker already started, the scheduler waits for its
        # resources to be updated before it launches the remaining min_workers.
        lm.update(worker_ip, {"CPU": 1}, {"CPU": 1}, {})
        autoscaler.update()
        self.waitForNodes(
            10, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})

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
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(4)])
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
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(2)])
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
        config["provider"]["type"] = "mock"
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner(fail_cmds=["setup_cmd"])
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(2)])
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
        try:
            self.waitForNodes(
                2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UPDATE_FAILED})
        except AssertionError:
            # The failed nodes might have been already terminated by autoscaler
            assert len(self.provider.non_terminated_nodes({})) < 2

        # Check the launch failure event is generated.
        autoscaler.update()
        events = autoscaler.event_summarizer.summary()
        assert (
            "Removing 2 nodes of type "
            "ray-legacy-worker-node-type (launch failed)." in events), events

    def testConfiguresOutdatedNodes(self):
        from ray.autoscaler._private.cli_logger import cli_logger

        def do_nothing(*args, **kwargs):
            pass

        cli_logger._print = type(cli_logger._print)(do_nothing,
                                                    type(cli_logger))

        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(4)])
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
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(6)])
        self.provider.create_node({}, {
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            TAG_RAY_USER_NODE_TYPE: NODE_TYPE_LEGACY_HEAD
        }, 1)
        lm.update("172.0.0.0", {"CPU": 1}, {"CPU": 0}, {})
        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(
            self.provider.non_terminated_nodes({
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER
            })) == 0
        autoscaler.update()
        self.waitForNodes(1, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})
        autoscaler.update()
        assert autoscaler.pending_launches.value == 0
        assert len(
            self.provider.non_terminated_nodes({
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER
            })) == 1

        autoscaler.update()
        lm.update(
            "172.0.0.1", {"CPU": 2}, {"CPU": 0}, {},
            waiting_bundles=2 * [{
                "CPU": 2
            }])
        autoscaler.update()
        self.waitForNodes(3, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})
        lm.update(
            "172.0.0.2", {"CPU": 2}, {"CPU": 0}, {},
            waiting_bundles=3 * [{
                "CPU": 2
            }])
        autoscaler.update()
        self.waitForNodes(5, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})

        # Holds steady when load is removed
        lm.update("172.0.0.1", {"CPU": 2}, {"CPU": 2}, {})
        lm.update("172.0.0.2", {"CPU": 2}, {"CPU": 2}, {})
        autoscaler.update()
        assert autoscaler.pending_launches.value == 0
        assert len(
            self.provider.non_terminated_nodes({
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER
            })) == 5

        # Scales down as nodes become unused
        lm.last_used_time_by_ip["172.0.0.1"] = 0
        lm.last_used_time_by_ip["172.0.0.2"] = 0
        autoscaler.update()

        assert autoscaler.pending_launches.value == 0
        # This actually remained 4 instead of 3, because the other 2 nodes
        # are not connected and hence we rely more on connected nodes for
        # min_workers. When the "pending" nodes show up as connected,
        # then we can terminate the ones connected before.
        assert len(
            self.provider.non_terminated_nodes({
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER
            })) == 4
        lm.last_used_time_by_ip["172.0.0.3"] = 0
        lm.last_used_time_by_ip["172.0.0.4"] = 0
        autoscaler.update()
        assert autoscaler.pending_launches.value == 0
        # 2 nodes and not 1 because 1 is needed for min_worker and the other 1
        # is still not connected.
        self.waitForNodes(2, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})
        # when we connect it, we will see 1 node.
        lm.last_used_time_by_ip["172.0.0.5"] = 0
        autoscaler.update()
        self.waitForNodes(1, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})

        # Check add/remove events.
        events = autoscaler.event_summarizer.summary()
        assert ("Adding 5 nodes of type "
                "ray-legacy-worker-node-type." in events), events
        assert ("Removing 4 nodes of type "
                "ray-legacy-worker-node-type (idle)." in events), events

        summary = autoscaler.summary()
        assert len(summary.failed_nodes) == 0, \
            "Autoscaling policy decisions shouldn't result in failed nodes"

    def testTargetUtilizationFraction(self):
        config = SMALL_CLUSTER.copy()
        config["min_workers"] = 0
        config["max_workers"] = 20
        config["upscaling_speed"] = 10
        config_path = self.write_config(config)
        self.provider = MockProvider()
        lm = LoadMetrics()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(12)])
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
        self.provider.create_node({}, {
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
            TAG_RAY_USER_NODE_TYPE: NODE_TYPE_LEGACY_HEAD
        }, 1)
        head_ip = self.provider.non_terminated_node_ips({})[0]
        lm.local_ip = head_ip
        lm.update(
            head_ip, {"CPU": 2}, {"CPU": 1}, {}, waiting_bundles=[{
                "CPU": 1
            }])  # head
        # The headnode should be sufficient for now
        autoscaler.update()
        self.waitForNodes(1)

        # Requires 1 more worker as the head node is fully used.
        lm.update(
            head_ip, {"CPU": 2}, {"CPU": 0}, {}, waiting_bundles=[{
                "CPU": 1
            }])
        autoscaler.update()
        self.waitForNodes(2)  # 1 worker is added to get its resources.
        worker_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER}, )[0]
        lm.update(
            worker_ip, {"CPU": 1}, {"CPU": 1}, {},
            waiting_bundles=[{
                "CPU": 1
            }] * 7,
            infeasible_bundles=[{
                "CPU": 1
            }] * 4)
        # Add another 10 workers (frac=1/0.1=10, 1 worker running, 10*1=10)
        # and bypass constraint of 5 due to target utiization fraction.
        autoscaler.update()
        self.waitForNodes(12)

        worker_ips = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER}, )
        for ip in worker_ips:
            lm.last_used_time_by_ip[ip] = 0
        autoscaler.update()
        self.waitForNodes(1)  # only the head node
        assert len(self.provider.non_terminated_nodes({})) == 1

    def testRecoverUnhealthyWorkers(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(3)])
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

        # Check the node removal event is generated.
        autoscaler.update()
        events = autoscaler.event_summarizer.summary()
        assert ("Restarting 1 nodes of type "
                "ray-legacy-worker-node-type (lost contact with raylet)." in
                events), events

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

    def testLegacyExternalNodeScalerMissingFields(self):
        """Should fail to validate legacy external config with missing
        head_node, worker_nodes, or both."""
        external_config = copy.deepcopy(SMALL_CLUSTER)
        external_config["provider"] = {
            "type": "external",
            "module": "ray.autoscaler.node_provider.NodeProvider",
        }

        missing_workers, missing_head, missing_both = [
            copy.deepcopy(external_config) for _ in range(3)
        ]
        del missing_workers["worker_nodes"]
        del missing_head["head_node"]
        del missing_both["worker_nodes"]
        del missing_both["head_node"]

        for faulty_config in missing_workers, missing_head, missing_both:
            faulty_config = prepare_config(faulty_config)
            with pytest.raises(jsonschema.ValidationError):
                validate_config(faulty_config)

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
        invalid_provider = self.write_config(config, call_prepare_config=False)
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
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(2)])
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

    def testSetupCommandsWithStoppedNodeCachingNoDocker(self):
        file_mount_dir = tempfile.mkdtemp()
        config = SMALL_CLUSTER.copy()
        del config["docker"]
        config["file_mounts"] = {"/root/test-folder": file_mount_dir}
        config["file_mounts_sync_continuously"] = True
        config["min_workers"] = 1
        config["max_workers"] = 1
        config_path = self.write_config(config)
        self.provider = MockProvider(cache_stopped=True)
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(3)])
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

        runner.clear_history()
        autoscaler.update()
        runner.assert_not_has_call("172.0.0.0", "setup_cmd")

        # We did not start any other nodes
        runner.assert_not_has_call("172.0.0.1", " ")

    def testSetupCommandsWithStoppedNodeCachingDocker(self):
        # NOTE(ilr) Setup & Init commands **should** run with stopped nodes
        # when Docker is in use.
        file_mount_dir = tempfile.mkdtemp()
        config = SMALL_CLUSTER.copy()
        config["file_mounts"] = {"/root/test-folder": file_mount_dir}
        config["file_mounts_sync_continuously"] = True
        config["min_workers"] = 1
        config["max_workers"] = 1
        config_path = self.write_config(config)
        self.provider = MockProvider(cache_stopped=True)
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(3)])
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
        # These all must happen when the node is stopped and resued
        runner.assert_has_call("172.0.0.0", "init_cmd")
        runner.assert_has_call("172.0.0.0", "setup_cmd")
        runner.assert_has_call("172.0.0.0", "worker_setup_cmd")
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
        runner.assert_has_call("172.0.0.0", "init_cmd")
        runner.assert_has_call("172.0.0.0", "setup_cmd")
        runner.assert_has_call("172.0.0.0", "worker_setup_cmd")
        runner.assert_has_call("172.0.0.0", "start_ray_worker")
        runner.assert_has_call("172.0.0.0", "docker run")

        runner.clear_history()
        autoscaler.update()
        runner.assert_not_has_call("172.0.0.0", "setup_cmd")

        # We did not start any other nodes
        runner.assert_not_has_call("172.0.0.1", " ")

    def testMultiNodeReuse(self):
        config = SMALL_CLUSTER.copy()
        # Docker re-runs setup commands when nodes are reused.
        del config["docker"]
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
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(4)])
        runner.respond_to_call("command -v docker",
                               ["docker" for _ in range(4)])
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

        runner.respond_to_call(".Config.Image", ["example" for _ in range(4)])
        runner.respond_to_call(".State.Running", ["true" for _ in range(4)])
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
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(2)])
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
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(2)])
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

    def testAutodetectResources(self):
        self.provider = MockProvider()
        config = SMALL_CLUSTER.copy()
        config_path = self.write_config(config)
        runner = MockProcessRunner()
        proc_meminfo = """
MemTotal:       16396056 kB
MemFree:        12869528 kB
MemAvailable:   33000000 kB
        """
        runner.respond_to_call("cat /proc/meminfo", 2 * [proc_meminfo])
        runner.respond_to_call(".Runtimes", 2 * ["nvidia-container-runtime"])
        runner.respond_to_call("nvidia-smi", 2 * ["works"])
        runner.respond_to_call("json .Config.Env", 2 * ["[]"])
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
        runner.assert_has_call("172.0.0.0", pattern="--shm-size")
        runner.assert_has_call("172.0.0.0", pattern="--runtime=nvidia")

    def testDockerImageExistsBeforeInspect(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["min_workers"] = 1
        config["max_workers"] = 1
        config["docker"]["pull_before_run"] = False
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(1)])
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        autoscaler.update()
        autoscaler.update()
        self.waitForNodes(1)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            1, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})
        first_pull = [(i, cmd)
                      for i, cmd in enumerate(runner.command_history())
                      if "docker pull" in cmd]
        first_targeted_inspect = [
            (i, cmd) for i, cmd in enumerate(runner.command_history())
            if "docker inspect -f" in cmd
        ]

        # This checks for the bug mentioned #13128 where the image is inspected
        # before the image is present.
        assert min(x[0]
                   for x in first_pull) < min(x[0]
                                              for x in first_targeted_inspect)

    def testGetRunningHeadNode(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        self.provider = MockProvider()

        # Node 0 is failed.
        self.provider.create_node({}, {
            TAG_RAY_CLUSTER_NAME: "default",
            TAG_RAY_NODE_KIND: "head",
            TAG_RAY_NODE_STATUS: "update-failed"
        }, 1)

        # Node 1 is okay.
        self.provider.create_node({}, {
            TAG_RAY_CLUSTER_NAME: "default",
            TAG_RAY_NODE_KIND: "head",
            TAG_RAY_NODE_STATUS: "up-to-date"
        }, 1)

        node = commands._get_running_head_node(
            config,
            "/fake/path",
            override_cluster_name=None,
            create_if_needed=False,
            _provider=self.provider)

        assert node == 1


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
