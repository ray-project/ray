import copy
import logging
import sys
import json
import os
import re
import shutil
import tempfile
import threading
import time
import unittest
from collections import defaultdict
from enum import Enum
from subprocess import CalledProcessError
from typing import Callable, Dict, List, Optional
from unittest.mock import (
    Mock,
    patch,
)

import grpc
import pytest
import yaml
from jsonschema.exceptions import ValidationError

import ray
from ray._private.test_utils import RayTestTimeoutException
from ray.autoscaler._private import commands
from ray.autoscaler._private.autoscaler import NonTerminatedNodes, StandardAutoscaler
from ray.autoscaler._private.commands import get_or_create_head_node
from ray.autoscaler._private.constants import (
    DISABLE_LAUNCH_CONFIG_CHECK_KEY,
    DISABLE_NODE_UPDATERS_KEY,
    FOREGROUND_NODE_LAUNCH_KEY,
    WORKER_LIVENESS_CHECK_KEY,
    WORKER_RPC_DRAIN_KEY,
)
from ray.autoscaler._private.load_metrics import LoadMetrics
from ray.autoscaler._private.monitor import Monitor
from ray.autoscaler._private.prom_metrics import (
    AutoscalerPrometheusMetrics,
    NullMetric,
)
from ray.autoscaler._private.providers import (
    _DEFAULT_CONFIGS,
    _NODE_PROVIDERS,
    _clear_provider_cache,
)
from ray.autoscaler._private.readonly.node_provider import ReadOnlyNodeProvider
from ray.autoscaler._private.util import prepare_config, validate_config
from ray.autoscaler.node_launch_exception import NodeLaunchException
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.sdk import get_docker_host_mount_location
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
    STATUS_UNINITIALIZED,
    STATUS_UP_TO_DATE,
    STATUS_UPDATE_FAILED,
    TAG_RAY_CLUSTER_NAME,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_USER_NODE_TYPE,
)
from ray.core.generated import gcs_service_pb2
from ray.tests.test_batch_node_provider_unit import (
    MockBatchingNodeProvider,
)

WORKER_FILTER = {TAG_RAY_NODE_KIND: NODE_KIND_WORKER}


class DrainNodeOutcome(str, Enum):
    """Potential outcomes of DrainNode calls, each of which is handled
    differently by the autoscaler.
    """

    # Return a reponse indicating all nodes were succesfully drained.
    Succeeded = "Succeeded"
    # Return response indicating at least one node failed to be drained.
    NotAllDrained = "NotAllDrained"
    # Return an unimplemented gRPC error, indicating an old GCS.
    Unimplemented = "Unimplemented"
    # Raise a generic unexpected RPC error.
    GenericRpcError = "GenericRpcError"
    # Raise a generic unexpected exception.
    GenericException = "GenericException"
    # Tell the autoscaler to fail finding ips during drain
    FailedToFindIp = "FailedToFindIp"
    # Represents the situation in which draining nodes before termination is disabled.
    DrainDisabled = "DrainDisabled"


class MockRpcException(grpc.RpcError):
    """Mock RpcError with a specified status code.

    Note (Dmitri): It might be possible to do this already with standard tools
    in the `grpc` module, but how wasn't immediately obvious to me.
    """

    def __init__(self, status_code: grpc.StatusCode):
        self.status_code = status_code

    def code(self):
        return self.status_code


class MockNodeInfoStub:
    """Mock for GCS node info stub used by autoscaler to drain Ray nodes.

    Can simulate DrainNode failures via the `drain_node_outcome` parameter.
    Comments in DrainNodeOutcome enum class indicate the behavior for each
    outcome.
    """

    def __init__(self, drain_node_outcome=DrainNodeOutcome.Succeeded):
        self.drain_node_outcome = drain_node_outcome
        # Tracks how many times we've called DrainNode.
        self.drain_node_call_count = 0
        # Tracks how many times DrainNode returned a successful RPC response.
        self.drain_node_reply_success = 0

    def DrainNode(self, drain_node_request, timeout: int):
        """Simulate NodeInfo stub's DrainNode call.

        Outcome determined by self.drain_outcome.
        """
        self.drain_node_call_count += 1
        if self.drain_node_outcome == DrainNodeOutcome.Unimplemented:
            raise MockRpcException(status_code=grpc.StatusCode.UNIMPLEMENTED)
        elif self.drain_node_outcome == DrainNodeOutcome.GenericRpcError:
            # Any StatusCode besides UNIMPLEMENTED will do here.
            raise MockRpcException(status_code=grpc.StatusCode.UNAVAILABLE)
        elif self.drain_node_outcome == DrainNodeOutcome.GenericException:
            raise Exception("DrainNode failed in some unexpected way.")

        node_ids_to_drain = [
            data_item.node_id for data_item in drain_node_request.drain_node_data
        ]

        ok_gcs_status = gcs_service_pb2.GcsStatus(code=0, message="Yeah, it's fine.")

        all_nodes_drained_status = [
            gcs_service_pb2.DrainNodeStatus(node_id=node_id)
            for node_id in node_ids_to_drain
        ]

        # All but the last.
        not_all_drained_status = all_nodes_drained_status[:-1]

        if self.drain_node_outcome in [
            DrainNodeOutcome.Succeeded,
            DrainNodeOutcome.FailedToFindIp,
        ]:
            drain_node_status = all_nodes_drained_status
        elif self.drain_node_outcome == DrainNodeOutcome.NotAllDrained:
            drain_node_status = not_all_drained_status
        else:
            # Shouldn't land here.
            assert False, "Possible drain node outcomes exhausted."

        self.drain_node_reply_success += 1
        return gcs_service_pb2.DrainNodeReply(
            status=ok_gcs_status, drain_node_status=drain_node_status
        )


def mock_raylet_id() -> bytes:
    """Random raylet id to pass to load_metrics.update."""
    return os.urandom(10)


def fill_in_raylet_ids(provider, load_metrics) -> None:
    """Raylet ids for each ip are usually obtained by polling the GCS
    in monitor.py. For test purposes, we sometimes need to manually fill
    these fields with mocks.
    """
    for node in provider.non_terminated_nodes({}):
        ip = provider.internal_ip(node)
        load_metrics.raylet_id_by_ip[ip] = mock_raylet_id()


class MockNode:
    def __init__(self, node_id, tags, node_config, node_type, unique_ips=False):
        self.node_id = node_id
        self.state = "pending"
        self.tags = tags
        self.external_ip = "1.2.3.4"
        self.internal_ip = "172.0.0.{}".format(self.node_id)
        if unique_ips:
            self.external_ip = f"1.2.3.{self.node_id}"

        self.node_config = node_config
        self.node_type = node_type
        self.created_in_main_thread = (
            threading.current_thread() is threading.main_thread()
        )

    def matches(self, tags):
        for k, v in tags.items():
            if k not in self.tags or self.tags[k] != v:
                return False
        return True


class MockProcessRunner:
    def __init__(self, fail_cmds=None, cmd_to_callback=None, print_out=False):
        self.calls = []
        self.cmd_to_callback = cmd_to_callback or {}  # type: Dict[str, Callable]
        self.print_out = print_out
        self.fail_cmds = fail_cmds or []
        self.call_response = {}
        self.ready_to_run = threading.Event()
        self.ready_to_run.set()

        self.lock = threading.RLock()

    def check_call(self, cmd, *args, **kwargs):
        with self.lock:
            self.ready_to_run.wait()
            self.calls.append(cmd)
            if self.print_out:
                print(f">>>Process runner: Executing \n {str(cmd)}")
            for token in self.cmd_to_callback:
                if token in str(cmd):
                    # Trigger a callback if token is in cmd.
                    # Can be used to simulate background events during a node
                    # update (e.g. node disconnected).
                    callback = self.cmd_to_callback[token]
                    callback()

            for token in self.fail_cmds:
                if token in str(cmd):
                    raise CalledProcessError(1, token, "Failing command on purpose")

    def check_output(self, cmd):
        with self.lock:
            self.check_call(cmd)
            return_string = "command-output"
            key_to_shrink = None
            for pattern, response_list in self.call_response.items():
                if pattern in str(cmd):
                    return_string = response_list[0]
                    key_to_shrink = pattern
                    break
            if key_to_shrink:
                self.call_response[key_to_shrink] = self.call_response[key_to_shrink][
                    1:
                ]
                if len(self.call_response[key_to_shrink]) == 0:
                    del self.call_response[key_to_shrink]

            return return_string.encode()

    def assert_has_call(
        self, ip: str, pattern: Optional[str] = None, exact: Optional[List[str]] = None
    ):
        """Checks if the given value was called by this process runner.

        NOTE: Either pattern or exact must be specified, not both!

        Args:
            ip: IP address of the node that the given call was executed on.
            pattern: RegEx that matches one specific call.
            exact: List of strings that when joined exactly match one call.
        """
        with self.lock:
            assert bool(pattern) ^ bool(
                exact
            ), "Must specify either a pattern or exact match."
            debug_output = ""
            if pattern is not None:
                for cmd in self.command_history():
                    if ip in cmd:
                        debug_output += cmd
                        debug_output += "\n"
                        if re.search(pattern, cmd):
                            return True
                else:
                    raise Exception(
                        f"Did not find [{pattern}] in [{debug_output}] for "
                        f"ip={ip}.\n\nFull output: {self.command_history()}"
                    )
            elif exact is not None:
                exact_cmd = " ".join(exact)
                for cmd in self.command_history():
                    if ip in cmd:
                        debug_output += cmd
                        debug_output += "\n"
                    if cmd == exact_cmd:
                        return True
                raise Exception(
                    f"Did not find [{exact_cmd}] in [{debug_output}] for "
                    f"ip={ip}.\n\nFull output: {self.command_history()}"
                )

    def assert_not_has_call(self, ip: str, pattern: str):
        """Ensure that the given regex pattern was never called."""
        with self.lock:
            out = ""
            for cmd in self.command_history():
                if ip in cmd:
                    out += cmd
                    out += "\n"
            if re.search(pattern, out):
                raise Exception("Found [{}] in [{}] for {}".format(pattern, out, ip))
            else:
                return True

    def clear_history(self):
        with self.lock:
            self.calls = []

    def command_history(self):
        with self.lock:
            return [" ".join(cmd) for cmd in self.calls]

    def respond_to_call(self, pattern, response_list):
        with self.lock:
            self.call_response[pattern] = response_list


class MockProvider(NodeProvider):
    def __init__(self, cache_stopped=False, unique_ips=False):
        self.mock_nodes = {}
        self.next_id = 0
        self.throw = False
        self.error_creates = None
        self.fail_creates = False
        self.ready_to_create = threading.Event()
        self.ready_to_create.set()
        self.cache_stopped = cache_stopped
        self.unique_ips = unique_ips
        self.fail_to_fetch_ip = False
        self.safe_to_scale_flag = True
        # Many of these functions are called by node_launcher or updater in
        # different threads. This can be treated as a global lock for
        # everything.
        self.lock = threading.Lock()
        self.num_non_terminated_nodes_calls = 0
        super().__init__(None, None)

    def non_terminated_nodes(self, tag_filters):
        self.num_non_terminated_nodes_calls += 1
        with self.lock:
            if self.throw:
                raise Exception("oops")
            return [
                n.node_id
                for n in self.mock_nodes.values()
                if n.matches(tag_filters) and n.state not in ["stopped", "terminated"]
            ]

    def non_terminated_node_ips(self, tag_filters):
        with self.lock:
            if self.throw:
                raise Exception("oops")
            return [
                n.internal_ip
                for n in self.mock_nodes.values()
                if n.matches(tag_filters) and n.state not in ["stopped", "terminated"]
            ]

    def is_running(self, node_id):
        with self.lock:
            return self.mock_nodes[node_id].state == "running"

    def is_terminated(self, node_id):
        if node_id is None:
            # Circumvent test-cases where there's no head node.
            return True
        with self.lock:
            return self.mock_nodes[node_id].state in ["stopped", "terminated"]

    def node_tags(self, node_id):
        if node_id is None:
            # Circumvent test cases where there's no head node.
            return {}
        # Don't assume that node providers can retrieve tags from
        # terminated nodes.
        if self.is_terminated(node_id):
            raise Exception(f"The node with id {node_id} has been terminated!")
        with self.lock:
            return self.mock_nodes[node_id].tags

    def internal_ip(self, node_id):
        if self.fail_to_fetch_ip:
            raise Exception("Failed to fetch ip on purpose.")
        if node_id is None:
            # Circumvent test-cases where there's no head node.
            return "mock"
        with self.lock:
            return self.mock_nodes[node_id].internal_ip

    def external_ip(self, node_id):
        with self.lock:
            return self.mock_nodes[node_id].external_ip

    def create_node(self, node_config, tags, count, _skip_wait=False):
        if self.error_creates is not None:
            raise self.error_creates
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
                    unique_ips=self.unique_ips,
                )
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

    def safe_to_scale(self):
        return self.safe_to_scale_flag


class MockAutoscaler(StandardAutoscaler):
    """Test autoscaler constructed to verify the property that each
    autoscaler update issues at most one provider.non_terminated_nodes call.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fail_to_find_ip_during_drain = False

    def _update(self):
        # Only works with MockProvider or MockBatchingNodeProvider.
        assert isinstance(self.provider, MockProvider) or isinstance(
            self.provider, MockBatchingNodeProvider
        )
        start_calls = self.provider.num_non_terminated_nodes_calls
        super()._update()
        end_calls = self.provider.num_non_terminated_nodes_calls

        # Strict inequality if update is called twice within the throttling
        # interval `self.update_interval_s`
        assert end_calls <= start_calls + 1

    def drain_nodes_via_gcs(self, provider_node_ids_to_drain):
        if self.fail_to_find_ip_during_drain:
            self.provider.fail_to_fetch_ip = True
        super().drain_nodes_via_gcs(provider_node_ids_to_drain)
        self.provider.fail_to_fetch_ip = False


class NoUpdaterMockAutoscaler(MockAutoscaler):
    def update_nodes(self):
        raise AssertionError(
            "Node updaters are disabled. This method should not be accessed!"
        )


SMALL_CLUSTER = {
    "cluster_name": "default",
    "idle_timeout_minutes": 5,
    "max_workers": 2,
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
        "head": {
            "node_config": {
                "TestProp": 1,
            },
            "resources": {"CPU": 1},
            "max_workers": 0,
        },
        "worker": {
            "node_config": {
                "TestProp": 2,
            },
            "resources": {"CPU": 1},
            "min_workers": 0,
            "max_workers": 2,
        },
    },
    "head_node_type": "head",
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
        "ray.head.default": {"resources": {}, "node_config": {"head_default_prop": 4}},
        "ray.worker.default": {
            "min_workers": 0,
            "max_workers": 2,
            "resources": {},
            "node_config": {"worker_default_prop": 7},
        },
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

TYPES_A = {
    "empty_node": {
        "node_config": {
            "FooProperty": 42,
            "TestProp": 1,
        },
        "resources": {},
        "max_workers": 0,
    },
    "m4.large": {
        "node_config": {},
        "resources": {"CPU": 2},
        "max_workers": 10,
    },
    "m4.4xlarge": {
        "node_config": {},
        "resources": {"CPU": 16},
        "max_workers": 8,
    },
    "m4.16xlarge": {
        "node_config": {},
        "resources": {"CPU": 64},
        "max_workers": 4,
    },
    "p2.xlarge": {
        "node_config": {},
        "resources": {"CPU": 16, "GPU": 1},
        "max_workers": 10,
    },
    "p2.8xlarge": {
        "node_config": {},
        "resources": {"CPU": 32, "GPU": 8},
        "max_workers": 4,
    },
}

MULTI_WORKER_CLUSTER = dict(
    SMALL_CLUSTER, **{"available_node_types": TYPES_A, "head_node_type": "empty_node"}
)

exc_info = None
try:
    raise Exception("Test exception.")
except Exception:
    exc_info = sys.exc_info()
assert exc_info is not None


class LoadMetricsTest(unittest.TestCase):
    def testHeartbeat(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", mock_raylet_id(), {"CPU": 2}, {"CPU": 1}, {})
        lm.mark_active("2.2.2.2")
        assert "1.1.1.1" in lm.last_heartbeat_time_by_ip
        assert "2.2.2.2" in lm.last_heartbeat_time_by_ip
        assert "3.3.3.3" not in lm.last_heartbeat_time_by_ip

    def testDebugString(self):
        lm = LoadMetrics()
        lm.update("1.1.1.1", mock_raylet_id(), {"CPU": 2}, {"CPU": 0}, {})
        lm.update(
            "2.2.2.2", mock_raylet_id(), {"CPU": 2, "GPU": 16}, {"CPU": 2, "GPU": 2}, {}
        )
        lm.update(
            "3.3.3.3",
            mock_raylet_id(),
            {
                "memory": 1.05 * 1024 * 1024 * 1024,
                "object_store_memory": 2.1 * 1024 * 1024 * 1024,
            },
            {
                "memory": 0,
                "object_store_memory": 1.05 * 1024 * 1024 * 1024,
            },
            {},
        )
        debug = lm.info_string()
        assert (
            "ResourceUsage: 2.0/4.0 CPU, 14.0/16.0 GPU, "
            "1.05 GiB/1.05 GiB memory, "
            "1.05 GiB/2.1 GiB object_store_memory"
        ) in debug


class AutoscalingTest(unittest.TestCase):
    def setUp(self):
        _NODE_PROVIDERS["mock"] = lambda config: self.create_provider
        _DEFAULT_CONFIGS["mock"] = _DEFAULT_CONFIGS["aws"]
        self.provider = None
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        self.provider = None
        del _NODE_PROVIDERS["mock"]
        _clear_provider_cache()
        shutil.rmtree(self.tmpdir)
        ray.shutdown()

    def waitFor(self, condition, num_retries=50, fail_msg=None):
        for _ in range(num_retries):
            if condition():
                return
            time.sleep(0.1)
        fail_msg = fail_msg or "Timed out waiting for {}".format(condition)
        raise RayTestTimeoutException(fail_msg)

    def waitForUpdatersToFinish(self, autoscaler):
        self.waitFor(
            lambda: all(
                not updater.is_alive() for updater in autoscaler.updaters.values()
            ),
            num_retries=500,
            fail_msg="Last round of updaters didn't complete on time.",
        )

    def num_nodes(self, tag_filters=None):
        if tag_filters is None:
            tag_filters = {}
        return len(self.provider.non_terminated_nodes(tag_filters))

    def waitForNodes(self, expected, comparison=None, tag_filters=None):
        if comparison is None:
            comparison = self.assertEqual
        MAX_ITER = 50
        for i in range(MAX_ITER):
            n = self.num_nodes(tag_filters)
            try:
                comparison(n, expected, msg="Unexpected node quantity.")
                return
            except Exception:
                if i == MAX_ITER - 1:
                    raise
            time.sleep(0.1)

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

    def worker_node_thread_check(self, foreground_node_launcher: bool):
        """Confirms that worker nodes were launched in the main thread if foreground
        node launch is enabled, in a subthread otherwise.

        Args:
            foreground_node_launcher: Whether workers nodes are expected to be
            launched in the foreground.

        """
        worker_ids = self.provider.non_terminated_nodes(tag_filters=WORKER_FILTER)
        worker_nodes = [self.provider.mock_nodes[worker_id] for worker_id in worker_ids]
        if foreground_node_launcher:
            # All workers were created in the main thread.
            assert all(
                worker_node.created_in_main_thread for worker_node in worker_nodes
            )
        else:
            # All workers were created in a background thread.
            assert not any(
                worker_node.created_in_main_thread for worker_node in worker_nodes
            )

    def testAutoscalerConfigValidationFailNotFatal(self):
        invalid_config = {**SMALL_CLUSTER, "invalid_property_12345": "test"}
        # First check that this config is actually invalid
        with pytest.raises(ValidationError):
            validate_config(invalid_config)
        config_path = self.write_config(invalid_config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = MockAutoscaler(
            config_path,
            LoadMetrics(),
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(1)
        autoscaler.update()
        self.waitForNodes(1)

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
        # Two initial calls to rsync, + 2 more calls during run_init
        runner.respond_to_call(".State.Running", ["false", "false", "false", "false"])
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
                        unique_ips=self.provider.unique_ips,
                    )
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
            _runner=runner,
        )
        self.waitForNodes(1)
        runner.assert_has_call("1.2.3.4", "init_cmd")
        runner.assert_has_call("1.2.3.4", "head_setup_cmd")
        runner.assert_has_call("1.2.3.4", "start_ray_head")
        self.assertEqual(self.provider.mock_nodes[0].node_type, "head")
        runner.assert_has_call("1.2.3.4", pattern="docker run")
        runner.assert_has_call("1.2.3.4", pattern=head_run_option)
        runner.assert_has_call("1.2.3.4", pattern=standard_run_option)

        docker_mount_prefix = get_docker_host_mount_location(
            SMALL_CLUSTER["cluster_name"]
        )
        runner.assert_not_has_call(
            "1.2.3.4", pattern=f"-v {docker_mount_prefix}/~/ray_bootstrap_config"
        )
        common_container_copy = f"rsync -e.*docker exec -i.*{docker_mount_prefix}/~/"
        runner.assert_has_call(
            "1.2.3.4", pattern=common_container_copy + "ray_bootstrap_key.pem"
        )
        runner.assert_has_call(
            "1.2.3.4", pattern=common_container_copy + "ray_bootstrap_config.yaml"
        )
        return config

    def testNodeTypeNameChange(self):
        """
        Tests that cluster launcher and autoscaler have correct behavior under
        changes and deletions of node type keys.

        Specifically if we change the key from "old-type" to "new-type", nodes
        of type "old-type" are deleted and (if required by the config) replaced
        by nodes of type "new-type".

        Strategy:
            1. launch a test cluster with a head and one `min_worker`
            2. change node type keys for both head and worker in cluster yaml
            3. update cluster with new yaml
            4. verify graceful replacement of the two nodes with old node types
                with two nodes with new node types.
        """

        # Default config with renamed node types, min_worker 1, docker off.
        config = copy.deepcopy(MOCK_DEFAULT_CONFIG)
        config["docker"] = {}
        node_types = config["available_node_types"]
        node_types["ray.head.old"] = node_types.pop("ray.head.default")
        node_types["ray.worker.old"] = node_types.pop("ray.worker.default")
        config["head_node_type"] = "ray.head.old"
        node_types["ray.worker.old"]["min_workers"] = 1

        # Create head and launch autoscaler
        runner = MockProcessRunner()
        self.provider = MockProvider()

        config_path = self.write_config(config)
        commands.get_or_create_head_node(
            config,
            printable_config_file=config_path,
            no_restart=False,
            restart_only=False,
            yes=True,
            override_cluster_name=None,
            _provider=self.provider,
            _runner=runner,
        )
        self.waitForNodes(1)
        lm = LoadMetrics()
        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )
        autoscaler.update()

        self.waitForNodes(2)
        head_list = self.provider.non_terminated_nodes(
            {TAG_RAY_NODE_KIND: NODE_KIND_HEAD}
        )
        worker_list = self.provider.non_terminated_nodes(
            {TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
        )
        # One head (as always)
        # One worker (min_workers 1 with no resource demands)
        assert len(head_list) == 1 and len(worker_list) == 1
        worker, head = worker_list.pop(), head_list.pop()

        # Confirm node type tags
        assert (
            self.provider.node_tags(head).get(TAG_RAY_USER_NODE_TYPE) == "ray.head.old"
        )
        assert (
            self.provider.node_tags(worker).get(TAG_RAY_USER_NODE_TYPE)
            == "ray.worker.old"
        )

        # Rename head and worker types
        new_config = copy.deepcopy(config)
        node_types = new_config["available_node_types"]
        node_types["ray.head.new"] = node_types.pop("ray.head.old")
        node_types["ray.worker.new"] = node_types.pop("ray.worker.old")
        new_config["head_node_type"] = "ray.head.new"
        config_path = self.write_config(new_config)

        # Expect this to delete "ray.head.old" head and create "ray.head.new"
        # head.
        commands.get_or_create_head_node(
            new_config,
            printable_config_file=config_path,
            no_restart=False,
            restart_only=False,
            yes=True,
            override_cluster_name=None,
            _provider=self.provider,
            _runner=runner,
        )

        self.waitForNodes(2)
        head_list = self.provider.non_terminated_nodes(
            {TAG_RAY_NODE_KIND: NODE_KIND_HEAD}
        )
        worker_list = self.provider.non_terminated_nodes(
            {TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
        )
        # One head (as always)
        # One worker (maintained from previous autoscaler update)
        assert len(head_list) == 1 and len(worker_list) == 1
        worker, head = worker_list.pop(), head_list.pop()
        # Confirm new head
        assert (
            self.provider.node_tags(head).get(TAG_RAY_USER_NODE_TYPE) == "ray.head.new"
        )
        # Still old worker, as we haven't made an autoscaler update yet.
        assert (
            self.provider.node_tags(worker).get(TAG_RAY_USER_NODE_TYPE)
            == "ray.worker.old"
        )

        fill_in_raylet_ids(self.provider, lm)
        autoscaler.update()
        self.waitForNodes(2)
        events = autoscaler.event_summarizer.summary()
        # Just one node (node_id 1) terminated in the last update.
        # Validates that we didn't try to double-terminate node 0.
        assert sorted(events) == [
            "Adding 1 node(s) of type ray.worker.new.",
            "Adding 1 node(s) of type ray.worker.old.",
            "Removing 1 nodes of type ray.worker.old (not "
            "in available_node_types: ['ray.head.new', 'ray.worker.new']).",
        ]

        head_list = self.provider.non_terminated_nodes(
            {TAG_RAY_NODE_KIND: NODE_KIND_HEAD}
        )
        worker_list = self.provider.non_terminated_nodes(
            {TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
        )
        # One head (as always)
        # One worker (min_workers 1 with no resource demands)
        assert len(head_list) == 1 and len(worker_list) == 1
        worker, head = worker_list.pop(), head_list.pop()

        # After the autoscaler update, new head and new worker.
        assert (
            self.provider.node_tags(head).get(TAG_RAY_USER_NODE_TYPE) == "ray.head.new"
        )
        assert (
            self.provider.node_tags(worker).get(TAG_RAY_USER_NODE_TYPE)
            == "ray.worker.new"
        )

    def testGetOrCreateHeadNodePodman(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["docker"]["use_podman"] = True
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Mounts", ["[]"])
        # Two initial calls to rsync, + 2 more calls during run_init
        runner.respond_to_call(".State.Running", ["false", "false", "false", "false"])
        runner.respond_to_call("json .Config.Env", ["[]"])
        commands.get_or_create_head_node(
            config,
            printable_config_file=config_path,
            no_restart=False,
            restart_only=False,
            yes=True,
            override_cluster_name=None,
            _provider=self.provider,
            _runner=runner,
        )
        self.waitForNodes(1)
        runner.assert_has_call("1.2.3.4", "init_cmd")
        runner.assert_has_call("1.2.3.4", "head_setup_cmd")
        runner.assert_has_call("1.2.3.4", "start_ray_head")
        self.assertEqual(self.provider.mock_nodes[0].node_type, "head")
        runner.assert_has_call("1.2.3.4", pattern="podman run")

        docker_mount_prefix = get_docker_host_mount_location(
            SMALL_CLUSTER["cluster_name"]
        )
        runner.assert_not_has_call(
            "1.2.3.4", pattern=f"-v {docker_mount_prefix}/~/ray_bootstrap_config"
        )
        common_container_copy = f"rsync -e.*podman exec -i.*{docker_mount_prefix}/~/"
        runner.assert_has_call(
            "1.2.3.4", pattern=common_container_copy + "ray_bootstrap_key.pem"
        )
        runner.assert_has_call(
            "1.2.3.4", pattern=common_container_copy + "ray_bootstrap_config.yaml"
        )

        for cmd in runner.command_history():
            assert "docker" not in cmd, "Docker (not podman) found in call: " f"{cmd}"

        runner.assert_has_call("1.2.3.4", "podman inspect")
        runner.assert_has_call("1.2.3.4", "podman exec")

    def testGetOrCreateHeadNodeFromStopped(self):
        config = self.testGetOrCreateHeadNode()
        self.provider.cache_stopped = True
        existing_nodes = self.provider.non_terminated_nodes({})
        assert len(existing_nodes) == 1
        self.provider.terminate_node(existing_nodes[0])
        config_path = self.write_config(config)
        runner = MockProcessRunner()
        runner.respond_to_call("json .Mounts", ["[]"])
        # Two initial calls to rsync, + 2 more calls during run_init
        runner.respond_to_call(".State.Running", ["false", "false", "false", "false"])
        runner.respond_to_call("json .Config.Env", ["[]"])
        commands.get_or_create_head_node(
            config,
            printable_config_file=config_path,
            no_restart=False,
            restart_only=False,
            yes=True,
            override_cluster_name=None,
            _provider=self.provider,
            _runner=runner,
        )
        self.waitForNodes(1)
        # Init & Setup commands must be run for Docker!
        runner.assert_has_call("1.2.3.4", "init_cmd")
        runner.assert_has_call("1.2.3.4", "head_setup_cmd")
        runner.assert_has_call("1.2.3.4", "start_ray_head")
        self.assertEqual(self.provider.mock_nodes[0].node_type, "head")
        runner.assert_has_call("1.2.3.4", pattern="docker run")

        docker_mount_prefix = get_docker_host_mount_location(
            SMALL_CLUSTER["cluster_name"]
        )
        runner.assert_not_has_call(
            "1.2.3.4", pattern=f"-v {docker_mount_prefix}/~/ray_bootstrap_config"
        )
        common_container_copy = f"rsync -e.*docker exec -i.*{docker_mount_prefix}/~/"
        runner.assert_has_call(
            "1.2.3.4", pattern=common_container_copy + "ray_bootstrap_key.pem"
        )
        runner.assert_has_call(
            "1.2.3.4", pattern=common_container_copy + "ray_bootstrap_config.yaml"
        )

        # This next section of code ensures that the following order of
        # commands are executed:
        # 1. mkdir -p {docker_mount_prefix}
        # 2. rsync bootstrap files (over ssh)
        # 3. rsync bootstrap files into container
        commands_with_mount = [
            (i, cmd)
            for i, cmd in enumerate(runner.command_history())
            if docker_mount_prefix in cmd
        ]
        rsync_commands = [x for x in commands_with_mount if "rsync --rsh" in x[1]]
        copy_into_container = [
            x
            for x in commands_with_mount
            if re.search("rsync -e.*docker exec -i", x[1])
        ]
        first_mkdir = min(x[0] for x in commands_with_mount if "mkdir" in x[1])
        docker_run_cmd_indx = [
            i for i, cmd in enumerate(runner.command_history()) if "docker run" in cmd
        ][0]
        for file_to_check in ["ray_bootstrap_config.yaml", "ray_bootstrap_key.pem"]:
            first_rsync = min(
                x[0] for x in rsync_commands if "ray_bootstrap_config.yaml" in x[1]
            )
            first_cp = min(x[0] for x in copy_into_container if file_to_check in x[1])
            # Ensures that `mkdir -p` precedes `docker run` because Docker
            # will auto-create the folder with wrong permissions.
            assert first_mkdir < docker_run_cmd_indx
            # Ensures that the folder is created before running rsync.
            assert first_mkdir < first_rsync
            # Checks that the file is present before copying into the container
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
        # Two initial calls to rsync, + 2 more calls during run_init
        runner.respond_to_call(".State.Running", ["false", "false", "false", "false"])
        runner.respond_to_call("json .Config.Env", ["[]"])
        commands.get_or_create_head_node(
            config,
            printable_config_file=config_path,
            no_restart=False,
            restart_only=True,
            yes=True,
            override_cluster_name=None,
            _provider=self.provider,
            _runner=runner,
        )
        self.waitForNodes(1)
        # Init & Setup commands must be run for Docker!
        runner.assert_has_call("1.2.3.4", "init_cmd")
        runner.assert_has_call("1.2.3.4", "head_setup_cmd")
        runner.assert_has_call("1.2.3.4", "start_ray_head")

    def testDockerFileMountsAdded(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["file_mounts"] = {"source": "/dev/null"}
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        mounts = [
            {
                "Type": "bind",
                "Source": "/sys",
                "Destination": "/sys",
                "Mode": "ro",
                "RW": False,
                "Propagation": "rprivate",
            }
        ]
        runner.respond_to_call("json .Mounts", [json.dumps(mounts)])
        # Two initial calls to rsync, +1 more call during run_init
        runner.respond_to_call(".State.Running", ["false", "false", "true", "true"])
        runner.respond_to_call("json .Config.Env", ["[]"])
        commands.get_or_create_head_node(
            config,
            printable_config_file=config_path,
            no_restart=False,
            restart_only=False,
            yes=True,
            override_cluster_name=None,
            _provider=self.provider,
            _runner=runner,
        )
        self.waitForNodes(1)
        runner.assert_has_call("1.2.3.4", "init_cmd")
        runner.assert_has_call("1.2.3.4", "head_setup_cmd")
        runner.assert_has_call("1.2.3.4", "start_ray_head")
        self.assertEqual(self.provider.mock_nodes[0].node_type, "head")
        runner.assert_has_call("1.2.3.4", pattern="docker stop")
        runner.assert_has_call("1.2.3.4", pattern="docker run")

        docker_mount_prefix = get_docker_host_mount_location(
            SMALL_CLUSTER["cluster_name"]
        )
        runner.assert_not_has_call(
            "1.2.3.4", pattern=f"-v {docker_mount_prefix}/~/ray_bootstrap_config"
        )
        common_container_copy = f"rsync -e.*docker exec -i.*{docker_mount_prefix}/~/"
        runner.assert_has_call(
            "1.2.3.4", pattern=common_container_copy + "ray_bootstrap_key.pem"
        )
        runner.assert_has_call(
            "1.2.3.4", pattern=common_container_copy + "ray_bootstrap_config.yaml"
        )

    def testDockerFileMountsRemoved(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["file_mounts"] = {}
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        mounts = [
            {
                "Type": "bind",
                "Source": "/sys",
                "Destination": "/sys",
                "Mode": "ro",
                "RW": False,
                "Propagation": "rprivate",
            }
        ]
        runner.respond_to_call("json .Mounts", [json.dumps(mounts)])
        # Two initial calls to rsync, +1 more call during run_init
        runner.respond_to_call(".State.Running", ["false", "false", "true", "true"])
        runner.respond_to_call("json .Config.Env", ["[]"])
        commands.get_or_create_head_node(
            config,
            printable_config_file=config_path,
            no_restart=False,
            restart_only=False,
            yes=True,
            override_cluster_name=None,
            _provider=self.provider,
            _runner=runner,
        )
        self.waitForNodes(1)
        runner.assert_has_call("1.2.3.4", "init_cmd")
        runner.assert_has_call("1.2.3.4", "head_setup_cmd")
        runner.assert_has_call("1.2.3.4", "start_ray_head")
        self.assertEqual(self.provider.mock_nodes[0].node_type, "head")
        # We only removed amount from the YAML, no changes should happen.
        runner.assert_not_has_call("1.2.3.4", pattern="docker stop")
        runner.assert_not_has_call("1.2.3.4", pattern="docker run")

        docker_mount_prefix = get_docker_host_mount_location(
            SMALL_CLUSTER["cluster_name"]
        )
        runner.assert_not_has_call(
            "1.2.3.4", pattern=f"-v {docker_mount_prefix}/~/ray_bootstrap_config"
        )
        common_container_copy = f"rsync -e.*docker exec -i.*{docker_mount_prefix}/~/"
        runner.assert_has_call(
            "1.2.3.4", pattern=common_container_copy + "ray_bootstrap_key.pem"
        )
        runner.assert_has_call(
            "1.2.3.4", pattern=common_container_copy + "ray_bootstrap_config.yaml"
        )

    def testRsyncCommandWithDocker(self):
        assert SMALL_CLUSTER["docker"]["container_name"]
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider(unique_ips=True)
        self.provider.create_node(
            {}, {TAG_RAY_NODE_KIND: "head", TAG_RAY_NODE_STATUS: "up-to-date"}, 1
        )
        self.provider.create_node(
            {}, {TAG_RAY_NODE_KIND: "worker", TAG_RAY_NODE_STATUS: "up-to-date"}, 10
        )
        self.provider.finish_starting_nodes()
        ray.autoscaler.node_provider._get_node_provider = Mock(
            return_value=self.provider
        )
        ray.autoscaler._private.commands._bootstrap_config = Mock(
            return_value=SMALL_CLUSTER
        )
        runner = MockProcessRunner()
        commands.rsync(
            config_path,
            source=config_path,
            target="/tmp/test_path",
            override_cluster_name=None,
            down=True,
            _runner=runner,
        )
        runner.assert_has_call("1.2.3.0", pattern="rsync -e.*docker exec -i")
        runner.assert_has_call("1.2.3.0", pattern="rsync --rsh")
        runner.clear_history()

        commands.rsync(
            config_path,
            source=config_path,
            target="/tmp/test_path",
            override_cluster_name=None,
            down=True,
            ip_address="1.2.3.5",
            _runner=runner,
        )
        runner.assert_has_call("1.2.3.5", pattern="rsync -e.*docker exec -i")
        runner.assert_has_call("1.2.3.5", pattern="rsync --rsh")
        runner.clear_history()

        commands.rsync(
            config_path,
            source=config_path,
            target="/tmp/test_path",
            ip_address="172.0.0.4",
            override_cluster_name=None,
            down=True,
            use_internal_ip=True,
            _runner=runner,
        )
        runner.assert_has_call("172.0.0.4", pattern="rsync -e.*docker exec -i")
        runner.assert_has_call("172.0.0.4", pattern="rsync --rsh")

    def testRsyncCommandWithoutDocker(self):
        cluster_cfg = copy.deepcopy(SMALL_CLUSTER)
        cluster_cfg["docker"] = {}
        config_path = self.write_config(cluster_cfg)
        self.provider = MockProvider(unique_ips=True)
        self.provider.create_node(
            {}, {TAG_RAY_NODE_KIND: "head", TAG_RAY_NODE_STATUS: "up-to-date"}, 1
        )
        self.provider.create_node(
            {}, {TAG_RAY_NODE_KIND: "worker", TAG_RAY_NODE_STATUS: "up-to-date"}, 10
        )
        self.provider.finish_starting_nodes()
        runner = MockProcessRunner()
        ray.autoscaler.node_provider._get_node_provider = Mock(
            return_value=self.provider
        )
        ray.autoscaler._private.commands._bootstrap_config = Mock(
            return_value=cluster_cfg
        )
        commands.rsync(
            config_path,
            source=config_path,
            target="/tmp/test_path",
            override_cluster_name=None,
            down=True,
            _runner=runner,
        )
        runner.assert_has_call("1.2.3.0", pattern="rsync")

        commands.rsync(
            config_path,
            source=config_path,
            target="/tmp/test_path",
            override_cluster_name=None,
            down=True,
            ip_address="1.2.3.5",
            _runner=runner,
        )
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
            _runner=runner,
        )
        runner.assert_has_call("172.0.0.4", pattern="rsync")
        runner.clear_history()

    def testSummarizerFailedCreate(self):
        """Checks that event summarizer reports failed node creation."""
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 2
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        mock_metrics = Mock(spec=AutoscalerPrometheusMetrics())
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        self.provider.error_creates = Exception(":(")
        autoscaler = MockAutoscaler(
            config_path,
            LoadMetrics(),
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
            prom_metrics=mock_metrics,
        )
        assert len(self.provider.non_terminated_nodes(WORKER_FILTER)) == 0
        autoscaler.update()

        # Expect the next two messages in the logs.
        msg = "Failed to launch 2 node(s) of type worker."

        def expected_message_logged():
            return msg in autoscaler.event_summarizer.summary()

        self.waitFor(expected_message_logged)

    def testSummarizerFailedCreateStructuredError(self):
        """Checks that event summarizer reports failed node creation with
        additional details when the node provider thorws a
        NodeLaunchException."""
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 2
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        mock_metrics = Mock(spec=AutoscalerPrometheusMetrics())
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        self.provider.error_creates = NodeLaunchException(
            "didn't work", "never did", exc_info
        )
        autoscaler = MockAutoscaler(
            config_path,
            LoadMetrics(),
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
            prom_metrics=mock_metrics,
        )
        assert len(self.provider.non_terminated_nodes(WORKER_FILTER)) == 0
        autoscaler.update()

        # Expect the next message in the logs.
        msg = "Failed to launch 2 node(s) of type worker. " "(didn't work): never did."

        def expected_message_logged():
            print(autoscaler.event_summarizer.summary())
            return msg in autoscaler.event_summarizer.summary()

        self.waitFor(expected_message_logged)

    def testSummarizerFailedCreateStructuredErrorNoUnderlyingException(self):
        """Checks that event summarizer reports failed node creation with
        additional details when the node provider thorws a
        NodeLaunchException."""
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 2
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        mock_metrics = Mock(spec=AutoscalerPrometheusMetrics())
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        self.provider.error_creates = NodeLaunchException(
            "didn't work", "never did", src_exc_info=None
        )
        autoscaler = MockAutoscaler(
            config_path,
            LoadMetrics(),
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
            prom_metrics=mock_metrics,
        )
        assert len(self.provider.non_terminated_nodes(WORKER_FILTER)) == 0
        autoscaler.update()

        # Expect the next message in the logs.
        msg = "Failed to launch 2 node(s) of type worker. " "(didn't work): never did."

        def expected_message_logged():
            print(autoscaler.event_summarizer.summary())
            return msg in autoscaler.event_summarizer.summary()

        self.waitFor(expected_message_logged)

    def testReadonlyNodeProvider(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 2
        config_path = self.write_config(config)
        self.provider = ReadOnlyNodeProvider(config_path, "readonly")
        runner = MockProcessRunner()
        mock_metrics = Mock(spec=AutoscalerPrometheusMetrics())
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
            prom_metrics=mock_metrics,
        )
        assert len(self.provider.non_terminated_nodes({})) == 0

        # No updates in read-only mode.
        autoscaler.update()
        self.waitForNodes(0)
        assert mock_metrics.started_nodes.inc.call_count == 0
        assert len(runner.calls) == 0

        # Reflect updates to the readonly provider.
        self.provider._set_nodes(
            [
                ("foo1", "1.1.1.1"),
                ("foo2", "1.1.1.1"),
                ("foo3", "1.1.1.1"),
            ]
        )

        # No updates in read-only mode.
        autoscaler.update()
        self.waitForNodes(3)
        assert mock_metrics.started_nodes.inc.call_count == 0
        assert mock_metrics.stopped_nodes.inc.call_count == 0
        assert mock_metrics.drain_node_exceptions.inc.call_count == 0
        assert len(runner.calls) == 0
        events = autoscaler.event_summarizer.summary()
        assert not events, events

    def ScaleUpHelper(self, disable_node_updaters):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 2
        config_path = self.write_config(config)
        config["provider"]["disable_node_updaters"] = disable_node_updaters
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        mock_metrics = Mock(spec=AutoscalerPrometheusMetrics())
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        if disable_node_updaters:
            # This class raises an assertion error if we try to create
            # a node updater thread.
            autoscaler_class = NoUpdaterMockAutoscaler
        else:
            autoscaler_class = MockAutoscaler
        autoscaler = autoscaler_class(
            config_path,
            LoadMetrics(),
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
            prom_metrics=mock_metrics,
        )
        assert len(self.provider.non_terminated_nodes(WORKER_FILTER)) == 0
        autoscaler.update()
        self.waitForNodes(2, tag_filters=WORKER_FILTER)

        # started_nodes metric should have been incremented by 2
        assert mock_metrics.started_nodes.inc.call_count == 1
        mock_metrics.started_nodes.inc.assert_called_with(2)
        assert mock_metrics.worker_create_node_time.observe.call_count == 2
        autoscaler.update()
        # The two autoscaler update iterations in this test led to two
        # observations of the update time.
        assert mock_metrics.update_time.observe.call_count == 2
        self.waitForNodes(2, tag_filters=WORKER_FILTER)

        # running_workers metric should be set to 2
        mock_metrics.running_workers.set.assert_called_with(2)

        if disable_node_updaters:
            # Node Updaters have NOT been invoked because they were explicitly
            # disabled.
            assert len(runner.calls) == 0
            # Nodes were create in uninitialized and not updated.
            self.waitForNodes(
                2,
                tag_filters={
                    TAG_RAY_NODE_STATUS: STATUS_UNINITIALIZED,
                    **WORKER_FILTER,
                },
            )
        else:
            # Node Updaters have been invoked.
            self.waitFor(lambda: len(runner.calls) > 0)
            # The updates failed. Key thing is that the updates completed.
            self.waitForNodes(
                2,
                tag_filters={
                    TAG_RAY_NODE_STATUS: STATUS_UPDATE_FAILED,
                    **WORKER_FILTER,
                },
            )
        assert mock_metrics.drain_node_exceptions.inc.call_count == 0

    def testScaleUp(self):
        self.ScaleUpHelper(disable_node_updaters=False)

    def testScaleUpNoUpdaters(self):
        self.ScaleUpHelper(disable_node_updaters=True)

    def testTerminateOutdatedNodesGracefully(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 5
        config["max_workers"] = 5
        config["available_node_types"]["worker"]["max_workers"] = 5
        config_path = self.write_config(config)
        self.provider = MockProvider()
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: "worker",
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "worker",
            },
            10,
        )
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(10)])
        mock_metrics = Mock(spec=AutoscalerPrometheusMetrics())
        lm = LoadMetrics()
        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
            prom_metrics=mock_metrics,
        )
        self.waitForNodes(10, tag_filters=WORKER_FILTER)

        fill_in_raylet_ids(self.provider, lm)
        # Gradually scales down to meet target size, never going too low
        for _ in range(10):
            autoscaler.update()
            self.waitForNodes(
                5, comparison=self.assertLessEqual, tag_filters=WORKER_FILTER
            )
            self.waitForNodes(
                4, comparison=self.assertGreaterEqual, tag_filters=WORKER_FILTER
            )

        # Eventually reaches steady state
        self.waitForNodes(5, tag_filters=WORKER_FILTER)

        # Check the outdated node removal event is generated.
        autoscaler.update()
        events = autoscaler.event_summarizer.summary()
        assert "Removing 10 nodes of type " "worker (outdated)." in events, events
        assert mock_metrics.stopped_nodes.inc.call_count == 10
        mock_metrics.started_nodes.inc.assert_called_with(5)
        assert mock_metrics.worker_create_node_time.observe.call_count == 5
        assert mock_metrics.drain_node_exceptions.inc.call_count == 0

    # Parameterization functionality in the unittest module is not great.
    # To test scale-down behavior, we parameterize the DynamicScaling test
    # manually over outcomes for the DrainNode RPC call.
    def testDynamicScaling1(self):
        self.helperDynamicScaling(DrainNodeOutcome.Succeeded)

    def testDynamicScaling2(self):
        self.helperDynamicScaling(DrainNodeOutcome.NotAllDrained)

    def testDynamicScaling3(self):
        self.helperDynamicScaling(DrainNodeOutcome.Unimplemented)

    def testDynamicScaling4(self):
        self.helperDynamicScaling(DrainNodeOutcome.GenericRpcError)

    def testDynamicScaling5(self):
        self.helperDynamicScaling(DrainNodeOutcome.GenericException)

    def testDynamicScaling6(self):
        self.helperDynamicScaling(DrainNodeOutcome.FailedToFindIp)

    def testDynamicScaling7(self):
        self.helperDynamicScaling(DrainNodeOutcome.DrainDisabled)

    def testDynamicScalingForegroundLauncher(self):
        """Test autoscaling with node launcher in the foreground."""
        self.helperDynamicScaling(foreground_node_launcher=True)

    def testDynamicScalingBatchingNodeProvider(self):
        """Test autoscaling with BatchingNodeProvider"""
        self.helperDynamicScaling(
            foreground_node_launcher=True, batching_node_provider=True
        )

    def helperDynamicScaling(
        self,
        drain_node_outcome: DrainNodeOutcome = DrainNodeOutcome.Succeeded,
        foreground_node_launcher: bool = False,
        batching_node_provider: bool = False,
    ):
        mock_metrics = Mock(spec=AutoscalerPrometheusMetrics())
        mock_node_info_stub = MockNodeInfoStub(drain_node_outcome)
        disable_drain = drain_node_outcome == DrainNodeOutcome.DrainDisabled

        # Run the core of the test logic.
        self._helperDynamicScaling(
            mock_metrics,
            mock_node_info_stub,
            foreground_node_launcher=foreground_node_launcher,
            batching_node_provider=batching_node_provider,
            disable_drain=disable_drain,
        )

        # Make assertions about DrainNode error handling during scale-down.

        if drain_node_outcome == DrainNodeOutcome.Succeeded:
            # DrainNode call was made.
            assert mock_node_info_stub.drain_node_call_count > 0
            # No drain node exceptions.
            assert mock_metrics.drain_node_exceptions.inc.call_count == 0
            # Each drain node call succeeded.
            assert (
                mock_node_info_stub.drain_node_reply_success
                == mock_node_info_stub.drain_node_call_count
            )
        elif drain_node_outcome == DrainNodeOutcome.Unimplemented:
            # DrainNode call was made.
            assert mock_node_info_stub.drain_node_call_count > 0
            # All errors were supressed.
            assert mock_metrics.drain_node_exceptions.inc.call_count == 0
            # Every call failed.
            assert mock_node_info_stub.drain_node_reply_success == 0
        elif drain_node_outcome in (
            DrainNodeOutcome.GenericRpcError,
            DrainNodeOutcome.GenericException,
        ):
            # DrainNode call was made.
            assert mock_node_info_stub.drain_node_call_count > 0

            # We encountered an exception.
            assert mock_metrics.drain_node_exceptions.inc.call_count > 0
            # Every call failed.
            assert (
                mock_metrics.drain_node_exceptions.inc.call_count
                == mock_node_info_stub.drain_node_call_count
            )
            assert mock_node_info_stub.drain_node_reply_success == 0
        elif drain_node_outcome == DrainNodeOutcome.FailedToFindIp:
            # We never called the drain node api because we were unable to
            # fetch ips
            assert mock_node_info_stub.drain_node_call_count == 0
            # We encountered an exception fetching ip.
            assert mock_metrics.drain_node_exceptions.inc.call_count > 0
        elif drain_node_outcome == DrainNodeOutcome.DrainDisabled:
            # We never called this API.
            assert mock_node_info_stub.drain_node_call_count == 0
            # There were no failed calls.
            assert mock_metrics.drain_node_exceptions.inc.call_count == 0
            # There were no successful calls either.
            assert mock_node_info_stub.drain_node_reply_success == 0

    def _helperDynamicScaling(
        self,
        mock_metrics,
        mock_node_info_stub,
        foreground_node_launcher=False,
        batching_node_provider=False,
        disable_drain=False,
    ):
        if batching_node_provider:
            assert (
                foreground_node_launcher
            ), "BatchingNodeProvider requires foreground node launch."
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 2
        if foreground_node_launcher:
            config["provider"][FOREGROUND_NODE_LAUNCH_KEY] = True
        if batching_node_provider:
            config["provider"][FOREGROUND_NODE_LAUNCH_KEY] = True
            config["provider"][DISABLE_LAUNCH_CONFIG_CHECK_KEY] = True
            config["provider"][DISABLE_NODE_UPDATERS_KEY] = True
        if disable_drain:
            config["provider"][WORKER_RPC_DRAIN_KEY] = False

        config_path = self.write_config(config)
        if batching_node_provider:
            self.provider = MockBatchingNodeProvider(
                provider_config={
                    DISABLE_LAUNCH_CONFIG_CHECK_KEY: True,
                    DISABLE_NODE_UPDATERS_KEY: True,
                    FOREGROUND_NODE_LAUNCH_KEY: True,
                },
                cluster_name="test-cluster",
                _allow_multiple=True,
            )
        else:
            self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(12)])
        lm = LoadMetrics()

        # As part of setup for this test, ensure there is a head node.
        if batching_node_provider:
            # MockBatchingNodeProvider creates a head node in the __init__ method.
            pass
        else:
            # MockProvider needs to create a head node with create_node.
            self.provider.create_node(
                {},
                {
                    TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                    TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                    TAG_RAY_USER_NODE_TYPE: "head",
                },
                1,
            )
        lm.update("172.0.0.0", mock_raylet_id(), {"CPU": 1}, {"CPU": 0}, {})
        autoscaler = MockAutoscaler(
            config_path,
            lm,
            mock_node_info_stub,
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
            prom_metrics=mock_metrics,
        )
        if mock_node_info_stub.drain_node_outcome == DrainNodeOutcome.FailedToFindIp:
            autoscaler.fail_to_find_ip_during_drain = True
        self.waitForNodes(0, tag_filters=WORKER_FILTER)
        # Test aborting an autoscaler update with the batching NodeProvider.
        if batching_node_provider:
            self.provider.safe_to_scale_flag = False
            autoscaler.update()
            # The autoscaler update was aborted, so there's no change in worker count.
            assert self.num_nodes(tag_filters=WORKER_FILTER) == 0
            self.provider.safe_to_scale_flag = True

        autoscaler.update()
        if foreground_node_launcher:
            # If we launched in the foreground, shouldn't need to wait for nodes
            # to be available. (Node creation should block.)
            assert self.num_nodes(tag_filters=WORKER_FILTER) == 2, (
                self.provider.non_terminated_nodes(tag_filters=WORKER_FILTER),
                self.provider.non_terminated_nodes(tag_filters={}),
            )
        else:
            self.waitForNodes(2, tag_filters=WORKER_FILTER)

        # Update the config to reduce the cluster size
        new_config = copy.deepcopy(SMALL_CLUSTER)
        new_config["max_workers"] = 1
        new_config["available_node_types"]["worker"]["max_workers"] = 1
        new_config["available_node_types"]["worker"]["min_workers"] = 1
        self.write_config(new_config)
        fill_in_raylet_ids(self.provider, lm)
        autoscaler.update()
        self.waitForNodes(1, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})

        # Check the scale-down event is generated.
        events = autoscaler.event_summarizer.summary()
        assert "Removing 1 nodes of type worker " "(max_workers_per_type)." in events
        assert mock_metrics.stopped_nodes.inc.call_count == 1

        # Update the config to increase the cluster size
        new_config["available_node_types"]["worker"]["min_workers"] = 10
        new_config["available_node_types"]["worker"]["max_workers"] = 10
        new_config["max_workers"] = 10
        self.write_config(new_config)
        autoscaler.update()
        # Because one worker already started, the scheduler waits for its
        # resources to be updated before it launches the remaining min_workers.
        worker_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER},
        )[0]
        lm.update(worker_ip, mock_raylet_id(), {"CPU": 1}, {"CPU": 1}, {})
        autoscaler.update()
        if foreground_node_launcher:
            # If we launched in the foreground, shouldn't need to wait for nodes
            # to be available. (Node creation should block.)
            assert self.num_nodes(tag_filters=WORKER_FILTER) == 10
        else:
            self.waitForNodes(10, tag_filters=WORKER_FILTER)

        # Awkward and unecessary to repeat the following check for BatchingNodeProvider.
        if not batching_node_provider:
            # Verify that worker nodes were launched in the main thread if foreground
            # node launch is enabled, in a subthread otherwise.
            self.worker_node_thread_check(foreground_node_launcher)

        autoscaler.update()
        assert mock_metrics.running_workers.set.call_args_list[-1][0][0] >= 10

    def testAggressiveAutoscaling(self):
        self._aggressiveAutoscalingHelper()

    def testAggressiveAutoscalingWithForegroundLauncher(self):
        self._aggressiveAutoscalingHelper(foreground_node_launcher=True)

    def _aggressiveAutoscalingHelper(self, foreground_node_launcher: bool = False):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 0
        config["available_node_types"]["worker"]["max_workers"] = 10
        config["max_workers"] = 10
        config["idle_timeout_minutes"] = 0
        config["upscaling_speed"] = config["available_node_types"]["worker"][
            "max_workers"
        ]
        if foreground_node_launcher:
            config["provider"][FOREGROUND_NODE_LAUNCH_KEY] = True
        config_path = self.write_config(config)

        self.provider = MockProvider()
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        head_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_HEAD},
        )[0]
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(11)])
        lm = LoadMetrics()

        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )

        self.waitForNodes(1)
        lm.update(
            head_ip,
            mock_raylet_id(),
            {"CPU": 1},
            {"CPU": 0},
            {},
            waiting_bundles=[{"CPU": 1}] * 7,
            infeasible_bundles=[{"CPU": 1}] * 3,
        )
        autoscaler.update()

        if foreground_node_launcher:
            # No wait if node launch is blocking and happens in the foreground.
            assert self.num_nodes() == 11
        else:
            self.waitForNodes(11)
        self.worker_node_thread_check(foreground_node_launcher)

        worker_ips = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER},
        )
        for ip in worker_ips:
            # Mark workers inactive.
            lm.last_used_time_by_ip[ip] = 0
        # Clear the resource demands.
        # Otherwise in "foreground launcher" mode, workers would be deleted
        # for being idle and instantly re-created due to resource demand!
        lm.update(
            head_ip,
            mock_raylet_id(),
            {},
            {},
            {},
            waiting_bundles=[],
            infeasible_bundles=[],
        )
        autoscaler.update()
        self.waitForNodes(1)  # only the head node
        # Make sure they don't get overwritten.
        assert autoscaler.resource_demand_scheduler.node_types["head"]["resources"] == {
            "CPU": 1
        }
        assert autoscaler.resource_demand_scheduler.node_types["worker"][
            "resources"
        ] == {"CPU": 1}

    def testUnmanagedNodes(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 0
        config["available_node_types"]["worker"]["max_workers"] = 20
        config["max_workers"] = 20
        config["idle_timeout_minutes"] = 0
        config["upscaling_speed"] = 9999
        config_path = self.write_config(config)

        self.provider = MockProvider()
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: "head",
                TAG_RAY_USER_NODE_TYPE: "head",
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            },
            1,
        )
        head_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: "head"},
        )[0]

        self.provider.create_node({}, {TAG_RAY_NODE_KIND: "unmanaged"}, 1)
        unmanaged_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: "unmanaged"},
        )[0]

        runner = MockProcessRunner()

        lm = LoadMetrics()
        lm.local_ip = head_ip

        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )

        autoscaler.update()
        self.waitForNodes(2)
        # This node has num_cpus=0
        lm.update(head_ip, mock_raylet_id(), {"CPU": 1}, {"CPU": 0}, {})
        lm.update(unmanaged_ip, mock_raylet_id(), {"CPU": 0}, {"CPU": 0}, {})
        autoscaler.update()
        self.waitForNodes(2)
        # 1 CPU task cannot be scheduled.
        lm.update(
            unmanaged_ip,
            mock_raylet_id(),
            {"CPU": 0},
            {"CPU": 0},
            {},
            waiting_bundles=[{"CPU": 1}],
        )
        autoscaler.update()
        self.waitForNodes(3)

    def testUnmanagedNodes2(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 0
        config["available_node_types"]["worker"]["max_workers"] = 20
        config["max_workers"] = 20
        config["idle_timeout_minutes"] = 0
        config["upscaling_speed"] = 9999
        config_path = self.write_config(config)

        self.provider = MockProvider()
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: "head",
                TAG_RAY_USER_NODE_TYPE: "head",
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            },
            1,
        )
        head_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: "head"},
        )[0]

        self.provider.create_node({}, {TAG_RAY_NODE_KIND: "unmanaged"}, 1)
        unmanaged_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: "unmanaged"},
        )[0]
        unmanaged_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: "unmanaged"},
        )[0]

        runner = MockProcessRunner()

        lm = LoadMetrics()
        lm.local_ip = head_ip

        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )

        lm.update(head_ip, mock_raylet_id(), {"CPU": 1}, {"CPU": 0}, {"CPU": 1})
        lm.update(unmanaged_ip, mock_raylet_id(), {"CPU": 0}, {"CPU": 0}, {})

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
        lm = LoadMetrics()
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        head_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_HEAD},
        )[0]
        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )
        assert (
            len(
                self.provider.non_terminated_nodes(
                    {TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
                )
            )
            == 0
        )

        # Update will try to create, but will block until we set the flag
        self.provider.ready_to_create.clear()
        lm.update(
            head_ip,
            mock_raylet_id(),
            {"CPU": 1},
            {"CPU": 0},
            {},
            waiting_bundles=[{"CPU": 1}] * 2,
        )
        autoscaler.update()
        assert (
            len(
                self.provider.non_terminated_nodes(
                    {TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
                )
            )
            == 0
        )
        assert autoscaler.pending_launches.value == 2

        # Set the flag, check it updates
        self.provider.ready_to_create.set()
        self.waitForNodes(2, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})
        assert autoscaler.pending_launches.value == 0

        # Update the config to reduce the cluster size
        new_config = copy.deepcopy(SMALL_CLUSTER)
        new_config["available_node_types"]["worker"]["max_workers"] = 1
        self.write_config(new_config)
        fill_in_raylet_ids(self.provider, lm)
        autoscaler.update()
        assert (
            len(
                self.provider.non_terminated_nodes(
                    {TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
                )
            )
            == 1
        )

    def testDelayedLaunchWithMinWorkers(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 10
        config["available_node_types"]["worker"]["max_workers"] = 10
        config["max_workers"] = 10
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(10)])
        mock_metrics = Mock(spec=AutoscalerPrometheusMetrics())
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        autoscaler = MockAutoscaler(
            config_path,
            LoadMetrics(),
            MockNodeInfoStub(),
            max_launch_batch=5,
            max_concurrent_launches=8,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
            prom_metrics=mock_metrics,
        )
        assert (
            len(
                self.provider.non_terminated_nodes(
                    {TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
                )
            )
            == 0
        )

        # update() should launch a wave of 5 nodes (max_launch_batch)
        # Force this first wave to block.
        rtc1 = self.provider.ready_to_create
        rtc1.clear()
        autoscaler.update()
        # Synchronization: wait for launchy thread to be blocked on rtc1
        waiters = rtc1._cond._waiters
        self.waitFor(lambda: len(waiters) == 2)
        assert autoscaler.pending_launches.value == 10
        mock_metrics.pending_nodes.set.assert_called_with(10)
        assert (
            len(
                self.provider.non_terminated_nodes(
                    {TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
                )
            )
            == 0
        )
        autoscaler.update()
        self.waitForNodes(
            0, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
        )  # Nodes are not added on top of pending.
        rtc1.set()
        self.waitFor(lambda: autoscaler.pending_launches.value == 0)
        assert (
            len(
                self.provider.non_terminated_nodes(
                    {TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
                )
            )
            == 10
        )
        self.waitForNodes(10, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})
        assert autoscaler.pending_launches.value == 0
        mock_metrics.pending_nodes.set.assert_called_with(0)
        autoscaler.update()
        self.waitForNodes(10, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})
        assert autoscaler.pending_launches.value == 0
        mock_metrics.pending_nodes.set.assert_called_with(0)
        assert mock_metrics.drain_node_exceptions.inc.call_count == 0

    def testUpdateThrottling(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 2
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        autoscaler = MockAutoscaler(
            config_path,
            LoadMetrics(),
            MockNodeInfoStub(),
            max_launch_batch=5,
            max_concurrent_launches=5,
            max_failures=0,
            process_runner=runner,
            update_interval_s=10,
        )
        autoscaler.update()
        self.waitForNodes(2, tag_filters=WORKER_FILTER)
        assert autoscaler.pending_launches.value == 0
        new_config = copy.deepcopy(SMALL_CLUSTER)
        new_config["max_workers"] = 1
        self.write_config(new_config)
        autoscaler.update()
        # not updated yet
        # note that node termination happens in the main thread, so
        # we do not need to add any delay here before checking
        assert len(self.provider.non_terminated_nodes(WORKER_FILTER)) == 2
        assert autoscaler.pending_launches.value == 0

    def testLaunchConfigChange(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 2
        config_path = self.write_config(config)
        self.provider = MockProvider()
        lm = LoadMetrics()
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        autoscaler = MockAutoscaler(
            config_path, lm, MockNodeInfoStub(), max_failures=0, update_interval_s=0
        )
        autoscaler.update()
        self.waitForNodes(2, tag_filters=WORKER_FILTER)

        # Update the config to change the node type
        new_config = copy.deepcopy(config)
        new_config["available_node_types"]["worker"]["node_config"][
            "InstanceType"
        ] = "updated"
        self.write_config(new_config)
        self.provider.ready_to_create.clear()
        fill_in_raylet_ids(self.provider, lm)
        for _ in range(5):
            autoscaler.update()
        self.waitForNodes(0, tag_filters=WORKER_FILTER)
        self.provider.ready_to_create.set()
        self.waitForNodes(2, tag_filters=WORKER_FILTER)

    def testIgnoresCorruptedConfig(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 2
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(11)])
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        lm = LoadMetrics()
        lm.update("172.0.0.0", mock_raylet_id(), {"CPU": 1}, {"CPU": 0}, {})
        mock_metrics = Mock(spec=AutoscalerPrometheusMetrics())
        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_launch_batch=10,
            max_concurrent_launches=10,
            process_runner=runner,
            max_failures=0,
            update_interval_s=0,
            prom_metrics=mock_metrics,
        )
        autoscaler.update()
        assert mock_metrics.config_validation_exceptions.inc.call_count == 0
        self.waitForNodes(2, tag_filters=WORKER_FILTER)

        # Write a corrupted config
        self.write_config("asdf", call_prepare_config=False)
        for _ in range(10):
            autoscaler.update()
        # config validation exceptions metrics should be incremented 10 times
        assert mock_metrics.config_validation_exceptions.inc.call_count == 10
        time.sleep(0.1)
        assert autoscaler.pending_launches.value == 0
        assert (
            len(
                self.provider.non_terminated_nodes(
                    {TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
                )
            )
            == 2
        )

        # New a good config again
        new_config = copy.deepcopy(SMALL_CLUSTER)
        new_config["available_node_types"]["worker"]["min_workers"] = 10
        new_config["max_workers"] = 10
        new_config["available_node_types"]["worker"]["max_workers"] = 10
        self.write_config(new_config)
        worker_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER},
        )[0]
        # Because one worker already started, the scheduler waits for its
        # resources to be updated before it launches the remaining min_workers.
        lm.update(worker_ip, mock_raylet_id(), {"CPU": 1}, {"CPU": 1}, {})
        autoscaler.update()
        self.waitForNodes(10, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})
        assert mock_metrics.drain_node_exceptions.inc.call_count == 0

    def testMaxFailures(self):
        config_path = self.write_config(SMALL_CLUSTER)
        self.provider = MockProvider()
        self.provider.throw = True
        runner = MockProcessRunner()
        mock_metrics = Mock(spec=AutoscalerPrometheusMetrics())
        autoscaler = MockAutoscaler(
            config_path,
            LoadMetrics(),
            MockNodeInfoStub(),
            max_failures=2,
            process_runner=runner,
            update_interval_s=0,
            prom_metrics=mock_metrics,
        )
        autoscaler.update()
        assert autoscaler.summary() is None
        assert mock_metrics.update_loop_exceptions.inc.call_count == 1
        autoscaler.update()
        assert mock_metrics.update_loop_exceptions.inc.call_count == 2
        with pytest.raises(Exception):
            autoscaler.update()
        assert mock_metrics.drain_node_exceptions.inc.call_count == 0

    def testLaunchNewNodeOnOutOfBandTerminate(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 2
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(4)])
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        head_ip = self.provider.non_terminated_node_ips(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_HEAD},
        )[0]
        autoscaler = MockAutoscaler(
            config_path,
            LoadMetrics(),
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )
        autoscaler.update()
        autoscaler.update()
        self.waitForNodes(2, tag_filters=WORKER_FILTER)
        for node in self.provider.mock_nodes.values():
            if node.internal_ip == head_ip:
                continue
            node.state = "terminated"
        assert len(self.provider.non_terminated_nodes(WORKER_FILTER)) == 0
        autoscaler.update()
        self.waitForNodes(2, tag_filters=WORKER_FILTER)

    def testConfiguresNewNodes(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 1
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(2)])
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        autoscaler = MockAutoscaler(
            config_path,
            LoadMetrics(),
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )
        autoscaler.update()
        autoscaler.update()
        self.waitForNodes(2)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})

    def testReportsConfigFailures(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 2
        config_path = self.write_config(config)
        config["provider"]["type"] = "mock"
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner(fail_cmds=["setup_cmd"])
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(2)])
        lm = LoadMetrics()
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )
        autoscaler.update()
        self.waitForNodes(2, tag_filters=WORKER_FILTER)
        self.provider.finish_starting_nodes()
        fill_in_raylet_ids(self.provider, lm)
        autoscaler.update()
        try:
            self.waitForNodes(
                2,
                tag_filters={
                    TAG_RAY_NODE_STATUS: STATUS_UPDATE_FAILED,
                    **WORKER_FILTER,
                },
            )
        except AssertionError:
            # The failed nodes might have been already terminated by autoscaler
            assert len(self.provider.non_terminated_nodes({})) < 2

        # Check the launch failure event is generated.
        autoscaler.update()
        events = autoscaler.event_summarizer.summary()
        assert "Removing 2 nodes of type " "worker (launch failed)." in events, events

    def testConfiguresOutdatedNodes(self):
        from ray.autoscaler._private.cli_logger import cli_logger

        def do_nothing(*args, **kwargs):
            pass

        cli_logger._print = type(cli_logger._print)(do_nothing, type(cli_logger))

        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 1
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(4)])
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        autoscaler = MockAutoscaler(
            config_path,
            LoadMetrics(),
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )
        autoscaler.update()
        autoscaler.update()
        self.waitForNodes(2)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})
        runner.calls = []
        new_config = copy.deepcopy(SMALL_CLUSTER)
        new_config["worker_setup_commands"] = ["cmdX", "cmdY"]
        self.write_config(new_config)
        autoscaler.update()
        autoscaler.update()
        self.waitFor(lambda: len(runner.calls) > 0)

    def testScaleDownMaxWorkers(self):
        """Tests terminating nodes due to max_nodes per type."""
        config = copy.deepcopy(MULTI_WORKER_CLUSTER)
        config["available_node_types"]["m4.large"]["min_workers"] = 3
        config["available_node_types"]["m4.large"]["max_workers"] = 3
        config["available_node_types"]["m4.large"]["resources"] = {}
        config["available_node_types"]["m4.16xlarge"]["resources"] = {}
        config["available_node_types"]["p2.xlarge"]["min_workers"] = 5
        config["available_node_types"]["p2.xlarge"]["max_workers"] = 8
        config["available_node_types"]["p2.xlarge"]["resources"] = {}
        config["available_node_types"]["p2.8xlarge"]["min_workers"] = 2
        config["available_node_types"]["p2.8xlarge"]["max_workers"] = 4
        config["available_node_types"]["p2.8xlarge"]["resources"] = {}
        config["max_workers"] = 13

        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(15)])
        lm = LoadMetrics()

        get_or_create_head_node(
            config,
            printable_config_file=config_path,
            no_restart=False,
            restart_only=False,
            yes=True,
            override_cluster_name=None,
            _provider=self.provider,
            _runner=runner,
        )
        self.waitForNodes(1)

        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_failures=0,
            max_concurrent_launches=13,
            max_launch_batch=13,
            process_runner=runner,
            update_interval_s=0,
        )
        autoscaler.update()
        self.waitForNodes(11)
        assert autoscaler.pending_launches.value == 0
        assert (
            len(
                self.provider.non_terminated_nodes(
                    {TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
                )
            )
            == 10
        )

        # Terminate some nodes
        config["available_node_types"]["m4.large"]["min_workers"] = 2  # 3
        config["available_node_types"]["m4.large"]["max_workers"] = 2
        config["available_node_types"]["p2.8xlarge"]["min_workers"] = 0  # 2
        config["available_node_types"]["p2.8xlarge"]["max_workers"] = 0
        # And spawn one.
        config["available_node_types"]["p2.xlarge"]["min_workers"] = 6  # 5
        config["available_node_types"]["p2.xlarge"]["max_workers"] = 6
        self.write_config(config)
        fill_in_raylet_ids(self.provider, lm)
        autoscaler.update()
        events = autoscaler.event_summarizer.summary()
        self.waitFor(lambda: autoscaler.pending_launches.value == 0)
        self.waitForNodes(8, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})
        assert autoscaler.pending_launches.value == 0
        events = autoscaler.event_summarizer.summary()
        assert "Removing 1 nodes of type m4.large (max_workers_per_type)." in events
        assert "Removing 2 nodes of type p2.8xlarge (max_workers_per_type)." in events

        # We should not be starting/stopping empty_node at all.
        for event in events:
            assert "empty_node" not in event

        node_type_counts = defaultdict(int)
        for node_id in NonTerminatedNodes(self.provider).worker_ids:
            tags = self.provider.node_tags(node_id)
            if TAG_RAY_USER_NODE_TYPE in tags:
                node_type = tags[TAG_RAY_USER_NODE_TYPE]
                node_type_counts[node_type] += 1
        assert node_type_counts == {"m4.large": 2, "p2.xlarge": 6}

    def testFalseyLoadMetrics(self):
        lm = LoadMetrics()
        assert not lm
        lm.update("172.0.0.0", mock_raylet_id(), {"CPU": 1}, {"CPU": 0}, {})
        assert lm

    def testRecoverUnhealthyWorkers(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 2
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(3)])
        lm = LoadMetrics()
        mock_metrics = Mock(spec=AutoscalerPrometheusMetrics())
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
            prom_metrics=mock_metrics,
        )
        autoscaler.update()
        self.waitForNodes(2, tag_filters=WORKER_FILTER)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
        )

        # Mark a node as unhealthy
        for _ in range(5):
            if autoscaler.updaters:
                time.sleep(0.05)
                autoscaler.update()
        assert not autoscaler.updaters
        mock_metrics.recovering_nodes.set.assert_called_with(0)
        num_calls = len(runner.calls)
        lm.last_heartbeat_time_by_ip["172.0.0.1"] = 0
        autoscaler.update()
        mock_metrics.recovering_nodes.set.assert_called_with(1)
        self.waitFor(lambda: len(runner.calls) > num_calls, num_retries=150)

        # Check the node removal event is generated.
        autoscaler.update()
        events = autoscaler.event_summarizer.summary()
        assert (
            "Restarting 1 nodes of type " "worker (lost contact with raylet)." in events
        ), events
        assert mock_metrics.drain_node_exceptions.inc.call_count == 0

    def testTerminateUnhealthyWorkers(self):
        """Test termination of unhealthy workers, when
        autoscaler.disable_node_updaters == True.

        Similar to testRecoverUnhealthyWorkers.
        """
        self.unhealthyWorkerHelper(disable_liveness_check=False)

    def testDontTerminateUnhealthyWorkers(self):
        """Test that the autoscaler leaves unhealthy workers alone when the worker
        liveness check is disabled.
        """
        self.unhealthyWorkerHelper(disable_liveness_check=True)

    def unhealthyWorkerHelper(self, disable_liveness_check: bool):
        """Helper used to test the autoscaler's handling of unhealthy worker nodes.
        If disable liveness check is False, the default code path is tested and we
        expect to see workers terminated.

        If disable liveness check is True, we expect the autoscaler not to take action
        on unhealthy nodes, instead delegating node management to another component.
        """
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 2
        # Make it clear we're not timing out idle nodes here.
        config["idle_timeout_minutes"] = 1000000000
        if disable_liveness_check:
            config["provider"][WORKER_LIVENESS_CHECK_KEY] = False
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(3)])
        lm = LoadMetrics()
        mock_metrics = Mock(spec=AutoscalerPrometheusMetrics())
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
            prom_metrics=mock_metrics,
        )
        autoscaler.update()
        self.waitForNodes(2, tag_filters=WORKER_FILTER)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
        )

        # Clear out updaters.
        for _ in range(5):
            if autoscaler.updaters:
                time.sleep(0.05)
                autoscaler.update()
        assert not autoscaler.updaters

        num_calls = len(runner.calls)

        # Mark a node as unhealthy
        lm.last_heartbeat_time_by_ip["172.0.0.1"] = 0
        # Turn off updaters.
        autoscaler.disable_node_updaters = True
        # Reduce min_workers to 1
        autoscaler.config["available_node_types"]["worker"]["min_workers"] = 1
        fill_in_raylet_ids(self.provider, lm)

        if disable_liveness_check:
            # We've disabled the liveness check, so the unhealthy node should stick
            # around until someone else takes care of it.
            # Do several autoscaler updates, to reinforce the fact that the
            # autoscaler will never take down the unhealthy nodes.
            for _ in range(10):
                autoscaler.update()
            # The nodes are still there.
            assert self.num_nodes(tag_filters=WORKER_FILTER) == 2
            # There's no synchronization required to make the last assertion valid:
            # The autoscaler's node termination is synchronous and blocking, as is
            # the terminate_node method of the mock node provider used in this test.

            # No events generated indicating that we are removing nodes.
            for event in autoscaler.event_summarizer.summary():
                assert "Removing" not in event
        else:
            # We expect the unhealthy node to be cleared out with a single
            # autoscaler update.
            autoscaler.update()
            # Stopped node metric incremented.
            mock_metrics.stopped_nodes.inc.assert_called_once_with()
            # One node left.
            self.waitForNodes(1, tag_filters=WORKER_FILTER)

            # Check the node removal event is generated.
            autoscaler.update()
            events = autoscaler.event_summarizer.summary()
            assert (
                "Removing 1 nodes of type "
                "worker (lost contact with raylet)." in events
            ), events

            # No additional runner calls, since updaters were disabled.
            assert len(runner.calls) == num_calls
            assert mock_metrics.drain_node_exceptions.inc.call_count == 0

    def testTerminateUnhealthyWorkers2(self):
        """Tests finer details of termination of unhealthy workers when
        node updaters are disabled.

        Specifically, test that newly up-to-date nodes which haven't sent a
        heartbeat are marked active.
        """
        config = copy.deepcopy(SMALL_CLUSTER)
        config["provider"]["disable_node_updaters"] = True
        config["available_node_types"]["worker"]["min_workers"] = 2
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        mock_metrics = Mock(spec=AutoscalerPrometheusMetrics())
        lm = LoadMetrics()
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
            prom_metrics=mock_metrics,
        )
        assert len(self.provider.non_terminated_nodes(WORKER_FILTER)) == 0
        for _ in range(10):
            autoscaler.update()
            # Nodes stay in uninitialized state because no one has finished
            # updating them.
            self.waitForNodes(
                2,
                tag_filters={
                    TAG_RAY_NODE_STATUS: STATUS_UNINITIALIZED,
                    **WORKER_FILTER,
                },
            )
        nodes = self.provider.non_terminated_nodes(WORKER_FILTER)
        ips = [self.provider.internal_ip(node) for node in nodes]
        # No heartbeats recorded yet.
        assert not any(ip in lm.last_heartbeat_time_by_ip for ip in ips)
        for node in nodes:
            self.provider.set_node_tags(
                node, {TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
            )
        autoscaler.update()
        # Nodes marked active after up-to-date status detected.
        assert all(ip in lm.last_heartbeat_time_by_ip for ip in ips)
        # Nodes are kept.
        self.waitForNodes(
            2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
        )
        # Mark nodes unhealthy.
        for ip in ips:
            lm.last_heartbeat_time_by_ip[ip] = 0
        fill_in_raylet_ids(self.provider, lm)
        autoscaler.update()
        # Unhealthy nodes are gone.
        self.waitForNodes(0, tag_filters=WORKER_FILTER)
        autoscaler.update()
        # IPs pruned
        assert lm.last_heartbeat_time_by_ip == {}
        assert mock_metrics.drain_node_exceptions.inc.call_count == 0

    def testExternalNodeScaler(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["provider"] = {
            "type": "external",
            "module": "ray.autoscaler.node_provider.NodeProvider",
        }
        config_path = self.write_config(config)
        autoscaler = MockAutoscaler(
            config_path,
            LoadMetrics(),
            MockNodeInfoStub(),
            max_failures=0,
            update_interval_s=0,
        )
        assert isinstance(autoscaler.provider, NodeProvider)

    def testExternalNodeScalerWrongImport(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["provider"] = {
            "type": "external",
            "module": "mymodule.provider_class",
        }
        invalid_provider = self.write_config(config)
        with pytest.raises(ImportError):
            MockAutoscaler(
                invalid_provider, LoadMetrics(), MockNodeInfoStub(), update_interval_s=0
            )

    def testExternalNodeScalerWrongModuleFormat(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["provider"] = {
            "type": "external",
            "module": "does-not-exist",
        }
        invalid_provider = self.write_config(config, call_prepare_config=False)
        with pytest.raises(ValueError):
            MockAutoscaler(
                invalid_provider, LoadMetrics(), MockNodeInfoStub(), update_interval_s=0
            )

    def testSetupCommandsWithNoNodeCaching(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 1
        config["available_node_types"]["worker"]["max_workers"] = 1
        config["max_workers"] = 1
        config_path = self.write_config(config)
        self.provider = MockProvider(cache_stopped=False)
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(2)])
        lm = LoadMetrics()
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )
        autoscaler.update()
        self.waitForNodes(1, tag_filters=WORKER_FILTER)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            1, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
        )
        worker_ip = self.provider.non_terminated_node_ips(WORKER_FILTER)[0]
        runner.assert_has_call(worker_ip, "init_cmd")
        runner.assert_has_call(worker_ip, "setup_cmd")
        runner.assert_has_call(worker_ip, "worker_setup_cmd")
        runner.assert_has_call(worker_ip, "start_ray_worker")

        # Check the node was not reused
        self.provider.terminate_node(1)
        autoscaler.update()
        runner.clear_history()
        self.waitForNodes(1, tag_filters=WORKER_FILTER)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            1, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
        )
        new_worker_ip = self.provider.non_terminated_node_ips(WORKER_FILTER)[0]
        runner.assert_has_call(new_worker_ip, "init_cmd")
        runner.assert_has_call(new_worker_ip, "setup_cmd")
        runner.assert_has_call(new_worker_ip, "worker_setup_cmd")
        runner.assert_has_call(new_worker_ip, "start_ray_worker")
        assert worker_ip != new_worker_ip

    def testSetupCommandsWithStoppedNodeCachingNoDocker(self):
        file_mount_dir = tempfile.mkdtemp()
        config = copy.deepcopy(SMALL_CLUSTER)
        del config["docker"]
        config["file_mounts"] = {"/root/test-folder": file_mount_dir}
        config["file_mounts_sync_continuously"] = True
        config["available_node_types"]["worker"]["min_workers"] = 1
        config["available_node_types"]["worker"]["max_workers"] = 1
        config["max_workers"] = 1
        config_path = self.write_config(config)
        self.provider = MockProvider(cache_stopped=True)
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(3)])
        lm = LoadMetrics()
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )
        autoscaler.update()
        self.waitForNodes(1, tag_filters=WORKER_FILTER)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            1, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
        )
        worker_ip = self.provider.non_terminated_node_ips(WORKER_FILTER)[0]
        runner.assert_has_call(worker_ip, "init_cmd")
        runner.assert_has_call(worker_ip, "setup_cmd")
        runner.assert_has_call(worker_ip, "worker_setup_cmd")
        runner.assert_has_call(worker_ip, "start_ray_worker")

        # Check the node was indeed reused
        self.provider.terminate_node(1)
        runner.clear_history()
        autoscaler.update()
        self.waitForNodes(1, tag_filters=WORKER_FILTER)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            1, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
        )
        runner.assert_not_has_call(worker_ip, "init_cmd")
        runner.assert_not_has_call(worker_ip, "setup_cmd")
        runner.assert_not_has_call(worker_ip, "worker_setup_cmd")
        runner.assert_has_call(worker_ip, "start_ray_worker")

        with open(f"{file_mount_dir}/new_file", "w") as f:
            f.write("abcdefgh")

        # Check that run_init happens when file_mounts have updated
        self.provider.terminate_node(1)
        autoscaler.update()
        runner.clear_history()
        self.waitForNodes(1, tag_filters=WORKER_FILTER)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            1, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
        )
        runner.assert_not_has_call(worker_ip, "init_cmd")
        runner.assert_not_has_call(worker_ip, "setup_cmd")
        runner.assert_not_has_call(worker_ip, "worker_setup_cmd")
        runner.assert_has_call(worker_ip, "start_ray_worker")

        autoscaler.update()
        runner.assert_not_has_call(worker_ip, "setup_cmd")

        # We did not start any other nodes
        self.waitForNodes(
            1, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
        )

    def testSetupCommandsWithStoppedNodeCachingDocker(self):
        # NOTE(ilr) Setup & Init commands **should** run with stopped nodes
        # when Docker is in use.
        file_mount_dir = tempfile.mkdtemp()
        config = copy.deepcopy(SMALL_CLUSTER)
        config["file_mounts"] = {"/root/test-folder": file_mount_dir}
        config["file_mounts_sync_continuously"] = True
        config["available_node_types"]["worker"]["min_workers"] = 1
        config["available_node_types"]["worker"]["max_workers"] = 1
        config["max_workers"] = 1
        config_path = self.write_config(config)
        self.provider = MockProvider(cache_stopped=True)
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(3)])
        lm = LoadMetrics()
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )
        autoscaler.update()
        self.waitForNodes(1, tag_filters=WORKER_FILTER)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            1, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
        )
        worker_ip = self.provider.non_terminated_node_ips(WORKER_FILTER)[0]
        runner.assert_has_call(worker_ip, "init_cmd")
        runner.assert_has_call(worker_ip, "setup_cmd")
        runner.assert_has_call(worker_ip, "worker_setup_cmd")
        runner.assert_has_call(worker_ip, "start_ray_worker")
        runner.assert_has_call(worker_ip, "docker run")

        # Check the node was indeed reused
        self.provider.terminate_node(1)
        runner.clear_history()
        autoscaler.update()
        self.waitForNodes(1, tag_filters=WORKER_FILTER)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            1, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
        )
        print(runner.command_history())
        # These all must happen when the node is stopped and resued
        runner.assert_has_call(worker_ip, "init_cmd")
        runner.assert_has_call(worker_ip, "setup_cmd")
        runner.assert_has_call(worker_ip, "worker_setup_cmd")
        runner.assert_has_call(worker_ip, "start_ray_worker")
        runner.assert_has_call(worker_ip, "docker run")

        with open(f"{file_mount_dir}/new_file", "w") as f:
            f.write("abcdefgh")

        # Check that run_init happens when file_mounts have updated
        self.provider.terminate_node(0)
        runner.clear_history()
        autoscaler.update()
        self.waitForNodes(1, tag_filters=WORKER_FILTER)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            1, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
        )
        runner.assert_has_call(worker_ip, "init_cmd")
        runner.assert_has_call(worker_ip, "setup_cmd")
        runner.assert_has_call(worker_ip, "worker_setup_cmd")
        runner.assert_has_call(worker_ip, "start_ray_worker")
        runner.assert_has_call(worker_ip, "docker run")

        docker_run_cmd_indx = [
            i for i, cmd in enumerate(runner.command_history()) if "docker run" in cmd
        ][0]
        mkdir_cmd_indx = [
            i for i, cmd in enumerate(runner.command_history()) if "mkdir -p" in cmd
        ][0]
        assert mkdir_cmd_indx < docker_run_cmd_indx
        runner.clear_history()
        autoscaler.update()
        runner.assert_not_has_call(worker_ip, "setup_cmd")

        # We did not start any other nodes
        runner.assert_not_has_call("172.0.0.2", " ")

    def testMultiNodeReuse(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        # Docker re-runs setup commands when nodes are reused.
        del config["docker"]
        config["available_node_types"]["worker"]["min_workers"] = 3
        config["available_node_types"]["worker"]["max_workers"] = 3
        config["max_workers"] = 3
        config_path = self.write_config(config)
        self.provider = MockProvider(cache_stopped=True)
        runner = MockProcessRunner()
        lm = LoadMetrics()
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )
        autoscaler.update()
        self.waitForNodes(3, tag_filters=WORKER_FILTER)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            3, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
        )

        self.provider.terminate_node(1)
        self.provider.terminate_node(2)
        self.provider.terminate_node(3)
        runner.clear_history()

        # Scale up to 10 nodes, check we reuse the first 3 and add 5 more.
        config["available_node_types"]["worker"]["min_workers"] = 8
        config["available_node_types"]["worker"]["max_workers"] = 8
        config["max_workers"] = 8
        self.write_config(config)
        autoscaler.update()
        autoscaler.update()
        self.waitForNodes(8, tag_filters=WORKER_FILTER)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            8, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
        )
        autoscaler.update()
        for i in [1, 2, 3]:
            runner.assert_not_has_call("172.0.0.{}".format(i), "setup_cmd")
            runner.assert_has_call("172.0.0.{}".format(i), "start_ray_worker")
        for i in range(4, 9):
            runner.assert_has_call("172.0.0.{}".format(i), "setup_cmd")
            runner.assert_has_call("172.0.0.{}".format(i), "start_ray_worker")

    def testContinuousFileMounts(self):
        file_mount_dir = tempfile.mkdtemp()

        self.provider = MockProvider()
        config = copy.deepcopy(SMALL_CLUSTER)
        config["file_mounts"] = {"/home/test-folder": file_mount_dir}
        config["file_mounts_sync_continuously"] = True
        config["available_node_types"]["worker"]["min_workers"] = 2
        config_path = self.write_config(config)
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(4)])
        runner.respond_to_call("command -v docker", ["docker" for _ in range(4)])
        lm = LoadMetrics()
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )

        autoscaler.update()
        self.waitForNodes(3)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(3, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})
        autoscaler.update()
        docker_mount_prefix = get_docker_host_mount_location(config["cluster_name"])
        for i in self.provider.non_terminated_nodes(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
        ):
            runner.assert_has_call(f"172.0.0.{i}", "setup_cmd")
            runner.assert_has_call(
                f"172.0.0.{i}",
                f"{file_mount_dir}/ ubuntu@172.0.0.{i}:"
                f"{docker_mount_prefix}/home/test-folder/",
            )

        runner.clear_history()

        with open(os.path.join(file_mount_dir, "test.txt"), "wb") as temp_file:
            temp_file.write("hello".encode())

        runner.respond_to_call(".Config.Image", ["example" for _ in range(4)])
        runner.respond_to_call(".State.Running", ["true" for _ in range(4)])
        autoscaler.update()
        self.waitForNodes(3)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(3, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})
        autoscaler.update()

        for i in self.provider.non_terminated_nodes(
            tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
        ):
            runner.assert_not_has_call(f"172.0.0.{i}", "setup_cmd")
            runner.assert_has_call(
                f"172.0.0.{i}",
                f"{file_mount_dir}/ ubuntu@172.0.0.{i}:"
                f"{docker_mount_prefix}/home/test-folder/",
            )

    def testFileMountsNonContinuous(self):
        file_mount_dir = tempfile.mkdtemp()

        self.provider = MockProvider()
        config = copy.deepcopy(SMALL_CLUSTER)
        config["file_mounts"] = {"/home/test-folder": file_mount_dir}
        config["available_node_types"]["worker"]["min_workers"] = 2
        config_path = self.write_config(config)
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(2)])
        lm = LoadMetrics()
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )

        autoscaler.update()
        self.waitForNodes(2, tag_filters=WORKER_FILTER)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
        )
        autoscaler.update()
        docker_mount_prefix = get_docker_host_mount_location(config["cluster_name"])

        for i in self.provider.non_terminated_nodes(WORKER_FILTER):
            runner.assert_has_call(f"172.0.0.{i}", "setup_cmd")
            runner.assert_has_call(
                f"172.0.0.{i}",
                f"{file_mount_dir}/ ubuntu@172.0.0.{i}:"
                f"{docker_mount_prefix}/home/test-folder/",
            )

        runner.clear_history()

        with open(os.path.join(file_mount_dir, "test.txt"), "wb") as temp_file:
            temp_file.write("hello".encode())

        autoscaler.update()
        self.waitForNodes(2, tag_filters=WORKER_FILTER)
        self.provider.finish_starting_nodes()
        self.waitForNodes(
            2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
        )

        for i in self.provider.non_terminated_nodes(WORKER_FILTER):
            runner.assert_not_has_call(f"172.0.0.{i}", "setup_cmd")
            runner.assert_not_has_call(
                f"172.0.0.{i}",
                f"{file_mount_dir}/ ubuntu@172.0.0.{i}:"
                f"{docker_mount_prefix}/home/test-folder/",
            )

        # Simulate a second `ray up` call
        from ray.autoscaler._private import util

        util._hash_cache = {}
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(2)])
        lm = LoadMetrics()
        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )

        autoscaler.update()
        self.waitForNodes(2, tag_filters=WORKER_FILTER)
        self.provider.finish_starting_nodes()
        self.waitForNodes(
            2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, **WORKER_FILTER}
        )
        autoscaler.update()

        for i in self.provider.non_terminated_nodes(WORKER_FILTER):
            runner.assert_has_call(f"172.0.0.{i}", "setup_cmd")
            runner.assert_has_call(
                f"172.0.0.{i}",
                f"{file_mount_dir}/ ubuntu@172.0.0.{i}:"
                f"{docker_mount_prefix}/home/test-folder/",
            )

    def testDockerImageExistsBeforeInspect(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 1
        config["available_node_types"]["worker"]["max_workers"] = 1
        config["max_workers"] = 1
        config["docker"]["pull_before_run"] = False
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(1)])
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        autoscaler = MockAutoscaler(
            config_path,
            LoadMetrics(),
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
        )
        autoscaler.update()
        autoscaler.update()
        self.waitForNodes(1, tag_filters={TAG_RAY_NODE_KIND: NODE_KIND_WORKER})
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(
            1,
            tag_filters={
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
            },
        )
        first_pull = [
            (i, cmd)
            for i, cmd in enumerate(runner.command_history())
            if "docker pull" in cmd
        ]
        first_targeted_inspect = [
            (i, cmd)
            for i, cmd in enumerate(runner.command_history())
            if "docker inspect -f" in cmd
        ]

        # This checks for the bug mentioned #13128 where the image is inspected
        # before the image is present.
        assert min(x[0] for x in first_pull) < min(x[0] for x in first_targeted_inspect)

    def testGetRunningHeadNode(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        self.provider = MockProvider()

        # Node 0 is failed.
        self.provider.create_node(
            {},
            {
                TAG_RAY_CLUSTER_NAME: "default",
                TAG_RAY_NODE_KIND: "head",
                TAG_RAY_NODE_STATUS: "update-failed",
            },
            1,
        )

        # `_allow_uninitialized_state` should return the head node
        # in the `update-failed` state.
        allow_failed = commands._get_running_head_node(
            config,
            "/fake/path",
            override_cluster_name=None,
            create_if_needed=False,
            _provider=self.provider,
            _allow_uninitialized_state=True,
        )

        assert allow_failed == 0

        # Node 1 is okay.
        self.provider.create_node(
            {},
            {
                TAG_RAY_CLUSTER_NAME: "default",
                TAG_RAY_NODE_KIND: "head",
                TAG_RAY_NODE_STATUS: "up-to-date",
            },
            1,
        )

        node = commands._get_running_head_node(
            config,
            "/fake/path",
            override_cluster_name=None,
            create_if_needed=False,
            _provider=self.provider,
        )

        assert node == 1

        # `_allow_uninitialized_state` should return the up-to-date head node
        # if it is present.
        optionally_failed = commands._get_running_head_node(
            config,
            "/fake/path",
            override_cluster_name=None,
            create_if_needed=False,
            _provider=self.provider,
            _allow_uninitialized_state=True,
        )

        assert optionally_failed == 1

    def testNodeTerminatedDuringUpdate(self):
        """
        Tests autoscaler handling a node getting terminated during an update
        triggered by the node missing a heartbeat.

        Extension of testRecoverUnhealthyWorkers.

        In this test, two nodes miss a heartbeat.
        One of them (node 0) is terminated during its recovery update.
        The other (node 1) just fails its update.

        When processing completed updates, the autoscaler terminates node 1
        but does not try to terminate node 0 again.
        """
        cluster_config = copy.deepcopy(MOCK_DEFAULT_CONFIG)
        cluster_config["available_node_types"]["ray.worker.default"]["min_workers"] = 2
        cluster_config["worker_start_ray_commands"] = ["ray_start_cmd"]

        # Don't need the extra node type or a docker config.
        cluster_config["head_node_type"] = ["ray.worker.default"]
        del cluster_config["available_node_types"]["ray.head.default"]
        del cluster_config["docker"]

        config_path = self.write_config(cluster_config)

        self.provider = MockProvider()
        runner = MockProcessRunner()
        lm = LoadMetrics()
        mock_metrics = Mock(spec=AutoscalerPrometheusMetrics())
        autoscaler = MockAutoscaler(
            config_path,
            lm,
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
            prom_metrics=mock_metrics,
        )

        # Scale up to two up-to-date workers
        autoscaler.update()
        self.waitForNodes(2)
        self.provider.finish_starting_nodes()
        autoscaler.update()
        self.waitForNodes(2, tag_filters={TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})

        # Mark both nodes as unhealthy
        for _ in range(5):
            if autoscaler.updaters:
                time.sleep(0.05)
                autoscaler.update()

        lm.last_heartbeat_time_by_ip["172.0.0.0"] = 0
        lm.last_heartbeat_time_by_ip["172.0.0.1"] = 0

        # Expect both updates to be successful, no nodes in updating state
        assert mock_metrics.successful_updates.inc.call_count == 2
        assert mock_metrics.worker_update_time.observe.call_count == 2
        mock_metrics.updating_nodes.set.assert_called_with(0)
        assert not autoscaler.updaters

        # Set up process runner to terminate worker 0 during missed heartbeat
        # recovery and also cause the updater to fail.
        def terminate_worker_zero():
            self.provider.terminate_node(0)

        autoscaler.process_runner = MockProcessRunner(
            fail_cmds=["ray_start_cmd"],
            cmd_to_callback={"ray_start_cmd": terminate_worker_zero},
        )
        # ensures that no updates are completed until after the next call
        # to update()
        autoscaler.process_runner.ready_to_run.clear()
        num_calls = len(autoscaler.process_runner.calls)
        autoscaler.update()
        mock_metrics.updating_nodes.set.assert_called_with(2)
        mock_metrics.recovering_nodes.set.assert_called_with(2)
        autoscaler.process_runner.ready_to_run.set()
        # Wait for updaters spawned by last autoscaler update to finish.
        self.waitForUpdatersToFinish(autoscaler)
        # Check that updaters processed some commands in the last autoscaler
        # update.
        assert (
            len(autoscaler.process_runner.calls) > num_calls
        ), "Did not get additional process runner calls on last autoscaler update."
        # Missed heartbeat triggered recovery for both nodes.
        events = autoscaler.event_summarizer.summary()
        assert (
            "Restarting 2 nodes of type "
            "ray.worker.default (lost contact with raylet)." in events
        ), events
        # Node 0 was terminated during the last update.
        # Node 1's updater failed, but node 1 won't be terminated until the
        # next autoscaler update.
        assert (
            0 not in NonTerminatedNodes(self.provider).worker_ids
        ), "Node zero still non-terminated."
        assert not self.provider.is_terminated(1), "Node one terminated prematurely."

        fill_in_raylet_ids(self.provider, lm)
        autoscaler.update()
        # Failed updates processed are now processed.
        assert (
            autoscaler.num_failed_updates[0] == 1
        ), "Node zero update failure not registered"
        assert (
            autoscaler.num_failed_updates[1] == 1
        ), "Node one update failure not registered"
        assert mock_metrics.failed_updates.inc.call_count == 2
        assert mock_metrics.failed_recoveries.inc.call_count == 2
        assert mock_metrics.successful_recoveries.inc.call_count == 0
        # Completed-update-processing logic should have terminated node 1.
        assert self.provider.is_terminated(1), "Node 1 not terminated on time."

        events = autoscaler.event_summarizer.summary()
        # Just one node (node_id 1) terminated in the last update.
        # Validates that we didn't try to double-terminate node 0.
        assert (
            "Removing 1 nodes of type ray.worker.default (launch failed)." in events
        ), events
        # To be more explicit,
        assert (
            "Removing 2 nodes of type "
            "ray.worker.default (launch failed)." not in events
        ), events

        # Should get two new nodes after the next update.
        fill_in_raylet_ids(self.provider, lm)
        autoscaler.update()
        self.waitForNodes(2)
        assert set(NonTerminatedNodes(self.provider).worker_ids) == {
            2,
            3,
        }, "Unexpected node_ids"

        assert mock_metrics.stopped_nodes.inc.call_count == 1
        assert mock_metrics.drain_node_exceptions.inc.call_count == 0

    def testProviderException(self):
        config = copy.deepcopy(SMALL_CLUSTER)
        config["available_node_types"]["worker"]["min_workers"] = 2
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        mock_metrics = Mock(spec=AutoscalerPrometheusMetrics())
        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_USER_NODE_TYPE: "head",
            },
            1,
        )
        self.provider.error_creates = Exception(":(")
        autoscaler = MockAutoscaler(
            config_path,
            LoadMetrics(),
            MockNodeInfoStub(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0,
            prom_metrics=mock_metrics,
        )
        autoscaler.update()

        def metrics_incremented():
            exceptions = mock_metrics.node_launch_exceptions.inc.call_count == 1
            create_failures = mock_metrics.failed_create_nodes.inc.call_count == 1
            create_arg = False
            if create_failures:
                # number of failed creations should be incremented by 2
                create_arg = mock_metrics.failed_create_nodes.inc.call_args[0] == (2,)
            return exceptions and create_failures and create_arg

        self.waitFor(metrics_incremented, fail_msg="Expected metrics to update")
        assert mock_metrics.drain_node_exceptions.inc.call_count == 0

    def testDefaultMinMaxWorkers(self):
        config = copy.deepcopy(MOCK_DEFAULT_CONFIG)
        config = prepare_config(config)
        node_types = config["available_node_types"]
        head_node_config = node_types["ray.head.default"]
        assert head_node_config["min_workers"] == 0
        assert head_node_config["max_workers"] == 0

    def testAutoscalerInitFailure(self):
        """Validates error handling for failed autoscaler initialization in the
        Monitor.
        """

        class AutoscalerInitFailException(Exception):
            pass

        class FaultyAutoscaler:
            def __init__(self, *args, **kwargs):
                raise AutoscalerInitFailException

        with patch("ray._private.utils.publish_error_to_driver") as mock_publish:
            with patch.multiple(
                "ray.autoscaler._private.monitor",
                StandardAutoscaler=FaultyAutoscaler,
                _internal_kv_initialized=Mock(return_value=False),
            ):
                monitor = Monitor(
                    address="Here", autoscaling_config="", log_dir=self.tmpdir
                )
                with pytest.raises(AutoscalerInitFailException):
                    monitor.run()
                mock_publish.assert_called_once()

    def testInitializeSDKArguments(self):
        # https://github.com/ray-project/ray/issues/23166
        from ray.autoscaler.sdk import request_resources

        with self.assertRaises(TypeError):
            request_resources(num_cpus="bar")
        with self.assertRaises(TypeError):
            request_resources(bundles="bar")
        with self.assertRaises(TypeError):
            request_resources(bundles=["foo"])
        with self.assertRaises(TypeError):
            request_resources(bundles=[{"foo": "bar"}])
        with self.assertRaises(TypeError):
            request_resources(bundles=[{"foo": 1}, {"bar": "baz"}])

    def test_autoscaler_status_log(self):
        self._test_autoscaler_status_log(status_log_enabled_env=1)
        self._test_autoscaler_status_log(status_log_enabled_env=0)

    def _test_autoscaler_status_log(self, status_log_enabled_env: int):
        mock_logger = Mock(spec=logging.Logger(""))
        with patch.multiple(
            "ray.autoscaler._private.autoscaler",
            logger=mock_logger,
            AUTOSCALER_STATUS_LOG=status_log_enabled_env,
        ):
            config = copy.deepcopy(SMALL_CLUSTER)
            config_path = self.write_config(config)
            runner = MockProcessRunner()
            mock_metrics = Mock(spec=AutoscalerPrometheusMetrics())
            autoscaler = MockAutoscaler(
                config_path,
                LoadMetrics(),
                MockNodeInfoStub(),
                max_failures=0,
                process_runner=runner,
                update_interval_s=0,
                prom_metrics=mock_metrics,
            )
            autoscaler.update()
            status_log_found = False
            for call in mock_logger.info.call_args_list:
                args, _ = call
                arg = args[0]
                if " Autoscaler status: " in arg:
                    status_log_found = True
                    break
            assert status_log_found is bool(status_log_enabled_env)


def test_import():
    """This test ensures that all the autoscaler imports work as expected to
    prevent errors such as #19840.
    """
    import ray  # noqa

    ray.autoscaler.sdk.request_resources  # noqa
    import ray.autoscaler  # noqa
    import ray.autoscaler.sdk  # noqa
    from ray.autoscaler.sdk import request_resources  # noqa


def test_prom_null_metric_inc_fix():
    """Verify the bug fix https://github.com/ray-project/ray/pull/27532
    for NullMetric's signature.
    Check that NullMetric can be called with or without an argument.
    """
    NullMetric().inc()
    NullMetric().inc(5)



if __name__ == "__main__":

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
