import re
import threading
from subprocess import CalledProcessError
from typing import Any, Dict, List, Optional

from ray.autoscaler.node_provider import NodeProvider


class MockNode:
    def __init__(
        self,
        node_id,
        tags,
        node_config,
        node_type,
        unique_ips=False,
        resources=None,
        labels=None,
    ):
        self.node_id = str(node_id)
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
        self.resources = resources or {}
        self.labels = labels or {}

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
        self.creation_error = None
        self.termination_errors = None
        self.fail_creates = False
        self.ready_to_create = threading.Event()
        self.ready_to_create.set()
        self.cache_stopped = cache_stopped
        self.unique_ips = unique_ips
        self.fail_to_fetch_ip = False
        self.safe_to_scale_flag = True
        self.partical_success_count = None
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

    def create_node(
        self,
        node_config: Dict[str, Any],
        tags: Dict[str, str],
        count: int,
        _skip_wait=False,
    ) -> Dict[str, Any]:
        return self.create_node_with_resources_and_labels(
            node_config, tags, count, {}, {}, _skip_wait=_skip_wait
        )

    def create_node_with_resources_and_labels(
        self, node_config, tags, count, resources, labels, _skip_wait=False
    ):
        from ray.autoscaler.tags import TAG_RAY_USER_NODE_TYPE

        if self.creation_error is not None:
            raise self.creation_error
        if not _skip_wait:
            self.ready_to_create.wait()
        if self.fail_creates:
            return

        created_nodes = {}
        if self.partical_success_count is not None:
            count = min(count, self.partical_success_count)
        with self.lock:
            if self.cache_stopped:
                for node in self.mock_nodes.values():
                    if node.state == "stopped" and count > 0:
                        count -= 1
                        node.state = "pending"
                        node.tags.update(tags)
                        created_nodes[node.node_id] = node
            for _ in range(count):
                new_node = MockNode(
                    str(self.next_id),
                    tags.copy(),
                    node_config,
                    tags.get(TAG_RAY_USER_NODE_TYPE),
                    resources=resources,
                    labels=labels,
                    unique_ips=self.unique_ips,
                )
                self.mock_nodes[new_node.node_id] = new_node
                created_nodes[new_node.node_id] = new_node
                self.next_id += 1
        return created_nodes

    def set_node_tags(self, node_id, tags):
        with self.lock:
            self.mock_nodes[node_id].tags.update(tags)

    def terminate_node(self, node_id):
        with self.lock:
            if self.termination_errors is not None:
                raise self.termination_errors

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
