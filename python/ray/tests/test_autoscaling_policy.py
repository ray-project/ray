import copy
import logging
import yaml
import tempfile
from typing import Dict, Callable
import shutil
from queue import PriorityQueue
import unittest

import ray
from ray.tests.test_autoscaler import MockProvider, MockProcessRunner
from ray.tests.test_resource_demand_scheduler import MULTI_WORKER_CLUSTER
from ray.autoscaler._private.providers import (
    _NODE_PROVIDERS,
    _clear_provider_cache,
)
from ray.autoscaler._private.autoscaler import StandardAutoscaler
from ray.autoscaler._private.load_metrics import LoadMetrics
from ray.autoscaler._private.node_launcher import NodeLauncher
from ray.autoscaler.tags import TAG_RAY_USER_NODE_TYPE, TAG_RAY_NODE_KIND
from ray.autoscaler._private.constants import AUTOSCALER_UPDATE_INTERVAL_S
from ray.autoscaler._private.cli_logger import cli_logger


class Task:
    def __init__(
            self,
            duration: float,
            resources: Dict[str, float],
            start_callback: Callable[[None], None] = None,
            done_callback: Callable[[None], None] = None,
            submission_time: float = None,
    ):
        self.duration = duration
        self.resources = resources
        self.start_callback = start_callback
        self.done_callback = done_callback
        self.start_time = None
        self.end_time = None
        self.node = None


class Actor(Task):
    pass


class Node:
    def __init__(self, resources, in_cluster):
        self.total_resources = copy.deepcopy(resources)
        self.available_resources = copy.deepcopy(resources)
        self.in_cluster = in_cluster

    def bundle_fits(self, bundle):
        if not self.in_cluster:
            return False
        for resource, quantity in bundle.items():
            if self.available_resources.get(resource, -1) < quantity:
                return False
        return True

    def feasible(self, bundle):
        if not self.in_cluster:
            return False
        for resource, quantity in bundle.items():
            if self.total_resources.get(resource, -1) < quantity:
                return False
        return True

    def allocate(self, bundle):
        assert self.bundle_fits(bundle) and self.in_cluster
        for resource, quantity in bundle.items():
            self.available_resources[resource] -= quantity

    def free(self, bundle):
        for resource, quantity in bundle.items():
            self.available_resources[resource] += quantity
        assert self.feasible(self.available_resources)


class Event:
    def __init__(self, time, event_type, data=None):
        self.time = time
        self.event_type = event_type
        self.data = data

    def __lt__(self, other):
        return self.time < other.time

    def __eq__(self, other):

        return self.time == other.time


SIMULATOR_EVENT_AUTOSCALER_UPDATE = 0
SIMULATOR_EVENT_TASK_DONE = 1
SIMULATOR_EVEN_NODE_JOINED = 2


class Simulator:
    def __init__(
            self,
            config_path,
            provider,
            autoscaler_update_interval_s=AUTOSCALER_UPDATE_INTERVAL_S,
            node_startup_delay_s=120,
    ):
        self.config_path = config_path
        self.provider = provider
        self.autoscaler_update_interval_s = autoscaler_update_interval_s
        self.node_startup_delay_s = node_startup_delay_s

        self._setup_autoscaler()
        self._setup_simulator()

    def _setup_autoscaler(self):
        self.runner = MockProcessRunner()
        self.config = yaml.safe_load(open(self.config_path).read())

        self.provider.create_node(
            {},
            {
                TAG_RAY_NODE_KIND: "head",
                TAG_RAY_USER_NODE_TYPE: self.config["head_node_type"],
            },
            1,
        )
        self.head_ip = self.provider.non_terminated_node_ips({})[0]

        self.load_metrics = LoadMetrics(local_ip=self.head_ip)
        self.autoscaler = StandardAutoscaler(
            self.config_path,
            self.load_metrics,
            # Don't let the autoscaler start any node launchers. Instead, we
            # will launch nodes ourself after every update call.
            max_concurrent_launches=0,
            max_failures=0,
            process_runner=self.runner,
            update_interval_s=0,
        )

        # Manually create a node launcher. Note that we won't start it as a
        # separate thread.
        self.node_launcher = NodeLauncher(
            provider=self.autoscaler.provider,
            queue=self.autoscaler.launch_queue,
            index=0,
            pending=self.autoscaler.pending_launches,
            node_types=self.autoscaler.available_node_types,
        )

    def _setup_simulator(self):
        self.virtual_time = 0
        self.ip_to_nodes = {}
        self._update_cluster_state(join_immediately=True)

        self.work_queue = []
        self.event_queue = PriorityQueue()
        self.event_queue.put(Event(0, SIMULATOR_EVENT_AUTOSCALER_UPDATE))

    def _update_cluster_state(self, join_immediately=False):
        nodes = self.provider.non_terminated_nodes(tag_filters={})
        for node_id in nodes:
            ip = self.provider.internal_ip(node_id)
            if ip in self.ip_to_nodes:
                continue
            node_tags = self.provider.node_tags(node_id)
            if TAG_RAY_USER_NODE_TYPE in node_tags:
                node_type = node_tags[TAG_RAY_USER_NODE_TYPE]
                resources = self.config["available_node_types"][node_type].get(
                    "resources", {})
                node = Node(resources, join_immediately)
                self.ip_to_nodes[ip] = node
                if not join_immediately:
                    join_time = self.virtual_time + self.node_startup_delay_s
                    self.event_queue.put(
                        Event(join_time, SIMULATOR_EVEN_NODE_JOINED, node))

    def submit(self, work):
        if isinstance(work, list):
            self.work_queue.extend(work)
        else:
            self.work_queue.append(work)

    def _schedule_task(self, task):
        for ip, node in self.ip_to_nodes.items():
            if node.bundle_fits(task.resources):
                node.allocate(task.resources)
                task.node = node
                task.start_time = self.virtual_time
                end_time = self.virtual_time + task.duration
                self.event_queue.put(
                    Event(end_time, SIMULATOR_EVENT_TASK_DONE, task))
                if task.start_callback:
                    task.start_callback()
                return True

        return False

    def schedule(self):
        # TODO (Alex): Implement a more realistic scheduling algorithm.
        new_work_queue = []
        for work in self.work_queue:
            if isinstance(work, Task):
                scheduled = self._schedule_task(work)

            if scheduled is False:
                new_work_queue.append(work)
        self.work_queue = new_work_queue

    def _launch_nodes(self):
        """Launch all queued nodes. Since this will be run serially after
        `autoscaler.update` there are no race conditions in checking if the
        queue is empty.
        """
        while not self.node_launcher.queue.empty():
            config, count, node_type = self.node_launcher.queue.get()
            try:
                self.node_launcher._launch_node(config, count, node_type)
            except Exception:
                pass
            finally:
                self.node_launcher.pending.dec(node_type, count)

    def _infeasible(self, bundle):
        for node in self.ip_to_nodes.values():
            if node.feasible(bundle):
                return False
        return True

    def run_autoscaler(self):

        waiting_bundles = []
        infeasible_bundles = []
        for work in self.work_queue:
            if isinstance(work, Task):
                shape = work.resources
                if self._infeasible(shape):
                    infeasible_bundles.append(shape)
                else:
                    waiting_bundles.append(shape)

        for ip, node in self.ip_to_nodes.items():
            if not node.in_cluster:
                continue
            self.load_metrics.update(
                ip=ip,
                static_resources=node.total_resources,
                update_dynamic_resources=True,
                dynamic_resources=node.available_resources,
                update_resource_load=False,
                resource_load={},
                waiting_bundles=waiting_bundles,
                infeasible_bundles=infeasible_bundles,
                pending_placement_groups=[],
            )

        self.autoscaler.update()
        self._launch_nodes()
        self._update_cluster_state()

    def process_event(self, event):
        if event.event_type == SIMULATOR_EVENT_AUTOSCALER_UPDATE:
            self.run_autoscaler()
            next_update = self.virtual_time + self.autoscaler_update_interval_s
            self.event_queue.put(
                Event(next_update, SIMULATOR_EVENT_AUTOSCALER_UPDATE))
        elif event.event_type == SIMULATOR_EVENT_TASK_DONE:
            task = event.data
            task.node.free(task.resources)
            if task.done_callback:
                task.done_callback()
        elif event.event_type == SIMULATOR_EVEN_NODE_JOINED:
            node = event.data
            node.in_cluster = True

    def step(self):
        self.virtual_time = self.event_queue.queue[0].time
        while self.event_queue.queue[0].time == self.virtual_time:
            event = self.event_queue.get()
            self.process_event(event)
        self.schedule()
        print(self.info_string())
        return self.virtual_time

    def info_string(self):
        return (f"[t={self.virtual_time}] Nodes: {len(self.ip_to_nodes)} " +
                f"Remaining requests: {len(self.work_queue)} ")


SAMPLE_CLUSTER_CONFIG = copy.deepcopy(MULTI_WORKER_CLUSTER)
SAMPLE_CLUSTER_CONFIG["min_workers"] = 0
SAMPLE_CLUSTER_CONFIG["max_workers"] = 9999
SAMPLE_CLUSTER_CONFIG["target_utilization_fraction"] = 1.0
SAMPLE_CLUSTER_CONFIG["available_node_types"]["m4.16xlarge"][
    "max_workers"] = 100
SAMPLE_CLUSTER_CONFIG["available_node_types"]["m4.4xlarge"][
    "max_workers"] = 10000


class AutoscalingPolicyTest(unittest.TestCase):
    def setUp(self):
        _NODE_PROVIDERS["mock"] = lambda config: self.create_provider
        self.provider = None
        self.tmpdir = tempfile.mkdtemp()
        logging.disable(level=logging.CRITICAL)

        # This seems to be the only way of turning the cli logger off. The
        # expected methods like `cli_logger.configure` don't work.
        def do_nothing(*args, **kwargs):
            pass

        cli_logger._print = type(cli_logger._print)(do_nothing,
                                                    type(cli_logger))

    def tearDown(self):
        self.provider = None
        del _NODE_PROVIDERS["mock"]
        _clear_provider_cache()
        shutil.rmtree(self.tmpdir)
        ray.shutdown()

    def create_provider(self, config, cluster_name):
        assert self.provider
        return self.provider

    def write_config(self, config):
        path = self.tmpdir + "/simple.yaml"
        with open(path, "w") as f:
            f.write(yaml.dump(config))
        return path

    def testManyTasks(self):
        cli_logger.configure(log_style="record", verbosity=-1)
        config = copy.deepcopy(SAMPLE_CLUSTER_CONFIG)
        config_path = self.write_config(config)
        self.provider = MockProvider()
        simulator = Simulator(config_path, self.provider)

        done_count = 0

        def done_callback():
            nonlocal done_count
            done_count += 1

        tasks = [
            Task(
                duration=200,
                resources={"CPU": 1},
                done_callback=done_callback) for _ in range(5000)
        ]
        simulator.submit(tasks)

        time = 0
        while done_count < len(tasks):
            time = simulator.step()

        assert time < 400

    def testManyActors(self):
        # cli_logger.configure(log_style="record", verbosity=-1)
        config = copy.deepcopy(SAMPLE_CLUSTER_CONFIG)
        config_path = self.write_config(config)
        self.provider = MockProvider()
        simulator = Simulator(config_path, self.provider)

        start_count = 0

        def start_callback():
            nonlocal start_count
            start_count += 1

        tasks = [
            Actor(
                duration=float("inf"),
                resources={"CPU": 1},
                start_callback=start_callback,
            ) for _ in range(5000)
        ]
        simulator.submit(tasks)

        time = 0
        while start_count < len(tasks):
            time = simulator.step()

        assert time < 200
