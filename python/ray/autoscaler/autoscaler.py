from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import hashlib
import math
import os
import subprocess
import time
import traceback

from collections import defaultdict

import yaml

from ray.autoscaler.node_provider import get_node_provider
from ray.autoscaler.updater import NodeUpdaterProcess
from ray.autoscaler.tags import TAG_RAY_LAUNCH_CONFIG, \
    TAG_RAY_RUNTIME_CONFIG, TAG_RAY_NODE_STATUS, TAG_RAY_NODE_TYPE, TAG_NAME
import ray.services as services


CLUSTER_CONFIG_SCHEMA = {
    # An unique identifier for the head node and workers of this cluster.
    "cluster_name": str,

    # The minimum number of workers nodes to launch in addition to the head
    # node. This number should be >= 0.
    "min_workers": int,

    # The maximum number of workers nodes to launch in addition to the head
    # node. This takes precedence over min_workers.
    "max_workers": int,

    # The autoscaler will scale up the cluster to this target fraction of
    # resources usage. For example, if a cluster of 8 nodes is 100% busy
    # and target_utilization was 0.8, it would resize the cluster to 10.
    "target_utilization_fraction": float,

    # If a node is idle for this many minutes, it will be removed.
    "idle_timeout_minutes": int,

    # Cloud-provider specific configuration.
    "provider": {
        "type": str,  # e.g. aws
        "region": str,  # e.g. us-east-1
    },

    # How Ray will authenticate with newly launched nodes.
    "auth": dict,

    # Provider-specific config for the head node, e.g. instance type.
    "head_node": dict,

    # Provider-specific config for worker nodes. e.g. instance type.
    "worker_nodes": dict,

    # Map of remote paths to local paths, e.g. {"/tmp/data": "/my/local/data"}
    "file_mounts": dict,

    # List of common shell commands to run to initialize nodes.
    "setup_commands": list,

    # Commands that will be run on the head node after common setup.
    "head_setup_commands": list,

    # Commands that will be run on worker nodes after common setup.
    "worker_setup_commands": list,

    # Command to start ray on the head node. You shouldn't need to modify this.
    "head_start_ray_commands": list,

    # Command to start ray on worker nodes. You shouldn't need to modify this.
    "worker_start_ray_commands": list,

    # Whether to avoid restarting the cluster during updates. This field is
    # controlled by the ray --no-restart flag and cannot be set by the user.
    "no_restart": None,
}


# Abort autoscaling if more than this number of errors are encountered. This
# is a safety feature to prevent e.g. runaway node launches.
MAX_NUM_FAILURES = 5

# Max number of nodes to launch at a time.
MAX_CONCURRENT_LAUNCHES = 10

# Interval at which to perform autoscaling updates.
UPDATE_INTERVAL_S = 5


class LoadMetrics(object):
    """Container for cluster load metrics.

    Metrics here are updated from local scheduler heartbeats. The autoscaler
    queries these metrics to determine when to scale up, and which nodes
    can be removed.
    """

    def __init__(self):
        self.last_used_time_by_ip = {}
        self.static_resources_by_ip = {}
        self.dynamic_resources_by_ip = {}
        self.local_ip = services.get_node_ip_address()

    def update(self, ip, static_resources, dynamic_resources):
        self.static_resources_by_ip[ip] = static_resources
        self.dynamic_resources_by_ip[ip] = dynamic_resources
        if ip not in self.last_used_time_by_ip or \
                static_resources != dynamic_resources:
            self.last_used_time_by_ip[ip] = time.time()

    def prune_active_ips(self, active_ips):
        active_ips = set(active_ips)
        active_ips.add(self.local_ip)

        def prune(mapping):
            unwanted = set(mapping) - active_ips
            for unwanted_key in unwanted:
                del mapping[unwanted_key]
            if unwanted:
                print(
                    "Removed {} stale ip mappings: {} not in {}".format(
                        len(unwanted), unwanted, active_ips))
        prune(self.last_used_time_by_ip)
        prune(self.static_resources_by_ip)
        prune(self.dynamic_resources_by_ip)

    def approx_workers_used(self):
        return self._info()["ApproxWorkersUsed"]

    def debug_string(self):
        return "Load metrics: {}".format(
            " ".join(["{}={}".format(k, v) for k, v in self._info().items()]))

    def _info(self):
        nodes_used = 0.0
        resources_used = {}
        resources_total = {}
        now = time.time()
        if self.last_used_time_by_ip:
            max_last_used_time = max(self.last_used_time_by_ip.values())
            min_last_used_time = min(self.last_used_time_by_ip.values())
        else:
            max_last_used_time = now
            min_last_used_time = now
        for ip, max_resources in self.static_resources_by_ip.items():
            avail_resources = self.dynamic_resources_by_ip[ip]
            max_frac = 0.0
            for resource_id, amount in max_resources.items():
                used = amount - avail_resources[resource_id]
                if resource_id not in resources_used:
                    resources_used[resource_id] = 0.0
                    resources_total[resource_id] = 0.0
                resources_used[resource_id] += used
                resources_total[resource_id] += amount
                assert used >= 0
                if amount > 0:
                    frac = used / float(amount)
                    if frac > max_frac:
                        max_frac = frac
            nodes_used += max_frac
        return {
            "Usage": ", ".join([
                "{}/{} {}".format(
                    round(resources_used[rid], 2),
                    round(resources_total[rid], 2), rid)
                for rid in resources_used]),
            "Tracked": len(self.static_resources_by_ip),
            "ApproxWorkersUsed": round(nodes_used, 2),
            "MinIdleSeconds": int(now - max_last_used_time),
            "MaxIdleSeconds": int(now - min_last_used_time),
        }


class StandardAutoscaler(object):
    """The autoscaling control loop for a Ray cluster.

    There are two ways to start an autoscaling cluster: manually by running
    `ray start --head --autoscaling-config=/path/to/config.yaml` on a
    instance that has permission to launch other instances, or you can also use
    `ray create_or_update /path/to/config.yaml` from your laptop, which will
    configure the right AWS/Cloud roles automatically.

    StandardAutoscaler's `update` method is periodically called by `monitor.py`
    to add and remove nodes as necessary. Currently, load-based autoscaling is
    not implemented, so all this class does is try to maintain a constant
    cluster size.

    StandardAutoscaler is also used to bootstrap clusters (by adding workers
    until the target cluster size is met).
    """

    def __init__(
            self, config_path, load_metrics,
            max_concurrent_launches=MAX_CONCURRENT_LAUNCHES,
            max_failures=MAX_NUM_FAILURES, process_runner=subprocess,
            verbose_updates=False, node_updater_cls=NodeUpdaterProcess):
        self.config_path = config_path
        self.reload_config(errors_fatal=True)
        self.load_metrics = load_metrics
        self.provider = get_node_provider(
            self.config["provider"], self.config["cluster_name"])

        self.max_failures = max_failures
        self.max_concurrent_launches = max_concurrent_launches
        self.verbose_updates = verbose_updates
        self.process_runner = process_runner
        self.node_updater_cls = node_updater_cls

        # Map from node_id to NodeUpdater processes
        self.updaters = {}
        self.num_failed_updates = defaultdict(int)
        self.num_successful_updates = defaultdict(int)
        self.num_failures = 0
        self.last_update_time = 0.0

        for local_path in self.config["file_mounts"].values():
            assert os.path.exists(local_path)

        print("StandardAutoscaler: {}".format(self.config))

    def update(self):
        print(time.time() - self.last_update_time)
        try:
            self.reload_config(errors_fatal=False)
            self._update()
        except Exception as e:
            print(
                "StandardAutoscaler: Error during autoscaling: {}",
                traceback.format_exc())
            self.num_failures += 1
            if self.num_failures > self.max_failures:
                print("*** StandardAutoscaler: Too many errors, abort. ***")
                raise e

    def _update(self):
        # Throttle autoscaling updates to this interval to avoid exceeding
        # rate limits on API calls.
        if time.time() - self.last_update_time < UPDATE_INTERVAL_S:
            return

        self.last_update_time = time.time()
        nodes = self.workers()
        print(self.debug_string(nodes))
        max_workers = self.config["max_workers"]
        self.load_metrics.prune_active_ips(
            [self.provider.internal_ip(node_id) for node_id in nodes])

        # Terminate any idle or out of date nodes
        last_used = self.load_metrics.last_used_time_by_ip
        horizon = time.time() - (60 * self.config["idle_timeout_minutes"])
        terminated = False
        for node_id in nodes:
            node_ip = self.provider.internal_ip(node_id)
            if node_ip in last_used and last_used[node_ip] < horizon:
                terminated = True
                print(
                    "StandardAutoscaler: Terminating idle node: "
                    "{}".format(node_id))
                self.provider.terminate_node(node_id)
            elif not self.launch_config_ok(node_id):
                terminated = True
                print(
                    "StandardAutoscaler: Terminating outdated node: "
                    "{}".format(node_id))
                self.provider.terminate_node(node_id)
        if terminated:
            nodes = self.workers()
            print(self.debug_string(nodes))

        # Terminate nodes if there are too many
        terminated = False
        while len(nodes) > max_workers:
            terminated = True
            print(
                "StandardAutoscaler: Terminating unneeded node: "
                "{}".format(nodes[-1]))
            self.provider.terminate_node(nodes[-1])
        if terminated:
            nodes = self.workers()
            print(self.debug_string(nodes))

        # Launch new nodes if needed
        target_num = self.target_num_workers()
        if len(nodes) < target_num:
            self.launch_new_node(
                min(self.max_concurrent_launches, target_num - len(nodes)))
            print(self.debug_string())

        # Update nodes with out-of-date files
        for node_id in nodes:
            self.update_if_needed(node_id)

        # Process any completed updates
        completed = []
        for node_id, updater in self.updaters.items():
            if not updater.is_alive():
                completed.append(node_id)
        if completed:
            for node_id in completed:
                if self.updaters[node_id].exitcode == 0:
                    self.num_successful_updates[node_id] += 1
                else:
                    self.num_failed_updates[node_id] += 1
                del self.updaters[node_id]
            print(self.debug_string())

    def reload_config(self, errors_fatal=False):
        try:
            with open(self.config_path) as f:
                new_config = yaml.load(f.read())
            validate_config(new_config)
            new_launch_hash = hash_launch_conf(
                new_config["worker_nodes"], new_config["auth"])
            new_runtime_hash = hash_runtime_conf(
                new_config["file_mounts"],
                [new_config["setup_commands"],
                 new_config["worker_setup_commands"],
                 new_config["worker_start_ray_commands"]])
            self.config = new_config
            self.launch_hash = new_launch_hash
            self.runtime_hash = new_runtime_hash
        except Exception as e:
            if errors_fatal:
                raise e
            else:
                print(
                    "StandardAutoscaler: Error parsing config: {}",
                    traceback.format_exc())

    def target_num_workers(self):
        target_frac = self.config["target_utilization_fraction"]
        cur_used = self.load_metrics.approx_workers_used()
        ideal_num_workers = math.ceil(cur_used / float(target_frac))
        return min(
            self.config["max_workers"],
            max(self.config["min_workers"], ideal_num_workers))

    def launch_config_ok(self, node_id):
        launch_conf = self.provider.node_tags(node_id).get(
            TAG_RAY_LAUNCH_CONFIG)
        if self.launch_hash != launch_conf:
            return False
        return True

    def files_up_to_date(self, node_id):
        applied = self.provider.node_tags(node_id).get(TAG_RAY_RUNTIME_CONFIG)
        if applied != self.runtime_hash:
            print(
                "StandardAutoscaler: {} has runtime state {}, want {}".format(
                    node_id, applied, self.runtime_hash))
            return False
        return True

    def update_if_needed(self, node_id):
        if not self.provider.is_running(node_id):
            return
        if not self.launch_config_ok(node_id):
            return
        if node_id in self.updaters:
            return
        if self.num_failed_updates.get(node_id, 0) > 0:  # TODO(ekl) retry?
            return
        if self.files_up_to_date(node_id):
            return
        if self.config.get("no_restart", False) and \
                self.num_succeeded_updates.get(node_id, 0) > 0:
            init_commands = (
                self.config["setup_commands"] +
                self.config["worker_setup_commands"])
        else:
            init_commands = (
                self.config["setup_commands"] +
                self.config["worker_setup_commands"] +
                self.config["worker_start_ray_commands"])
        updater = self.node_updater_cls(
            node_id,
            self.config["provider"],
            self.config["auth"],
            self.config["cluster_name"],
            self.config["file_mounts"],
            with_head_node_ip(init_commands),
            self.runtime_hash,
            redirect_output=not self.verbose_updates,
            process_runner=self.process_runner)
        updater.start()
        self.updaters[node_id] = updater

    def launch_new_node(self, count):
        print("StandardAutoscaler: Launching {} new nodes".format(count))
        num_before = len(self.workers())
        self.provider.create_node(
            self.config["worker_nodes"],
            {
                TAG_NAME: "ray-{}-worker".format(self.config["cluster_name"]),
                TAG_RAY_NODE_TYPE: "Worker",
                TAG_RAY_NODE_STATUS: "Uninitialized",
                TAG_RAY_LAUNCH_CONFIG: self.launch_hash,
            },
            count)
        # TODO(ekl) be less conservative in this check
        assert len(self.workers()) > num_before, \
            "Num nodes failed to increase after creating a new node"

    def workers(self):
        return self.provider.nodes(tag_filters={
            TAG_RAY_NODE_TYPE: "Worker",
        })

    def debug_string(self, nodes=None):
        if nodes is None:
            nodes = self.workers()
        suffix = ""
        if self.updaters:
            suffix += " ({} updating)".format(len(self.updaters))
        if self.num_failed_updates:
            suffix += " ({} failed to update)".format(
                len(self.num_failed_updates))
        return "StandardAutoscaler: Have {} / {} target nodes{}\n{}".format(
                len(nodes), self.target_num_workers(), suffix,
                self.load_metrics.debug_string())


def validate_config(config, schema=CLUSTER_CONFIG_SCHEMA):
    if type(config) is not dict:
        raise ValueError("Config is not a dictionary")
    for k, v in schema.items():
        if v is None:
            continue  # None means we don't validate the field
        if k not in config:
            raise ValueError(
                "Missing required config key `{}` of type {}".format(
                    k, v.__name__))
        if isinstance(v, type):
            if not isinstance(config[k], v):
                raise ValueError(
                    "Config key `{}` has wrong type {}, expected {}".format(
                        k, type(config[k]).__name__, v.__name__))
        else:
            validate_config(config[k], schema[k])
    for k in config.keys():
        if k not in schema:
            raise ValueError(
                "Unexpected config key `{}` not in {}".format(
                    k, schema.keys()))


def with_head_node_ip(cmds):
    head_ip = services.get_node_ip_address()
    out = []
    for cmd in cmds:
        out.append("export RAY_HEAD_IP={}; {}".format(head_ip, cmd))
    return out


def hash_launch_conf(node_conf, auth):
    hasher = hashlib.sha1()
    hasher.update(json.dumps([node_conf, auth]).encode("utf-8"))
    return hasher.hexdigest()


def hash_runtime_conf(file_mounts, extra_objs):
    hasher = hashlib.sha1()

    def add_content_hashes(path):
        if os.path.isdir(path):
            dirs = []
            for dirpath, _, filenames in os.walk(path):
                dirs.append((dirpath, sorted(filenames)))
            for dirpath, filenames in sorted(dirs):
                hasher.update(dirpath.encode("utf-8"))
                for name in filenames:
                    hasher.update(name.encode("utf-8"))
                    with open(os.path.join(dirpath, name), "rb") as f:
                        hasher.update(f.read())
        else:
            with open(path, 'r') as f:
                hasher.update(f.read().encode("utf-8"))

    hasher.update(json.dumps(sorted(file_mounts.items())).encode("utf-8"))
    hasher.update(json.dumps(extra_objs).encode("utf-8"))
    for local_path in sorted(file_mounts.values()):
        add_content_hashes(local_path)

    return hasher.hexdigest()
