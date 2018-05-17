from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import hashlib
import os
import subprocess
import time
import traceback

from collections import defaultdict
from datetime import datetime

import numpy as np
import yaml

from ray.ray_constants import AUTOSCALER_MAX_NUM_FAILURES, \
    AUTOSCALER_MAX_CONCURRENT_LAUNCHES, AUTOSCALER_UPDATE_INTERVAL_S, \
    AUTOSCALER_HEARTBEAT_TIMEOUT_S
from ray.autoscaler.node_provider import get_node_provider, \
    get_default_config
from ray.autoscaler.updater import NodeUpdaterProcess
from ray.autoscaler.docker import dockerize_if_needed
from ray.autoscaler.tags import TAG_RAY_LAUNCH_CONFIG, \
    TAG_RAY_RUNTIME_CONFIG, TAG_RAY_NODE_STATUS, TAG_RAY_NODE_TYPE, TAG_NAME
import ray.services as services

REQUIRED, OPTIONAL = True, False

# For (a, b), if a is a dictionary object, then
# no extra fields can be introduced.
CLUSTER_CONFIG_SCHEMA = {
    # An unique identifier for the head node and workers of this cluster.
    "cluster_name": (str, REQUIRED),

    # The minimum number of workers nodes to launch in addition to the head
    # node. This number should be >= 0.
    "min_workers": (int, OPTIONAL),

    # The maximum number of workers nodes to launch in addition to the head
    # node. This takes precedence over min_workers.
    "max_workers": (int, REQUIRED),

    # The autoscaler will scale up the cluster to this target fraction of
    # resources usage. For example, if a cluster of 8 nodes is 100% busy
    # and target_utilization was 0.8, it would resize the cluster to 10.
    "target_utilization_fraction": (float, OPTIONAL),

    # If a node is idle for this many minutes, it will be removed.
    "idle_timeout_minutes": (int, OPTIONAL),

    # Cloud-provider specific configuration.
    "provider": (
        {
            "type": (str, REQUIRED),  # e.g. aws
            "region": (str, OPTIONAL),  # e.g. us-east-1
            "availability_zone": (str, OPTIONAL),  # e.g. us-east-1a
            "module": (str,
                       OPTIONAL),  # module, if using external node provider
        },
        REQUIRED),

    # How Ray will authenticate with newly launched nodes.
    "auth": (
        {
            "ssh_user": (str, REQUIRED),  # e.g. ubuntu
            "ssh_private_key": (str, OPTIONAL),
        },
        REQUIRED),

    # Docker configuration. If this is specified, all setup and start commands
    # will be executed in the container.
    "docker": (
        {
            "image": (str, OPTIONAL),  # e.g. tensorflow/tensorflow:1.5.0-py3
            "container_name": (str, OPTIONAL),  # e.g., ray_docker
        },
        OPTIONAL),

    # Provider-specific config for the head node, e.g. instance type.
    "head_node": (dict, OPTIONAL),

    # Provider-specific config for worker nodes. e.g. instance type.
    "worker_nodes": (dict, OPTIONAL),

    # Map of remote paths to local paths, e.g. {"/tmp/data": "/my/local/data"}
    "file_mounts": (dict, OPTIONAL),

    # List of common shell commands to run to initialize nodes.
    "setup_commands": (list, OPTIONAL),

    # Commands that will be run on the head node after common setup.
    "head_setup_commands": (list, OPTIONAL),

    # Commands that will be run on worker nodes after common setup.
    "worker_setup_commands": (list, OPTIONAL),

    # Command to start ray on the head node. You shouldn't need to modify this.
    "head_start_ray_commands": (list, OPTIONAL),

    # Command to start ray on worker nodes. You shouldn't need to modify this.
    "worker_start_ray_commands": (list, OPTIONAL),

    # Whether to avoid restarting the cluster during updates. This field is
    # controlled by the ray --no-restart flag and cannot be set by the user.
    "no_restart": (None, OPTIONAL),
}


class LoadMetrics(object):
    """Container for cluster load metrics.

    Metrics here are updated from local scheduler heartbeats. The autoscaler
    queries these metrics to determine when to scale up, and which nodes
    can be removed.
    """

    def __init__(self):
        self.last_used_time_by_ip = {}
        self.last_heartbeat_time_by_ip = {}
        self.static_resources_by_ip = {}
        self.dynamic_resources_by_ip = {}
        self.local_ip = services.get_node_ip_address()

    def update(self, ip, static_resources, dynamic_resources):
        self.static_resources_by_ip[ip] = static_resources
        self.dynamic_resources_by_ip[ip] = dynamic_resources
        now = time.time()
        if ip not in self.last_used_time_by_ip or \
                static_resources != dynamic_resources:
            self.last_used_time_by_ip[ip] = now
        self.last_heartbeat_time_by_ip[ip] = now

    def mark_active(self, ip):
        self.last_heartbeat_time_by_ip[ip] = time.time()

    def prune_active_ips(self, active_ips):
        active_ips = set(active_ips)
        active_ips.add(self.local_ip)

        def prune(mapping):
            unwanted = set(mapping) - active_ips
            for unwanted_key in unwanted:
                print("Removed mapping", unwanted_key, mapping[unwanted_key])
                del mapping[unwanted_key]
            if unwanted:
                print("Removed {} stale ip mappings: {} not in {}".format(
                    len(unwanted), unwanted, active_ips))

        prune(self.last_used_time_by_ip)
        prune(self.static_resources_by_ip)
        prune(self.dynamic_resources_by_ip)

    def approx_workers_used(self):
        return self._info()["NumNodesUsed"]

    def debug_string(self):
        return " - {}".format("\n - ".join(
            ["{}: {}".format(k, v) for k, v in sorted(self._info().items())]))

    def _info(self):
        nodes_used = 0.0
        resources_used = {}
        resources_total = {}
        now = time.time()
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
        idle_times = [now - t for t in self.last_used_time_by_ip.values()]
        return {
            "ResourceUsage":
            ", ".join([
                "{}/{} {}".format(
                    round(resources_used[rid], 2),
                    round(resources_total[rid], 2), rid)
                for rid in sorted(resources_used)
            ]),
            "NumNodesConnected":
            len(self.static_resources_by_ip),
            "NumNodesUsed":
            round(nodes_used, 2),
            "NodeIdleSeconds":
            "Min={} Mean={} Max={}".format(
                int(np.min(idle_times)) if idle_times else -1,
                int(np.mean(idle_times)) if idle_times else -1,
                int(np.max(idle_times)) if idle_times else -1),
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

    def __init__(self,
                 config_path,
                 load_metrics,
                 max_concurrent_launches=AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
                 max_failures=AUTOSCALER_MAX_NUM_FAILURES,
                 process_runner=subprocess,
                 verbose_updates=True,
                 node_updater_cls=NodeUpdaterProcess,
                 update_interval_s=AUTOSCALER_UPDATE_INTERVAL_S):
        self.config_path = config_path
        self.reload_config(errors_fatal=True)
        self.load_metrics = load_metrics
        self.provider = get_node_provider(self.config["provider"],
                                          self.config["cluster_name"])

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
        self.update_interval_s = update_interval_s

        for local_path in self.config["file_mounts"].values():
            assert os.path.exists(local_path)

        print("StandardAutoscaler: {}".format(self.config))

    def update(self):
        try:
            self.reload_config(errors_fatal=False)
            self._update()
        except Exception as e:
            print("StandardAutoscaler: Error during autoscaling: {}",
                  traceback.format_exc())
            self.num_failures += 1
            if self.num_failures > self.max_failures:
                print("*** StandardAutoscaler: Too many errors, abort. ***")
                raise e

    def _update(self):
        # Throttle autoscaling updates to this interval to avoid exceeding
        # rate limits on API calls.
        if time.time() - self.last_update_time < self.update_interval_s:
            return

        self.last_update_time = time.time()
        nodes = self.workers()
        print(self.debug_string(nodes))
        self.load_metrics.prune_active_ips(
            [self.provider.internal_ip(node_id) for node_id in nodes])

        # Terminate any idle or out of date nodes
        last_used = self.load_metrics.last_used_time_by_ip
        horizon = time.time() - (60 * self.config["idle_timeout_minutes"])
        num_terminated = 0
        for node_id in nodes:
            node_ip = self.provider.internal_ip(node_id)
            if node_ip in last_used and last_used[node_ip] < horizon and \
                    len(nodes) - num_terminated > self.config["min_workers"]:
                num_terminated += 1
                print("StandardAutoscaler: Terminating idle node: "
                      "{}".format(node_id))
                self.provider.terminate_node(node_id)
            elif not self.launch_config_ok(node_id):
                num_terminated += 1
                print("StandardAutoscaler: Terminating outdated node: "
                      "{}".format(node_id))
                self.provider.terminate_node(node_id)
        if num_terminated > 0:
            nodes = self.workers()
            print(self.debug_string(nodes))

        # Terminate nodes if there are too many
        num_terminated = 0
        while len(nodes) > self.config["max_workers"]:
            num_terminated += 1
            print("StandardAutoscaler: Terminating unneeded node: "
                  "{}".format(nodes[-1]))
            self.provider.terminate_node(nodes[-1])
            nodes = nodes[:-1]
        if num_terminated > 0:
            nodes = self.workers()
            print(self.debug_string(nodes))

        # Launch new nodes if needed
        target_num = self.target_num_workers()
        if len(nodes) < target_num:
            self.launch_new_node(
                min(self.max_concurrent_launches, target_num - len(nodes)))
            print(self.debug_string())

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
            # Mark the node as active to prevent the node recovery logic
            # immediately trying to restart Ray on the new node.
            self.load_metrics.mark_active(self.provider.internal_ip(node_id))
            nodes = self.workers()
            print(self.debug_string(nodes))

        # Update nodes with out-of-date files
        for node_id in nodes:
            self.update_if_needed(node_id)

        # Attempt to recover unhealthy nodes
        for node_id in nodes:
            self.recover_if_needed(node_id)

    def reload_config(self, errors_fatal=False):
        try:
            with open(self.config_path) as f:
                new_config = yaml.load(f.read())
            validate_config(new_config)
            new_launch_hash = hash_launch_conf(new_config["worker_nodes"],
                                               new_config["auth"])
            new_runtime_hash = hash_runtime_conf(new_config["file_mounts"], [
                new_config["setup_commands"],
                new_config["worker_setup_commands"],
                new_config["worker_start_ray_commands"]
            ])
            self.config = new_config
            self.launch_hash = new_launch_hash
            self.runtime_hash = new_runtime_hash
        except Exception as e:
            if errors_fatal:
                raise e
            else:
                print("StandardAutoscaler: Error parsing config: {}",
                      traceback.format_exc())

    def target_num_workers(self):
        target_frac = self.config["target_utilization_fraction"]
        cur_used = self.load_metrics.approx_workers_used()
        ideal_num_workers = int(np.ceil(cur_used / float(target_frac)))
        return min(self.config["max_workers"],
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

    def recover_if_needed(self, node_id):
        if not self.can_update(node_id):
            return
        last_heartbeat_time = self.load_metrics.last_heartbeat_time_by_ip.get(
            self.provider.internal_ip(node_id), 0)
        if time.time() - last_heartbeat_time < AUTOSCALER_HEARTBEAT_TIMEOUT_S:
            return
        print("StandardAutoscaler: Restarting Ray on {}".format(node_id))
        updater = self.node_updater_cls(
            node_id,
            self.config["provider"],
            self.config["auth"],
            self.config["cluster_name"], {},
            with_head_node_ip(self.config["worker_start_ray_commands"]),
            self.runtime_hash,
            redirect_output=not self.verbose_updates,
            process_runner=self.process_runner)
        updater.start()
        self.updaters[node_id] = updater

    def update_if_needed(self, node_id):
        if not self.can_update(node_id):
            return
        if self.files_up_to_date(node_id):
            return
        if self.config.get("no_restart", False) and \
                self.num_successful_updates.get(node_id, 0) > 0:
            init_commands = (self.config["setup_commands"] +
                             self.config["worker_setup_commands"])
        else:
            init_commands = (self.config["setup_commands"] +
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

    def can_update(self, node_id):
        if not self.provider.is_running(node_id):
            return False
        if not self.launch_config_ok(node_id):
            return False
        if node_id in self.updaters:
            return False
        if self.num_failed_updates.get(node_id, 0) > 0:  # TODO(ekl) retry?
            return False
        return True

    def launch_new_node(self, count):
        print("StandardAutoscaler: Launching {} new nodes".format(count))
        num_before = len(self.workers())
        self.provider.create_node(
            self.config["worker_nodes"], {
                TAG_NAME: "ray-{}-worker".format(self.config["cluster_name"]),
                TAG_RAY_NODE_TYPE: "Worker",
                TAG_RAY_NODE_STATUS: "Uninitialized",
                TAG_RAY_LAUNCH_CONFIG: self.launch_hash,
            }, count)
        if len(self.workers()) <= num_before:
            print("Warning: Num nodes failed to increase after node creation")

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
        return "StandardAutoscaler [{}]: {}/{} target nodes{}\n{}".format(
            datetime.now(), len(nodes), self.target_num_workers(), suffix,
            self.load_metrics.debug_string())


def typename(v):
    if isinstance(v, type):
        return v.__name__
    else:
        return type(v).__name__


def check_required(config, schema):
    # Check required schema entries
    if type(config) is not dict:
        raise ValueError("Config is not a dictionary")

    for k, (v, kreq) in schema.items():
        if v is None:
            continue  # None means we don't validate the field
        if kreq is REQUIRED:
            if k not in config:
                type_str = typename(v)
                raise ValueError(
                    "Missing required config key `{}` of type {}".format(
                        k, type_str))
            if not isinstance(v, type):
                check_required(config[k], v)


def check_extraneous(config, schema):
    """Make sure all items of config are in schema"""
    if type(config) is not dict:
        raise ValueError("Config {} is not a dictionary".format(config))
    for k in config:
        if k not in schema:
            raise ValueError("Unexpected config key `{}` not in {}".format(
                k, list(schema.keys())))
        v, kreq = schema[k]
        if v is None:
            continue
        elif isinstance(v, type):
            if not isinstance(config[k], v):
                raise ValueError(
                    "Config key `{}` has wrong type {}, expected {}".format(
                        k,
                        type(config[k]).__name__, v.__name__))
        else:
            check_extraneous(config[k], v)


def validate_config(config, schema=CLUSTER_CONFIG_SCHEMA):
    """Required Dicts indicate that no extra fields can be introduced."""
    if type(config) is not dict:
        raise ValueError("Config {} is not a dictionary".format(config))

    check_required(config, schema)
    check_extraneous(config, schema)


def fillout_defaults(config):
    defaults = get_default_config(config["provider"])
    defaults.update(config)
    dockerize_if_needed(defaults)
    return defaults


def with_head_node_ip(cmds):
    head_ip = services.get_node_ip_address()
    out = []
    for cmd in cmds:
        out.append("export RAY_HEAD_IP={}; {}".format(head_ip, cmd))
    return out


def hash_launch_conf(node_conf, auth):
    hasher = hashlib.sha1()
    hasher.update(
        json.dumps([node_conf, auth], sort_keys=True).encode("utf-8"))
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
    hasher.update(json.dumps(extra_objs, sort_keys=True).encode("utf-8"))
    for local_path in sorted(file_mounts.values()):
        add_content_hashes(local_path)

    return hasher.hexdigest()
