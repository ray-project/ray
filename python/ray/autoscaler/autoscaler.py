from collections import defaultdict
import copy
import json
import logging
import math
import numpy as np
import os
import subprocess
import threading
import time
import yaml

from ray.autoscaler.node_provider import get_node_provider
from ray.autoscaler.tags import (TAG_RAY_LAUNCH_CONFIG, TAG_RAY_RUNTIME_CONFIG,
                                 TAG_RAY_NODE_STATUS, TAG_RAY_NODE_TYPE,
                                 STATUS_UP_TO_DATE, NODE_TYPE_WORKER)
from ray.autoscaler.updater import NodeUpdaterThread
from ray.autoscaler.node_launcher import NodeLauncher
from ray.autoscaler.util import ConcurrentCounter, validate_config, \
    with_head_node_ip, hash_launch_conf, hash_runtime_conf
from ray.ray_constants import AUTOSCALER_MAX_NUM_FAILURES, \
    AUTOSCALER_MAX_LAUNCH_BATCH, AUTOSCALER_MAX_CONCURRENT_LAUNCHES, \
    AUTOSCALER_UPDATE_INTERVAL_S, AUTOSCALER_HEARTBEAT_TIMEOUT_S, \
    AUTOSCALER_RESOURCE_REQUEST_CHANNEL
import ray.services as services
from ray.worker import global_worker
from six.moves import queue

logger = logging.getLogger(__name__)


class StandardAutoscaler:
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
                 max_launch_batch=AUTOSCALER_MAX_LAUNCH_BATCH,
                 max_concurrent_launches=AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
                 max_failures=AUTOSCALER_MAX_NUM_FAILURES,
                 process_runner=subprocess,
                 update_interval_s=AUTOSCALER_UPDATE_INTERVAL_S):
        self.config_path = config_path
        self.reload_config(errors_fatal=True)
        self.load_metrics = load_metrics
        self.provider = get_node_provider(self.config["provider"],
                                          self.config["cluster_name"])

        self.max_failures = max_failures
        self.max_launch_batch = max_launch_batch
        self.max_concurrent_launches = max_concurrent_launches
        self.process_runner = process_runner

        # Map from node_id to NodeUpdater processes
        self.updaters = {}
        self.num_failed_updates = defaultdict(int)
        self.num_successful_updates = defaultdict(int)
        self.num_failures = 0
        self.last_update_time = 0.0
        self.update_interval_s = update_interval_s
        self.bringup = True

        # Node launchers
        self.launch_queue = queue.Queue()
        self.pending_launches = ConcurrentCounter()
        max_batches = math.ceil(
            max_concurrent_launches / float(max_launch_batch))
        for i in range(int(max_batches)):
            node_launcher = NodeLauncher(
                provider=self.provider,
                queue=self.launch_queue,
                index=i,
                pending=self.pending_launches)
            node_launcher.daemon = True
            node_launcher.start()

        # Expand local file_mounts to allow ~ in the paths. This can't be done
        # earlier when the config is written since we might be on different
        # platform and the expansion would result in wrong path.
        self.config["file_mounts"] = {
            remote: os.path.expanduser(local)
            for remote, local in self.config["file_mounts"].items()
        }

        for local_path in self.config["file_mounts"].values():
            assert os.path.exists(local_path)

        self.resource_requests = defaultdict(int)

        logger.info("StandardAutoscaler: {}".format(self.config))

    def update(self):
        try:
            self.reload_config(errors_fatal=False)
            self._update()
        except Exception as e:
            logger.exception("StandardAutoscaler: "
                             "Error during autoscaling.")
            self.num_failures += 1
            if self.num_failures > self.max_failures:
                logger.critical("StandardAutoscaler: "
                                "Too many errors, abort.")
                raise e

    def _update(self):
        now = time.time()

        # Throttle autoscaling updates to this interval to avoid exceeding
        # rate limits on API calls.
        if now - self.last_update_time < self.update_interval_s:
            return

        self.last_update_time = now
        nodes = self.workers()
        self.load_metrics.prune_active_ips(
            [self.provider.internal_ip(node_id) for node_id in nodes])
        target_workers = self.target_num_workers()

        if len(nodes) >= target_workers:
            if "CPU" in self.resource_requests:
                del self.resource_requests["CPU"]

        self.log_info_string(nodes, target_workers)

        # Terminate any idle or out of date nodes
        last_used = self.load_metrics.last_used_time_by_ip
        horizon = now - (60 * self.config["idle_timeout_minutes"])

        nodes_to_terminate = []
        for node_id in nodes:
            node_ip = self.provider.internal_ip(node_id)
            if node_ip in last_used and last_used[node_ip] < horizon and \
                    len(nodes) - len(nodes_to_terminate) > target_workers:
                logger.info("StandardAutoscaler: "
                            "{}: Terminating idle node".format(node_id))
                nodes_to_terminate.append(node_id)
            elif not self.launch_config_ok(node_id):
                logger.info("StandardAutoscaler: "
                            "{}: Terminating outdated node".format(node_id))
                nodes_to_terminate.append(node_id)

        if nodes_to_terminate:
            self.provider.terminate_nodes(nodes_to_terminate)
            nodes = self.workers()
            self.log_info_string(nodes, target_workers)

        # Terminate nodes if there are too many
        nodes_to_terminate = []
        while len(nodes) > self.config["max_workers"]:
            logger.info("StandardAutoscaler: "
                        "{}: Terminating unneeded node".format(nodes[-1]))
            nodes_to_terminate.append(nodes[-1])
            nodes = nodes[:-1]

        if nodes_to_terminate:
            self.provider.terminate_nodes(nodes_to_terminate)
            nodes = self.workers()
            self.log_info_string(nodes, target_workers)

        # Launch additional nodes of the default type, if still needed.
        num_pending = self.pending_launches.value
        num_workers = len(nodes) + num_pending
        if num_workers < target_workers:
            max_allowed = min(self.max_launch_batch,
                              self.max_concurrent_launches - num_pending)

            num_launches = min(max_allowed, target_workers - num_workers)
            self.launch_new_node(num_launches, instance_type=None)
            nodes = self.workers()
            self.log_info_string(nodes, target_workers)
        elif self.load_metrics.num_workers_connected() >= target_workers:
            logger.info("Ending bringup phase")
            self.bringup = False
            self.log_info_string(nodes, target_workers)

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
            self.log_info_string(nodes, target_workers)

        # Update nodes with out-of-date files.
        # TODO(edoakes): Spawning these threads directly seems to cause
        # problems. They should at a minimum be spawned as daemon threads.
        # See https://github.com/ray-project/ray/pull/5903 for more info.
        T = []
        for node_id, commands, ray_start in (self.should_update(node_id)
                                             for node_id in nodes):
            if node_id is not None:
                T.append(
                    threading.Thread(
                        target=self.spawn_updater,
                        args=(node_id, commands, ray_start)))
        for t in T:
            t.start()
        for t in T:
            t.join()

        # Attempt to recover unhealthy nodes
        for node_id in nodes:
            self.recover_if_needed(node_id, now)

    def reload_config(self, errors_fatal=False):
        try:
            with open(self.config_path) as f:
                new_config = yaml.safe_load(f.read())
            validate_config(new_config)
            new_launch_hash = hash_launch_conf(new_config["worker_nodes"],
                                               new_config["auth"])
            new_runtime_hash = hash_runtime_conf(new_config["file_mounts"], [
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
                logger.exception("StandardAutoscaler: "
                                 "Error parsing config.")

    def target_num_workers(self):
        target_frac = self.config["target_utilization_fraction"]
        cur_used = self.load_metrics.approx_workers_used()
        ideal_num_nodes = int(np.ceil(cur_used / float(target_frac)))
        ideal_num_workers = ideal_num_nodes - 1  # subtract 1 for head node

        initial_workers = self.config["initial_workers"]
        aggressive = self.config["autoscaling_mode"] == "aggressive"
        if self.bringup:
            ideal_num_workers = max(ideal_num_workers, initial_workers)
        elif aggressive and cur_used > 0:
            # If we want any workers, we want at least initial_workers
            ideal_num_workers = max(ideal_num_workers, initial_workers)

        # Other resources are not supported at present.
        if "CPU" in self.resource_requests:
            try:
                cores_per_worker = self.config["worker_nodes"]["Resources"][
                    "CPU"]
            except KeyError:
                cores_per_worker = 1  # Assume the worst

            cores_desired = self.resource_requests["CPU"]

            ideal_num_workers = max(
                ideal_num_workers,
                int(np.ceil(cores_desired / cores_per_worker)))

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
            logger.info("StandardAutoscaler: "
                        "{}: Runtime state is {}, want {}".format(
                            node_id, applied, self.runtime_hash))
            return False
        return True

    def recover_if_needed(self, node_id, now):
        if not self.can_update(node_id):
            return
        key = self.provider.internal_ip(node_id)
        if key not in self.load_metrics.last_heartbeat_time_by_ip:
            self.load_metrics.last_heartbeat_time_by_ip[key] = now
        last_heartbeat_time = self.load_metrics.last_heartbeat_time_by_ip[key]
        delta = now - last_heartbeat_time
        if delta < AUTOSCALER_HEARTBEAT_TIMEOUT_S:
            return
        logger.warning("StandardAutoscaler: "
                       "{}: No heartbeat in {}s, "
                       "restarting Ray to recover...".format(node_id, delta))
        updater = NodeUpdaterThread(
            node_id=node_id,
            provider_config=self.config["provider"],
            provider=self.provider,
            auth_config=self.config["auth"],
            cluster_name=self.config["cluster_name"],
            file_mounts={},
            initialization_commands=[],
            setup_commands=[],
            ray_start_commands=with_head_node_ip(
                self.config["worker_start_ray_commands"]),
            runtime_hash=self.runtime_hash,
            process_runner=self.process_runner,
            use_internal_ip=True)
        updater.start()
        self.updaters[node_id] = updater

    def should_update(self, node_id):
        if not self.can_update(node_id):
            return None, None, None  # no update

        status = self.provider.node_tags(node_id).get(TAG_RAY_NODE_STATUS)
        if status == STATUS_UP_TO_DATE and self.files_up_to_date(node_id):
            return None, None, None  # no update

        successful_updated = self.num_successful_updates.get(node_id, 0) > 0
        if successful_updated and self.config.get("restart_only", False):
            init_commands = []
            ray_commands = self.config["worker_start_ray_commands"]
        elif successful_updated and self.config.get("no_restart", False):
            init_commands = self.config["worker_setup_commands"]
            ray_commands = []
        else:
            init_commands = self.config["worker_setup_commands"]
            ray_commands = self.config["worker_start_ray_commands"]

        return (node_id, init_commands, ray_commands)

    def spawn_updater(self, node_id, init_commands, ray_start_commands):
        updater = NodeUpdaterThread(
            node_id=node_id,
            provider_config=self.config["provider"],
            provider=self.provider,
            auth_config=self.config["auth"],
            cluster_name=self.config["cluster_name"],
            file_mounts=self.config["file_mounts"],
            initialization_commands=with_head_node_ip(
                self.config["initialization_commands"]),
            setup_commands=with_head_node_ip(init_commands),
            ray_start_commands=with_head_node_ip(ray_start_commands),
            runtime_hash=self.runtime_hash,
            process_runner=self.process_runner,
            use_internal_ip=True)
        updater.start()
        self.updaters[node_id] = updater

    def can_update(self, node_id):
        if node_id in self.updaters:
            return False
        if not self.launch_config_ok(node_id):
            return False
        if self.num_failed_updates.get(node_id, 0) > 0:  # TODO(ekl) retry?
            return False
        return True

    def launch_new_node(self, count, instance_type):
        logger.info(
            "StandardAutoscaler: Queue {} new nodes for launch".format(count))
        self.pending_launches.inc(instance_type, count)
        config = copy.deepcopy(self.config)
        self.launch_queue.put((config, count, instance_type))

    def workers(self):
        return self.provider.non_terminated_nodes(
            tag_filters={TAG_RAY_NODE_TYPE: NODE_TYPE_WORKER})

    def log_info_string(self, nodes, target):
        logger.info("StandardAutoscaler: {}".format(
            self.info_string(nodes, target)))
        logger.info("LoadMetrics: {}".format(self.load_metrics.info_string()))

    def info_string(self, nodes, target):
        suffix = ""
        if self.pending_launches:
            suffix += " ({} pending)".format(self.pending_launches.value)
        if self.updaters:
            suffix += " ({} updating)".format(len(self.updaters))
        if self.num_failed_updates:
            suffix += " ({} failed to update)".format(
                len(self.num_failed_updates))
        if self.bringup:
            suffix += " (bringup=True)"

        return "{}/{} target nodes{}".format(len(nodes), target, suffix)

    def request_resources(self, resources):
        for resource, count in resources.items():
            self.resource_requests[resource] = max(
                self.resource_requests[resource], count)

        logger.info("StandardAutoscaler: resource_requests={}".format(
            self.resource_requests))

    def kill_workers(self):
        logger.error("StandardAutoscaler: kill_workers triggered")
        nodes = self.workers()
        if nodes:
            self.provider.terminate_nodes(nodes)
        logger.error("StandardAutoscaler: terminated {} node(s)".format(
            len(nodes)))


# Note: this is an (experimental) user-facing API, do not move.
def request_resources(num_cpus=None, num_gpus=None, bundles=None):
    """Remotely request some CPU or GPU resources from the autoscaler.

    This function is to be called e.g. on a node before submitting a bunch of
    ray.remote calls to ensure that resources rapidly become available.

    In the future this could be extended to do GPU cores or other custom
    resources.

    This function is non blocking.

    Args:

        num_cpus: int -- the number of CPU cores to request
        num_gpus: int -- the number of GPUs to request (Not implemented)

    """
    if num_gpus is not None:
        raise NotImplementedError(
            "GPU resource is not yet supported through request_resources")
    r = services.create_redis_client(
        global_worker.node.redis_address,
        password=global_worker.node.redis_password)
    if num_cpus > 0:
        r.publish(AUTOSCALER_RESOURCE_REQUEST_CHANNEL,
                  json.dumps({
                      "CPU": num_cpus
                  }))
