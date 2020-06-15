from collections import defaultdict
from ray.dashboard.util import to_unix_time, format_reply_id
from base64 import b64decode
import ray
import threading
import json
import traceback
import copy
import logging
import datetime
import time
import re

from operator import itemgetter

logger = logging.getLogger(__name__)


class NodeStats(threading.Thread):
    def __init__(self, redis_address, redis_password=None):
        self.redis_key = "{}.*".format(ray.gcs_utils.REPORTER_CHANNEL)
        self.redis_client = ray.services.create_redis_client(
            redis_address, password=redis_password)

        self._node_stats = {}
        self._addr_to_owner_addr = {}
        self._addr_to_actor_id = {}
        self._addr_to_extra_info_dict = {}
        self._node_stats_lock = threading.Lock()

        self._default_info = {
            "actorId": "",
            "children": {},
            "currentTaskFuncDesc": [],
            "ipAddress": "",
            "jobId": "",
            "numExecutedTasks": 0,
            "numLocalObjects": 0,
            "numObjectIdsInScope": 0,
            "port": 0,
            "state": 0,
            "taskQueueLength": 0,
            "usedObjectStoreMemory": 0,
            "usedResources": {},
        }

        # Mapping from IP address to PID to list of log lines
        self._logs = defaultdict(lambda: defaultdict(list))

        # Mapping from IP address to PID to list of error messages
        self._errors = defaultdict(lambda: defaultdict(list))

        ray.state.state._initialize_global_state(
            redis_address=redis_address, redis_password=redis_password)

        super().__init__()

    def _calculate_log_counts(self):
        return {
            ip: {
                pid: len(logs_for_pid)
                for pid, logs_for_pid in logs_for_ip.items()
            }
            for ip, logs_for_ip in self._logs.items()
        }

    def _calculate_error_counts(self):
        return {
            ip: {
                pid: len(errors_for_pid)
                for pid, errors_for_pid in errors_for_ip.items()
            }
            for ip, errors_for_ip in self._errors.items()
        }

    def _purge_outdated_stats(self):
        def current(then, now):
            if (now - then) > 5:
                return False

            return True

        now = to_unix_time(datetime.datetime.utcnow())
        self._node_stats = {
            k: v
            for k, v in self._node_stats.items() if current(v["now"], now)
        }

    def get_node_stats(self):
        with self._node_stats_lock:
            self._purge_outdated_stats()
            node_stats = sorted(
                (v for v in self._node_stats.values()),
                key=itemgetter("boot_time"))
            return {
                "clients": node_stats,
                "log_counts": self._calculate_log_counts(),
                "error_counts": self._calculate_error_counts(),
            }

    def get_actor_tree(self, workers_info_by_node, infeasible_tasks,
                       ready_tasks):
        now = time.time()
        # construct flattened actor tree
        flattened_tree = {"root": {"children": {}}}
        child_to_parent = {}
        with self._node_stats_lock:
            for addr, actor_id in self._addr_to_actor_id.items():
                flattened_tree[actor_id] = copy.deepcopy(self._default_info)
                flattened_tree[actor_id].update(
                    self._addr_to_extra_info_dict[addr])
                parent_id = self._addr_to_actor_id.get(
                    self._addr_to_owner_addr[addr], "root")
                child_to_parent[actor_id] = parent_id

            for node_id, workers_info in workers_info_by_node.items():
                for worker_info in workers_info:
                    if "coreWorkerStats" in worker_info:
                        core_worker_stats = worker_info["coreWorkerStats"]
                        addr = (core_worker_stats["ipAddress"],
                                str(core_worker_stats["port"]))
                        if addr in self._addr_to_actor_id:
                            actor_info = flattened_tree[self._addr_to_actor_id[
                                addr]]
                            format_reply_id(core_worker_stats)
                            actor_info.update(core_worker_stats)
                            actor_info["averageTaskExecutionSpeed"] = round(
                                actor_info["numExecutedTasks"] /
                                (now - actor_info["timestamp"] / 1000), 2)
                            actor_info["nodeId"] = node_id
                            actor_info["pid"] = worker_info["pid"]

            def _update_flatten_tree(task, task_spec_type, invalid_state_type):
                actor_id = ray.utils.binary_to_hex(
                    b64decode(task[task_spec_type]["actorId"]))
                caller_addr = (task["callerAddress"]["ipAddress"],
                               str(task["callerAddress"]["port"]))
                caller_id = self._addr_to_actor_id.get(caller_addr, "root")
                child_to_parent[actor_id] = caller_id
                task["state"] = -1
                task["invalidStateType"] = invalid_state_type
                task["actorTitle"] = task["functionDescriptor"][
                    "pythonFunctionDescriptor"]["className"]
                format_reply_id(task)
                flattened_tree[actor_id] = task

            for infeasible_task in infeasible_tasks:
                _update_flatten_tree(infeasible_task, "actorCreationTaskSpec",
                                     "infeasibleActor")

            for ready_task in ready_tasks:
                _update_flatten_tree(ready_task, "actorCreationTaskSpec",
                                     "pendingActor")

        # construct actor tree
        actor_tree = flattened_tree
        for actor_id, parent_id in child_to_parent.items():
            actor_tree[parent_id]["children"][actor_id] = actor_tree[actor_id]
        return actor_tree["root"]["children"]

    def get_logs(self, hostname, pid):
        ip = self._node_stats.get(hostname, {"ip": None})["ip"]
        logs = self._logs.get(ip, {})
        if pid:
            logs = {pid: logs.get(pid, [])}
        return logs

    def get_errors(self, hostname, pid):
        ip = self._node_stats.get(hostname, {"ip": None})["ip"]
        errors = self._errors.get(ip, {})
        if pid:
            errors = {pid: errors.get(pid, [])}
        return errors

    def run(self):
        p = self.redis_client.pubsub(ignore_subscribe_messages=True)

        p.psubscribe(self.redis_key)
        logger.info("NodeStats: subscribed to {}".format(self.redis_key))

        log_channel = ray.gcs_utils.LOG_FILE_CHANNEL
        p.subscribe(log_channel)
        logger.info("NodeStats: subscribed to {}".format(log_channel))

        error_channel = ray.gcs_utils.TablePubsub.Value("ERROR_INFO_PUBSUB")
        p.subscribe(error_channel)
        logger.info("NodeStats: subscribed to {}".format(error_channel))

        actor_channel = ray.gcs_utils.RAY_ACTOR_PUBSUB_PATTERN
        p.psubscribe(actor_channel)
        logger.info("NodeStats: subscribed to {}".format(actor_channel))

        current_actor_table = ray.actors()
        with self._node_stats_lock:
            for actor_data in current_actor_table.values():
                addr = (actor_data["Address"]["IPAddress"],
                        str(actor_data["Address"]["Port"]))
                owner_addr = (actor_data["OwnerAddress"]["IPAddress"],
                              str(actor_data["OwnerAddress"]["Port"]))
                self._addr_to_owner_addr[addr] = owner_addr
                self._addr_to_actor_id[addr] = actor_data["ActorID"]
                self._addr_to_extra_info_dict[addr] = {
                    "jobId": actor_data["JobID"],
                    "state": actor_data["State"],
                    "timestamp": actor_data["Timestamp"]
                }

        for x in p.listen():
            try:
                with self._node_stats_lock:
                    channel = ray.utils.decode(x["channel"])\
                                if "pattern" not in x or x["pattern"] is None\
                                else x["pattern"]
                    data = x["data"]
                    if channel == log_channel:
                        data = json.loads(ray.utils.decode(data))
                        ip = data["ip"]
                        pid = str(data["pid"])
                        self._logs[ip][pid].extend(data["lines"])
                    elif channel == str(error_channel):
                        gcs_entry = ray.gcs_utils.GcsEntry.FromString(data)
                        error_data = ray.gcs_utils.ErrorTableData.FromString(
                            gcs_entry.entries[0])
                        message = error_data.error_message
                        message = re.sub(r"\x1b\[\d+m", "", message)
                        match = re.search(r"\(pid=(\d+), ip=(.*?)\)", message)
                        if match:
                            pid = match.group(1)
                            ip = match.group(2)
                            self._errors[ip][pid].append({
                                "message": message,
                                "timestamp": error_data.timestamp,
                                "type": error_data.type
                            })
                    elif channel == actor_channel:
                        pubsub_msg = ray.gcs_utils.PubSubMessage.FromString(
                            data)
                        actor_data = ray.gcs_utils.ActorTableData.FromString(
                            pubsub_msg.data)
                        addr = (actor_data.address.ip_address,
                                str(actor_data.address.port))
                        owner_addr = (actor_data.owner_address.ip_address,
                                      str(actor_data.owner_address.port))
                        self._addr_to_owner_addr[addr] = owner_addr
                        self._addr_to_actor_id[addr] = ray.utils.binary_to_hex(
                            actor_data.actor_id)
                        self._addr_to_extra_info_dict[addr] = {
                            "jobId": ray.utils.binary_to_hex(
                                actor_data.job_id),
                            "state": actor_data.state,
                            "timestamp": actor_data.timestamp
                        }
                    elif channel == ray.gcs_utils.RAY_REPORTER_PUBSUB_PATTERN:
                        data = json.loads(ray.utils.decode(data))
                        self._node_stats[data["hostname"]] = data
                    else:
                        logger.warning("Unexpected channel data received, "
                                       "channel: {}, data: {}".format(
                                           channel,
                                           json.loads(ray.utils.decode(data))))

            except Exception:
                logger.exception(traceback.format_exc())
                continue
