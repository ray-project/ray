import logging
import ray.new_dashboard.consts as dashboard_consts
import ray.new_dashboard.memory_utils as memory_utils
from collections import defaultdict
from ray.new_dashboard.actor_utils import actor_classname_from_task_spec
from ray.new_dashboard.utils import Dict, Signal

logger = logging.getLogger(__name__)


class GlobalSignals:
    node_info_fetched = Signal(dashboard_consts.SIGNAL_NODE_INFO_FETCHED)


class DataSource:
    # {node id hex(str): node stats(dict of GetNodeStatsReply
    # in node_manager.proto)}
    node_stats = Dict()
    # {node id hex(str): node physical stats(dict from reporter_agent.py)}
    node_physical_stats = Dict()
    # {actor id hex(str): actor table data(dict of ActorTableData
    # in gcs.proto)}
    actors = Dict()
    # {node id hex(str): dashboard agent [http port(int), grpc port(int)]}
    agents = Dict()
    # {node id hex(str): gcs node info(dict of GcsNodeInfo in gcs.proto)}
    nodes = Dict()
    # {node id hex(str): ip address(str)}
    node_id_to_ip = Dict()
    # {node id hex(str): hostname(str)}
    node_id_to_hostname = Dict()
    # {node ip (str): log entries by pid
    # (dict from pid to list of latest log entries)}
    ip_and_pid_to_logs = Dict()
    # {node ip (str): error entries by pid
    # (dict from pid to list of latest err entries)}
    ip_and_pid_to_errors = Dict()


class DataOrganizer:
    @staticmethod
    async def purge():
        # Purge data that is out of date.
        # These data sources are maintained by DashboardHead,
        # we do not needs to purge them:
        #   * agents
        #   * nodes
        #   * node_id_to_ip
        #   * node_id_to_hostname
        logger.info("Purge data.")
        alive_nodes = {
            node_id
            for node_id, node_info in DataSource.nodes.items()
            if node_info["state"] == "ALIVE"
        }
        for key in DataSource.node_stats.keys() - alive_nodes:
            DataSource.node_stats.pop(key)

        for key in DataSource.node_physical_stats.keys() - alive_nodes:
            DataSource.node_physical_stats.pop(key)

    @classmethod
    async def get_node_actors(cls, node_id):
        node_stats = DataSource.node_stats.get(node_id, {})
        node_physical_stats = DataSource.node_physical_stats.get(node_id, {})
        worker_id_to_raylet_info = {}
        pid_to_worker_id = {}

        for worker_stats in node_stats.get("workersStats", []):
            worker_id_to_raylet_info[worker_stats["workerId"]] = worker_stats
            pid_to_worker_id[worker_stats["pid"]] = worker_stats["workerId"]
        worker_id_to_process_info = {}

        for process_stats in node_physical_stats.get("workers"):
            if process_stats["pid"] in pid_to_worker_id:
                worker_id = pid_to_worker_id[process_stats["pid"]]
                worker_id_to_process_info[worker_id] = process_stats

        worker_id_to_gpu_stats = defaultdict(list)
        for gpu_stats in node_physical_stats.get("gpus"):
            for process in gpu_stats.get("processes", []):
                if process["pid"] in pid_to_worker_id:
                    worker_id = pid_to_worker_id[process["pid"]]
                    worker_id_to_gpu_stats[worker_id].append(gpu_stats)

        node_actors = {}
        for actor_id, actor_table_data in DataSource.actors.items():
            worker_id = actor_table_data["address"]["workerId"]
            if worker_id in worker_id_to_raylet_info:
                worker_raylet_stats = worker_id_to_raylet_info[worker_id]
                core_worker = worker_raylet_stats.get("coreWorkerStats", {})
                actor_constructor = core_worker.get(
                    "actorTitle", "Unknown actor constructor")

                actor_table_data["actorConstructor"] = actor_constructor

                actor_class = actor_classname_from_task_spec(
                    actor_table_data.get("taskSpec", {}))

                actor_table_data["actorClass"] = actor_class
                actor_table_data.update(core_worker)
                node_actors[actor_id] = actor_table_data
            actor_table_data["gpus"] = worker_id_to_gpu_stats.get(
                worker_id, [])
            actor_table_data["processStats"] = worker_id_to_process_info.get(
                worker_id, {})
        return node_actors

    @classmethod
    async def get_node_info(cls, node_id):
        node_physical_stats = DataSource.node_physical_stats.get(node_id, {})
        node_stats = DataSource.node_stats.get(node_id, {})
        node = DataSource.nodes.get(node_id, {})

        # Merge node log count information into the payload
        log_info = DataSource.ip_and_pid_to_logs.get(node_physical_stats["ip"],
                                                     {})
        node_log_count = 0
        for entries in log_info.values():
            node_log_count += len(entries)
        error_info = DataSource.ip_and_pid_to_errors.get(
            node_physical_stats["ip"], {})
        node_err_count = 0
        for entries in error_info.values():
            node_err_count += len(entries)

        # Merge coreWorkerStats (node stats) to workers (node physical stats)
        workers_stats = node_stats.pop("workersStats", {})
        pid_to_worker_stats = {}
        pid_to_language = {}
        pid_to_job_id = {}
        for stats in workers_stats:
            d = pid_to_worker_stats.setdefault(stats["pid"], {}).setdefault(
                stats["workerId"], stats["coreWorkerStats"])
            d["workerId"] = stats["workerId"]
            pid_to_language.setdefault(stats["pid"],
                                       stats.get("language", "PYTHON"))
            pid_to_job_id.setdefault(stats["pid"],
                                     stats["coreWorkerStats"]["jobId"])

        for worker in node_physical_stats.get("workers", []):
            worker_stats = pid_to_worker_stats.get(worker["pid"], {})
            worker["coreWorkerStats"] = list(worker_stats.values())
            worker["language"] = pid_to_language.get(worker["pid"], "")
            worker["jobId"] = pid_to_job_id.get(worker["pid"], "ffff")
            worker["logCount"] = len(log_info.get(str(worker["pid"]), []))
            worker["errorCount"] = len(error_info.get(str(worker["pid"]), []))

        ray_stats = _extract_view_data(
            node_stats["viewData"],
            {"object_store_used_memory", "object_store_available_memory"})

        node_info = node_physical_stats
        # Merge node stats to node physical stats under raylet
        node_info["raylet"] = node_stats
        node_info["raylet"].update(ray_stats)

        # Merge GcsNodeInfo to node physical stats
        node_info["raylet"].update(node)
        # Merge actors to node physical stats
        node_info["actors"] = await cls.get_node_actors(node_id)
        node_info["logCount"] = node_log_count
        node_info["errorCount"] = node_err_count
        await GlobalSignals.node_info_fetched.send(node_info)

        return node_info

    @classmethod
    async def get_all_node_summary(cls):
        all_nodes_summary = []
        for node_id in DataSource.nodes.keys():
            node_info = await cls.get_node_info(node_id)
            node_info.pop("workers", None)
            node_info.pop("actors", None)
            node_info["raylet"].pop("workersStats", None)
            node_info["raylet"].pop("viewData", None)
            all_nodes_summary.append(node_info)
        return all_nodes_summary

    @classmethod
    async def get_all_node_details(cls):
        node_details = []
        for node_id in DataSource.nodes.keys():
            node_details.append(await cls.get_node_info(node_id))
        return node_details

    @classmethod
    async def get_all_actors(cls):
        all_actors = {}
        for node_id in DataSource.nodes.keys():
            all_actors.update(await cls.get_node_actors(node_id))
        return all_actors

    @classmethod
    async def get_actor_creation_tasks(cls):
        infeasible_tasks = sum(
            (node_stats.get("infeasibleTasks", [])
             for node_stats in DataSource.node_stats.values()), [])
        for task in infeasible_tasks:
            task["actorClass"] = actor_classname_from_task_spec(task)
            task["state"] = "INFEASIBLE"

        resource_pending_tasks = sum(
            (data.get("readyTasks", [])
             for data in DataSource.node_stats.values()), [])
        for task in resource_pending_tasks:
            task["actorClass"] = actor_classname_from_task_spec(task)
            task["state"] = "PENDING_RESOURCES"

        results = {
            task["actorCreationTaskSpec"]["actorId"]: task
            for task in resource_pending_tasks + infeasible_tasks
        }
        return results

    @classmethod
    async def get_memory_table(cls,
                               sort_by=memory_utils.SortingType.OBJECT_SIZE,
                               group_by=memory_utils.GroupByType.STACK_TRACE):
        all_worker_stats = []
        for node_stats in DataSource.node_stats.values():
            all_worker_stats.extend(node_stats.get("workersStats", []))
        memory_information = memory_utils.construct_memory_table(
            all_worker_stats, group_by=group_by, sort_by=sort_by)
        return memory_information


def _extract_view_data(views, data_keys):
    view_data = {}
    for view in views:
        view_name = view["viewName"]
        if view_name in data_keys:
            if not view.get("measures"):
                view_data[view_name] = 0
                continue
            measure = view["measures"][0]
            if "doubleValue" in measure:
                measure_value = measure["doubleValue"]
            elif "intValue" in measure:
                measure_value = measure["intValue"]
            else:
                measure_value = 0
            view_data[view_name] = measure_value

    return view_data
