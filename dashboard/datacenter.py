import asyncio
import logging
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.memory_utils as memory_utils

# TODO(fyrestone): Not import from dashboard module.
from ray.dashboard.modules.actor.actor_utils import actor_classname_from_task_spec
from ray.dashboard.utils import Dict, Signal, async_loop_forever

logger = logging.getLogger(__name__)


class GlobalSignals:
    node_info_fetched = Signal(dashboard_consts.SIGNAL_NODE_INFO_FETCHED)
    node_summary_fetched = Signal(dashboard_consts.SIGNAL_NODE_SUMMARY_FETCHED)
    job_info_fetched = Signal(dashboard_consts.SIGNAL_JOB_INFO_FETCHED)
    worker_info_fetched = Signal(dashboard_consts.SIGNAL_WORKER_INFO_FETCHED)


class DataSource:
    # {node id hex(str): node stats(dict of GetNodeStatsReply
    # in node_manager.proto)}
    node_stats = Dict()
    # {node id hex(str): node physical stats(dict from reporter_agent.py)}
    node_physical_stats = Dict()
    # {actor id hex(str): actor table data(dict of ActorTableData
    # in gcs.proto)}
    actors = Dict()
    # {job id hex(str): job table data(dict of JobTableData in gcs.proto)}
    jobs = Dict()
    # {node id hex(str): dashboard agent [http port(int), grpc port(int)]}
    agents = Dict()
    # {node id hex(str): gcs node info(dict of GcsNodeInfo in gcs.proto)}
    nodes = Dict()
    # {node id hex(str): ip address(str)}
    node_id_to_ip = Dict()
    # {node id hex(str): hostname(str)}
    node_id_to_hostname = Dict()
    # {node id hex(str): worker list}
    node_workers = Dict()
    # {node id hex(str): {actor id hex(str): actor table data}}
    node_actors = Dict()
    # {job id hex(str): worker list}
    job_workers = Dict()
    # {job id hex(str): {actor id hex(str): actor table data}}
    job_actors = Dict()
    # {worker id(str): core worker stats}
    core_worker_stats = Dict()
    # {job id hex(str): {event id(str): event dict}}
    events = Dict()
    # {node ip (str): log counts by pid
    # (dict from pid to count of logs for that pid)}
    ip_and_pid_to_log_counts = Dict()
    # {node ip (str): error entries by pid
    # (dict from pid to list of latest err entries)}
    ip_and_pid_to_errors = Dict()
    # The current scheduling stats (e.g., pending actor creation tasks)
    # of gcs.
    # {task type(str): task list}
    gcs_scheduling_stats = Dict()


class DataOrganizer:
    @staticmethod
    @async_loop_forever(dashboard_consts.PURGE_DATA_INTERVAL_SECONDS)
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
    @async_loop_forever(dashboard_consts.ORGANIZE_DATA_INTERVAL_SECONDS)
    async def organize(cls):
        job_workers = {}
        node_workers = {}
        core_worker_stats = {}
        # await inside for loop, so we create a copy of keys().
        for node_id in list(DataSource.nodes.keys()):
            workers = await cls.get_node_workers(node_id)
            for worker in workers:
                job_id = worker["jobId"]
                job_workers.setdefault(job_id, []).append(worker)
                for stats in worker.get("coreWorkerStats", []):
                    worker_id = stats["workerId"]
                    core_worker_stats[worker_id] = stats
            node_workers[node_id] = workers
        DataSource.job_workers.reset(job_workers)
        DataSource.node_workers.reset(node_workers)
        DataSource.core_worker_stats.reset(core_worker_stats)

    @classmethod
    async def get_node_workers(cls, node_id):
        workers = []
        node_ip = DataSource.node_id_to_ip[node_id]
        node_log_counts = DataSource.ip_and_pid_to_log_counts.get(node_ip, {})
        node_errs = DataSource.ip_and_pid_to_errors.get(node_ip, {})
        node_physical_stats = DataSource.node_physical_stats.get(node_id, {})
        node_stats = DataSource.node_stats.get(node_id, {})
        # Merge coreWorkerStats (node stats) to workers (node physical stats)
        pid_to_worker_stats = {}
        pid_to_language = {}
        pid_to_job_id = {}
        pids_on_node = set()
        for core_worker_stats in node_stats.get("coreWorkersStats", []):
            pid = core_worker_stats["pid"]
            pids_on_node.add(pid)
            pid_to_worker_stats.setdefault(pid, []).append(core_worker_stats)
            pid_to_language[pid] = core_worker_stats["language"]
            pid_to_job_id[pid] = core_worker_stats["jobId"]

        # Clean up logs from a dead pid.
        dead_pids = set(node_log_counts.keys()) - pids_on_node
        for dead_pid in dead_pids:
            if dead_pid in node_log_counts:
                node_log_counts.mutable().pop(dead_pid)

        for worker in node_physical_stats.get("workers", []):
            worker = dict(worker)
            pid = worker["pid"]
            worker["logCount"] = node_log_counts.get(str(pid), 0)
            worker["errorCount"] = len(node_errs.get(str(pid), []))
            worker["coreWorkerStats"] = pid_to_worker_stats.get(pid, [])
            worker["language"] = pid_to_language.get(
                pid, dashboard_consts.DEFAULT_LANGUAGE
            )
            worker["jobId"] = pid_to_job_id.get(pid, dashboard_consts.DEFAULT_JOB_ID)

            await GlobalSignals.worker_info_fetched.send(node_id, worker)

            workers.append(worker)
        return workers

    @classmethod
    async def get_node_info(cls, node_id):
        node_physical_stats = dict(DataSource.node_physical_stats.get(node_id, {}))
        node_stats = dict(DataSource.node_stats.get(node_id, {}))
        node = DataSource.nodes.get(node_id, {})
        node_ip = DataSource.node_id_to_ip.get(node_id)
        # Merge node log count information into the payload
        log_counts = DataSource.ip_and_pid_to_log_counts.get(node_ip, {})
        node_log_count = 0
        for entries in log_counts.values():
            node_log_count += entries
        error_info = DataSource.ip_and_pid_to_errors.get(node_ip, {})
        node_err_count = 0
        for entries in error_info.values():
            node_err_count += len(entries)

        node_stats.pop("coreWorkersStats", None)

        view_data = node_stats.get("viewData", [])
        ray_stats = cls._extract_view_data(
            view_data, {"object_store_used_memory", "object_store_available_memory"}
        )

        node_info = node_physical_stats
        # Merge node stats to node physical stats under raylet
        node_info["raylet"] = node_stats
        node_info["raylet"].update(ray_stats)

        # Merge GcsNodeInfo to node physical stats
        node_info["raylet"].update(node)
        # Merge actors to node physical stats
        node_info["actors"] = DataSource.node_actors.get(node_id, {})
        # Update workers to node physical stats
        node_info["workers"] = DataSource.node_workers.get(node_id, [])
        node_info["logCount"] = node_log_count
        node_info["errorCount"] = node_err_count
        await GlobalSignals.node_info_fetched.send(node_info)

        return node_info

    @classmethod
    async def get_node_summary(cls, node_id):
        node_physical_stats = dict(DataSource.node_physical_stats.get(node_id, {}))
        node_stats = dict(DataSource.node_stats.get(node_id, {}))
        node = DataSource.nodes.get(node_id, {})

        node_physical_stats.pop("workers", None)
        node_stats.pop("workersStats", None)
        view_data = node_stats.get("viewData", [])
        ray_stats = cls._extract_view_data(
            view_data, {"object_store_used_memory", "object_store_available_memory"}
        )
        node_stats.pop("viewData", None)

        node_summary = node_physical_stats
        # Merge node stats to node physical stats
        node_summary["raylet"] = node_stats
        node_summary["raylet"].update(ray_stats)
        # Merge GcsNodeInfo to node physical stats
        node_summary["raylet"].update(node)

        await GlobalSignals.node_summary_fetched.send(node_summary)

        return node_summary

    @classmethod
    async def get_all_node_summary(cls):
        return [
            await DataOrganizer.get_node_summary(node_id)
            for node_id in DataSource.nodes.keys()
        ]

    @classmethod
    async def get_all_node_details(cls):
        return [
            await DataOrganizer.get_node_info(node_id)
            for node_id in DataSource.nodes.keys()
        ]

    @classmethod
    async def get_all_actors(cls):
        result = {}
        for index, (actor_id, actor) in enumerate(DataSource.actors.items()):
            result[actor_id] = await cls._get_actor(actor)
            # There can be thousands of actors including dead ones. Processing
            # them all can take many seconds, which blocks all other requests
            # to the dashboard. The ideal solution might be to implement
            # pagination. For now, use a workaround to yield to the event loop
            # periodically, so other request handlers have a chance to run and
            # avoid long latencies.
            if index % 1000 == 0 and index > 0:
                # Canonical way to yield to the event loop:
                # https://github.com/python/asyncio/issues/284
                await asyncio.sleep(0)
        return result

    @staticmethod
    async def _get_actor(actor):
        actor = dict(actor)
        worker_id = actor["address"]["workerId"]
        core_worker_stats = DataSource.core_worker_stats.get(worker_id, {})
        actor_constructor = core_worker_stats.get(
            "actorTitle", "Unknown actor constructor"
        )
        actor["actorConstructor"] = actor_constructor
        actor.update(core_worker_stats)

        # TODO(fyrestone): remove this, give a link from actor
        # info to worker info in front-end.
        node_id = actor["address"]["rayletId"]
        pid = core_worker_stats.get("pid")
        node_physical_stats = DataSource.node_physical_stats.get(node_id, {})
        actor_process_stats = None
        actor_process_gpu_stats = []
        if pid:
            for process_stats in node_physical_stats.get("workers", []):
                if process_stats["pid"] == pid:
                    actor_process_stats = process_stats
                    break

            for gpu_stats in node_physical_stats.get("gpus", []):
                # gpu_stats.get("processes") can be None, an empty list or a
                # list of dictionaries.
                for process in gpu_stats.get("processes") or []:
                    if process["pid"] == pid:
                        actor_process_gpu_stats.append(gpu_stats)
                        break

        actor["gpus"] = actor_process_gpu_stats
        actor["processStats"] = actor_process_stats
        return actor

    @classmethod
    async def get_actor_creation_tasks(cls):
        # Collect infeasible tasks in worker nodes.
        infeasible_tasks = sum(
            (
                list(node_stats.get("infeasibleTasks", []))
                for node_stats in DataSource.node_stats.values()
            ),
            [],
        )
        # Collect infeasible actor creation tasks in gcs.
        infeasible_tasks.extend(
            list(DataSource.gcs_scheduling_stats.get("infeasibleTasks", []))
        )
        new_infeasible_tasks = []
        for task in infeasible_tasks:
            task = dict(task)
            task["actorClass"] = actor_classname_from_task_spec(task)
            task["state"] = "INFEASIBLE"
            new_infeasible_tasks.append(task)

        # Collect pending tasks in worker nodes.
        resource_pending_tasks = sum(
            (
                list(data.get("readyTasks", []))
                for data in DataSource.node_stats.values()
            ),
            [],
        )
        # Collect pending actor creation tasks in gcs.
        resource_pending_tasks.extend(
            list(DataSource.gcs_scheduling_stats.get("readyTasks", []))
        )
        new_resource_pending_tasks = []
        for task in resource_pending_tasks:
            task = dict(task)
            task["actorClass"] = actor_classname_from_task_spec(task)
            task["state"] = "PENDING_RESOURCES"
            new_resource_pending_tasks.append(task)

        results = {
            task["actorCreationTaskSpec"]["actorId"]: task
            for task in new_resource_pending_tasks + new_infeasible_tasks
        }
        return results

    @classmethod
    async def get_memory_table(
        cls,
        sort_by=memory_utils.SortingType.OBJECT_SIZE,
        group_by=memory_utils.GroupByType.STACK_TRACE,
    ):
        all_worker_stats = []
        for node_stats in DataSource.node_stats.values():
            all_worker_stats.extend(node_stats.get("coreWorkersStats", []))
        memory_information = memory_utils.construct_memory_table(
            all_worker_stats, group_by=group_by, sort_by=sort_by
        )
        return memory_information

    @staticmethod
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
