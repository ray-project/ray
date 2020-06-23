import logging

from ray.new_dashboard.utils import Dict

logger = logging.getLogger(__name__)


class DataSource:
    # {ip address(str): node stats(dict)}
    node_stats = Dict()
    # {ip address(str): node physical stats(dict)}
    node_physical_stats = Dict()
    # {actor id hex(str): actor table data(dict)}
    actors = Dict()
    # {ip address(str): port(int)}
    agents = Dict()
    # {ip address(str): port(int)}
    nodes = Dict()
    # {hostname(str): ip(str)}
    hostname_to_ip = Dict()
    # {ip(str): hostname(str)}
    ip_to_hostname = Dict()


class DataOrganizer:
    @staticmethod
    def purge():
        # These data sources are maintained by master,
        # we do not needs to purge them:
        #   * agents
        #   * nodes
        #   * hostname_to_ip
        #   * ip_to_hostname
        logger.info("Purge data.")
        valid_keys = DataSource.ip_to_hostname.keys()
        for key in DataSource.node_stats.keys() - valid_keys:
            DataSource.node_stats.pop(key)

        for key in DataSource.node_physical_stats.keys() - valid_keys:
            DataSource.node_physical_stats.pop(key)

    @classmethod
    def get_node_actors(cls, hostname):
        ip = DataSource.hostname_to_ip[hostname]
        node_stats = DataSource.node_stats.get(ip, {})
        node_worker_id_set = set()
        for worker_stats in node_stats.get("workersStats", []):
            node_worker_id_set.add(worker_stats["workerId"])
        node_actors = {}
        for actor_id, actor_table_data in DataSource.actors.items():
            if actor_table_data["workerId"] in node_worker_id_set:
                node_actors[actor_id] = actor_table_data
        return node_actors

    @classmethod
    def get_node_info(cls, hostname):
        ip = DataSource.hostname_to_ip[hostname]
        node_physical_stats = DataSource.node_physical_stats.get(ip, {})
        node_stats = DataSource.node_stats.get(ip, {})

        # Merge coreWorkerStats (node stats) to workers (node physical stats)
        workers_stats = node_stats.pop("workersStats", {})
        pid_to_worker_stats = {}
        for stats in workers_stats:
            d = pid_to_worker_stats.setdefault(stats["pid"], {}).setdefault(
                stats["workerId"], stats["coreWorkerStats"])
            d["workerId"] = stats["workerId"]

        for worker in node_physical_stats["workers"]:
            worker_stats = pid_to_worker_stats.get(worker["pid"], {})
            worker["coreWorkerStats"] = list(worker_stats.values())

        # Merge node stats to node physical stats
        node_info = node_physical_stats
        node_info["raylet"] = node_stats
        node_info["actors"] = cls.get_node_actors(hostname)

        return node_info

    @classmethod
    def get_all_node_summary(cls):
        all_nodes_summary = []
        for hostname in DataSource.hostname_to_ip.keys():
            node_info = cls.get_node_info(hostname)
            node_info.pop("workers", None)
            node_info["raylet"].pop("workersStats", None)
            node_info["raylet"].pop("viewData", None)
            all_nodes_summary.append(node_info)
        return all_nodes_summary
