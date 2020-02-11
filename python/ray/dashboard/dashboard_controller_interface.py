from abc import ABC, abstractmethod


class BaseDashboardController(ABC):
    """Interface to get Ray cluster metrics and control actions

    Make sure you run start_collecting_metrics function before using
    get_[stats]_info methods.
    """

    @abstractmethod
    def get_node_info(self):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def get_raylet_info(self):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def launch_profiling(self, node_id, pid, duration):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def check_profiling_status(self, profiling_id):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def get_profiling_info(self, profiling_id):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def kill_actor(self, actor_id, ip_address, port):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def get_logs(self, hostname, pid):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def get_errors(self, hostname, pid):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def start_collecting_metrics(self):
        raise NotImplementedError("Please implement this method.")
