from abc import ABC, abstractmethod


class BaseDashboardController(ABC):
    """Perform data fetching and other actions required by Dashboard 

    Make sure you run start_collecting_metrics function before using
    get_[stats]_info methods.

    TODO(sang): Write descriptions and requirements for each interface
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
        """Start threads/processes/actors to collect metrics
        
        NOTE: This interface should be called before using other 
                interface.
        """
        raise NotImplementedError("Please implement this method.")

