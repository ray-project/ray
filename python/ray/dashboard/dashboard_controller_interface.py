class DashboardControllerInterface:
    """Interface to get Ray cluster metrics and control actions"""

    def node_info(self):
        raise NotImplementedError("Please implement this method.")

    def raylet_info(self):
        raise NotImplementedError("Please implement this method.")

    def launch_profiling(self, node_id, pid, duration):
        raise NotImplementedError("Please implement this method.")

    def check_profiling_status(self, profiling_id):
        raise NotImplementedError("Please implement this method.")

    def get_profiling_info(self, profiling_id):
        raise NotImplementedError("Please implement this method.")

    def kill_actor(self, actor_id, ip_address, port):
        raise NotImplementedError("Please implement this method.")

    def logs(self, hostname, pid):
        raise NotImplementedError("Please implement this method.")

    def errors(self, hostname, pid):
        raise NotImplementedError("Please implement this method.")

    def start_collecting_metrics(self):
        raise NotImplementedError("Please implement this method.")