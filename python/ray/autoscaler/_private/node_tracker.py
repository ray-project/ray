import os
import sys

from ray.autoscaler._private.process_runner_interceptor \
    import ProcessRunnerInterceptor

class NodeTracker:
    """Map nodes to their corresponding logs.

    We need to be a little careful here. At an given point in time, node_id <->
    ip can be interchangeably used, but the node_id -> ip relation is not
    bijective _across time_ since IP addresses can be reused. Therefore, we
    should treat node_id as the only unique identifier.

    """

    def __init__(self, log_dir, process_runner):
        self.log_dir = log_dir
        self.process_runner = process_runner

        # Mapping from node_id -> (ip, node type, stdout_path, process runner)
        # TODO(Alex): In the spirit of statelessness, we should try to load
        # this mapping from the filesystem. _Technically_ this tracker is only
        # used for "recent" failures though, so remembering old nodes is a best
        # effort, therefore it's already correct
        self.node_mapping = {}

    def get_or_create_process_runner(self, node_id, ip=None, node_type=None):
        if node_id not in self.node_mapping:
            if self.log_dir is None:
                stdout_obj = sys.stdout
                stdout_path = "<stdout>"
                process_runner = self.process_runner
            else:
                stdout_name = f"{node_id}.out"
                stdout_path = os.path.join(self.log_dir, stdout_name)
                stdout_obj = open(stdout_path, "ab")
                process_runner = ProcessRunnerInterceptor(
                    stdout_obj, process_runner=self.process_runner)
            self.node_mapping[node_id] = (ip, node_type, stdout_path,
                                          process_runner)

        _, _, _, process_runner = self.node_mapping[node_id]
        return process_runner

    def get_log_path(self, node_id):
        if node_id in self.node_mapping:
            return self.node_mapping[node_id][2]
        return None

    def get_all_failed_node_info(self, non_failed_ids):
        failed_nodes = self.node_mapping.keys() - non_failed_ids
        failed_info = []
        for node_id in failed_nodes:
            ip, node_type, stdout_path, _ = self.node_mapping[node_id]
            failed_info.append((ip, node_type, stdout_path))
        return failed_info
