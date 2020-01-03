import ray.streaming.generated.remote_call_pb2 as remote_call_pb


class ExecutionGraph:
    def __init__(self, graph_pb: remote_call_pb.ExecutionGraph):
        self._graph_pb = graph_pb

    def build_time(self):
        return self._graph_pb.build_time()

    def execution_nodes(self):
        return self._graph_pb.execution_nodes()

    def get_execution_task_by_task_id(self, task_id):
        for execution_node in self._graph_pb.execution_nodes:
            for task in execution_node.execution_tasks:
                if task.task_id == task_id:
                    return task
        raise Exception("Task %s does not exist!".format(task_id))

    def get_execution_node_by_task_id(self, task_id):
        for execution_node in self._graph_pb.execution_nodes:
            for task in execution_node.execution_tasks:
                if task.task_id == task_id:
                    return execution_node
        raise Exception("Task %s does not exist!".format(task_id))

    def get_task_id2_worker_by_node_id(self, node_id):
        for execution_node in self._graph_pb.execution_nodes:
            if execution_node.node_id == node_id:
                task_id2_worker = {}
                for task in execution_node.execution_tasks:
                    worker_actor_id_bytes = task.worker_actor_id
                    task_id2_worker[task.task_id] = worker_actor_id_bytes
                return task_id2_worker
        raise Exception("Node %s does not exist!".format(node_id))