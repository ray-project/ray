import enum
import ray
import ray.streaming.generated.remote_call_pb2 as remote_call_pb


class NodeType(enum.Enum):
    SOURCE = 0
    TRANSFORM = 1
    SINK = 2


class ExecutionNode:

    def __init__(self, node_pb):
        self.node_id = node_pb.node_id
        self.node_type = NodeType[node_pb.node_type.name]
        # TODO deserialize
        self.stream_operator = node_pb.stream_operator  # bytes
        self.execution_tasks = [ExecutionTask(task) for task in node_pb.execution_tasks]
        self.inputs_edges = [ExecutionEdge(edge) for edge in node_pb.inputs_edges]
        self.output_edges = [ExecutionEdge(edge) for edge in node_pb.output_edges]


class ExecutionEdge:
    def __init__(self, edge_pb):
        self.src_node_id = edge_pb.src_node_id
        self.target_node_id = edge_pb.target_node_id
        # TODO deserialize
        self.partition = edge_pb.partition  # bytes


class ExecutionTask:
    def __init__(self, task_pb):
        self.task_id = task_pb.task_id
        self.task_index = task_pb.task_index
        self.worker_actor_id = ray.ActorID(task_pb.worker_actor_id)


class ExecutionGraph:
    def __init__(self, graph_pb: remote_call_pb.ExecutionGraph):
        self._graph_pb = graph_pb
        self.execution_nodes = [ExecutionNode(node) for node in graph_pb.execution_nodes()]

    def build_time(self):
        return self._graph_pb.build_time()

    def execution_nodes(self):
        return self.execution_nodes

    def get_execution_task_by_task_id(self, task_id):
        for execution_node in self.execution_nodes:
            for task in execution_node.execution_tasks:
                if task.task_id == task_id:
                    return task
        raise Exception("Task %s does not exist!".format(task_id))

    def get_execution_node_by_task_id(self, task_id):
        for execution_node in self.execution_nodes:
            for task in execution_node.execution_tasks:
                if task.task_id == task_id:
                    return execution_node
        raise Exception("Task %s does not exist!".format(task_id))

    def get_task_id2_worker_by_node_id(self, node_id):
        for execution_node in self.execution_nodes:
            if execution_node.node_id == node_id:
                task_id2_worker = {}
                for task in execution_node.execution_tasks:
                    worker_actor_id_bytes = task.worker_actor_id
                    task_id2_worker[task.task_id] = worker_actor_id_bytes
                return task_id2_worker
        raise Exception("Node %s does not exist!".format(node_id))
