import enum

import ray
import ray.streaming.generated.remote_call_pb2 as remote_call_pb
import ray.streaming.generated.streaming_pb2 as streaming_pb
import ray.streaming.operator as operator
import ray.streaming.partition as partition
from ray.streaming.generated.streaming_pb2 import Language


class NodeType(enum.Enum):
    """
    SOURCE: Sources are where your program reads its input from

    TRANSFORM: Operators transform one or more DataStreams into a new
     DataStream. Programs can combine multiple transformations into
     sophisticated dataflow topologies.

    SINK: Sinks consume DataStreams and forward them to files, sockets,
     external systems, or print them.
    """
    SOURCE = 0
    TRANSFORM = 1
    SINK = 2


class ExecutionNode:
    def __init__(self, node_pb):
        self.node_id = node_pb.node_id
        self.node_type = NodeType[streaming_pb.NodeType.Name(
            node_pb.node_type)]
        self.parallelism = node_pb.parallelism
        if node_pb.language == Language.PYTHON:
            operator_bytes = node_pb.operator  # python operator descriptor
            self.stream_operator = operator.load_operator(operator_bytes)
        self.execution_tasks = [
            ExecutionTask(task) for task in node_pb.execution_tasks
        ]
        self.input_edges = [
            ExecutionEdge(edge, node_pb.language)
            for edge in node_pb.input_edges
        ]
        self.output_edges = [
            ExecutionEdge(edge, node_pb.language)
            for edge in node_pb.output_edges
        ]


class ExecutionEdge:
    def __init__(self, edge_pb, language):
        self.src_node_id = edge_pb.src_node_id
        self.target_node_id = edge_pb.target_node_id
        partition_bytes = edge_pb.partition
        # Sink node doesn't have partition function,
        # so we only deserialize partition_bytes when it's not None or empty
        if language == Language.PYTHON and partition_bytes:
            self.partition = partition.load_partition(partition_bytes)


class ExecutionTask:
    def __init__(self, task_pb):
        self.task_id = task_pb.task_id
        self.task_index = task_pb.task_index
        self.worker_actor = ray.actor.ActorHandle.\
            _deserialization_helper(task_pb.worker_actor)


class ExecutionGraph:
    def __init__(self, graph_pb: remote_call_pb.ExecutionGraph):
        self._graph_pb = graph_pb
        self.execution_nodes = [
            ExecutionNode(node) for node in graph_pb.execution_nodes
        ]

    def build_time(self):
        return self._graph_pb.build_time

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
                    task_id2_worker[task.task_id] = task.worker_actor
                return task_id2_worker
        raise Exception("Node %s does not exist!".format(node_id))
