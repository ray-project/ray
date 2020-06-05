import enum

import ray
import ray.streaming.generated.remote_call_pb2 as remote_call_pb
import ray.streaming.generated.streaming_pb2 as streaming_pb
import ray.streaming.operator as operator
import ray.streaming.partition as partition
from ray.streaming import function
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


class ExecutionEdge:
    def __init__(self, edge_pb, language):
        self.source_vertex_id = edge_pb.source_vertex_id
        self.target_vertex_id = edge_pb.target_vertex_id
        partition_bytes = edge_pb.partition
        # Sink node doesn't have partition function,
        # so we only deserialize partition_bytes when it's not None or empty
        if language == Language.PYTHON and partition_bytes:
            self.partition = partition.load_partition(partition_bytes)


class ExecutionVertex:
    def __init__(self, vertex_pb):
        self.vertex_id = vertex_pb.vertex_id
        self.job_vertex_Id = vertex_pb.job_vertex_Id
        self.job_vertex_name = vertex_pb.job_vertex_name
        self.vertex_index = vertex_pb.vertex_index
        self.parallelism = vertex_pb.parallelism
        self.job_vertex_Id = vertex_pb.job_vertex_Id
        if vertex_pb.language == Language.PYTHON:
            self.stream_operator = operator.create_operator(
                function.load_function(vertex_pb.function))
        self.worker_actor = ray.actor.ActorHandle. \
            _deserialization_helper(vertex_pb.worker_actor)
        self.container_id = vertex_pb.container_id
        self.build_time = vertex_pb.build_time
        self.language = vertex_pb.language
        self.config = vertex_pb.config
        self.resource = vertex_pb.resource


class ExecutionVertexContext:
    def __init__(self,
                 vertex_context_pb: remote_call_pb.ExecutionVertexContext):
        self.vertex = ExecutionVertex(vertex_context_pb.current_vertex)
        self.upstream_vertices = [
            ExecutionVertex(vertex)
            for vertex in vertex_context_pb.upstream_vertices
        ]
        self.downstream_vertices = [
            ExecutionVertex(vertex)
            for vertex in vertex_context_pb.downstream_vertices
        ]
        self.input_edges = [
            ExecutionEdge(edge, self.vertex.language)
            for edge in vertex_context_pb.input_edges
        ]
        self.output_edges = [
            ExecutionEdge(edge, self.vertex.language)
            for edge in vertex_context_pb.output_edges
        ]

    def get_parallelism(self):
        return self.vertex.parallelism

    def get_upstream_parallelism(self):
        if self.upstream_vertices:
            return self.upstream_vertices[0].parallelism
        return 0

    def get_downstream_parallelism(self):
        if self.downstream_vertices:
            return self.downstream_vertices[0].parallelism
        return 0

    @property
    def build_time(self):
        return self.vertex.build_time

    @property
    def stream_operator(self):
        return self.vertex.stream_operator

    @property
    def config(self):
        return self.vertex.config

    def get_task_id(self):
        return self.vertex.vertex_id

    def get_source_actor_by_vertex_id(self, vertex_id):
        for vertex in self.upstream_vertices:
            if vertex.vertex_id == vertex_id:
                return vertex.worker_actor
        raise Exception("Vertex %s does not exist!".format(vertex_id))

    def get_target_actor_by_vertex_id(self, vertex_id):
        for vertex in self.downstream_vertices:
            if vertex.vertex_id == vertex_id:
                return vertex.worker_actor
        raise Exception("Vertex %s does not exist!".format(vertex_id))
