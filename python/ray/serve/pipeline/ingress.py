from ray.experimental.dag import DAGNode
from ray.ml.checkpoint import Checkpoint
from ray.ml.predictor import Predictor

import ray


class PipelineIngressModel(Predictor):
    """Pipeline's ingress node is another model wrapper to reuse ingres code."""

    def __init__(self, dag: DAGNode):
        self.dag = dag

    @classmethod
    def from_checkpoint(cls, checkpoint: "PipelineIngressCheckpoint") -> "Predictor":
        assert isinstance(checkpoint, PipelineIngressCheckpoint)
        return cls(checkpoint.dag)

    def predict(self, data) -> ray.ObjectRef:
        return self.dag.execute(data)


class PipelineIngressCheckpoint(Checkpoint):
    def __init__(self, serve_dag_node_json):
        import json

        from ray.serve.pipeline.json_serde import dagnode_from_json

        self.dag: DAGNode = json.loads(
            serve_dag_node_json, object_hook=dagnode_from_json
        )

    @classmethod
    def from_uri(cls, serve_dag_node_json):
        return PipelineIngressCheckpoint(serve_dag_node_json)
