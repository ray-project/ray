from typing import Type


from ray.experimental.dag import DAGNode
from ray.ml.checkpoint import Checkpoint
from ray.ml.predictor import Predictor
from ray.serve.model_wrappers import ModelWrapper
import ray
from ray import serve


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


# TODO(simon): replace other usage in pipeline codebase by this util function.
def _get_import_path(cls: Type):
    return f"{cls.__module__}.{cls.__qualname__}"


def make_ingress_deployment(
    name: str, serve_dag_root_json: str, input_schema_path: str
):
    return serve.deployment(ModelWrapper).options(
        name=name,
        init_kwargs={
            "predictor_cls": _get_import_path(PipelineIngressModel),
            "checkpoint": {
                "checkpoint_cls": _get_import_path(PipelineIngressCheckpoint),
                "uri": serve_dag_root_json,
            },
            "input_schema": input_schema_path,
            "batching_params": False,
        },
    )
