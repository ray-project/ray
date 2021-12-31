from typing import Any, Callable, Dict
import statistics

from ray import serve
from ray.serve import pipeline
from ray.serve.pipeline.common import ExecutionMode
from ray.serve.pipeline.node import INPUT, INJECTED, PipelineNode
from typing import List

# Pipeline nodes are written from leaf to root (entrypoint)
@pipeline.step
class Model:
    # For backwards compatibility
    _version: int = 1

    # Note there's ZERO pipeline logic here on purpose, just focus on the model
    # Can also be instantiated multiple times with different weights, under
    # same class def & implementation.
    def __init__(self, weight):
        self.weight = weight

    def __call__(self, req):
        return req * self.weight

@pipeline.step
class FeatureProcessing:
    # For backwards compatibility
    _version: int = 1

    def __init__(self):
        # self.dynamic_dispatch = DynamicDispatch()
        pass

    def __call__(self, req):
        return max(req, 0)

@pipeline.step
class Pipeline:
    # For backwards compatibility
    _version: int = 1

    def __init__(self):
        # Callable instantiated after forward()
        self.feature_processing = FeatureProcessing()
        self.models = [Model(i) for i in range(3)]

    def __call__(self, req):
        """
        Build graph via symbolic execution of pipeline steps.
        """
        processed_feature = self.feature_processing(req)

        if processed_feature < 5:
            x = self.models[0](processed_feature)
        elif processed_feature >= 5 and processed_feature < 10:
            x = self.models[1](processed_feature)
        else:
            x = self.models[2](processed_feature)

        return x

        # Whole graph is           model_1
        #                        /          \
        # preprocess -- dispatch - model_2 -- selection -- aggregate_fn --> output
        #                        \          /
        #                          model_3
        # dispatch: choose model subset based on input attribute
        # selection: choose model outputs subset based on value

        # fixed sized vector for model fanout ... didn't look simple as node_ref

        # In authoring --> left to right
        # On execution --> right to left

        # can we automatically merge selection & aggregate in one node ?

    def __repr__(self):
        """
        Return pretty printted nested nodes.
        """
        pass

    def resize(self, node_name: str):
        pass

    def reconfigure(self, new_config: Dict[str, Any]):
        pass

    def update(self, node_name: str, serialized_class_callable: bytes):
        pass


if __name__ == "__main__":
    pipeline = Pipeline()
    for i in range(10):
        print(pipeline(i))