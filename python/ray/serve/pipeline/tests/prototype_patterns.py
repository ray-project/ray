# preprocess_func -> [ModelVariant * 20] (choose one given attribute)
from typing import Callable
import statistics

from ray import serve
from ray.serve import pipeline
from ray.serve.pipeline.common import ExecutionMode
from ray.serve.pipeline.node import INPUT, PipelineNode
from typing import List


@pipeline.step
class FeatureProcessing:
    def __init__(self):
        pass

    def __call__(self, req):
        return req

# @pipeline.step(execution_mode=ExecutionMode.ACTORS)
# class DynamicDispatch:
#     def __init__(
#         self,
#         # We want to use class Model here .. but its only defined underneath
#         # Ideally this class is defined in its own dedicated file and imported
#         # into the file where we want to write the pipeline driver, which won't
#         # be a problem anymore.
#         downstream_models: List[PipelineNode]
#     ):
#         # Dynamically injected upon DAG instantiation
#         # (kinda like autograd that fills this up in backwards pass)
#         self.downstream_models = downstream_models

#     # Reserved fields ? Might make authoring annoying for user
#     def __downstream_nodes__(self):
#         return self.downstream_models

#     def __upstream_nodes__(self):
#         return self.upstream_nodes

#     def __call__(self, req):
#         """
#         Choose right downstream model to forward request to.
#         """
#         assert len(self.downstream_models) == 3, (
#             "There should be exactly three downstream models to dispatch to.")
#         if req < 5:
#             return self.downstream_models[0].call()
#         elif req >= 5 and req < 10:
#             return self.downstream_models[1].call()
#         else:
#             return self.downstream_models[2].call()

# @pipeline.step
# class Model:
#     # Note there's ZERO pipeline logic here on purpose, just focus on the model
#     # Can also be instantiated multiple times with different weights, under
#     # same class def & implementation.
#     def __init__(self, weight):
#         self.weight = weight

#     def __call__(self, req):
#         return req * self.weight

# @pipeline.step
# class DynamicSelection:
#     """
#     Choose a subset of all models available to call
#     """
#     def __init__(self, policy: Callable):
#         self.policy = policy

#     def __call__(self, *args):
#         # Only pick outputs below some threshold
#         return self.policy(*args)

# @pipeline.step
# def aggregate_fn(*args):
#     return statistics.mean(*args)


# @serve.deployment
class Pipeline:
    def __init__(self):
        feature_processing = FeatureProcessing()
        # dispatch = DynamicDispatch()
        # selection = DynamicSelection(lambda x : x if x[1] > 5 else None)

        # # All inputs go through this step, no forking yet
        # dispatched = dispatch(feature_processing(pipeline.INPUT))
        # for i in range(10):
        #     model = Model(i)
        #     # model - feature DAG is built for each connection
        #     selection(
        #         model(dispatched)
        #     )


        # self._pipeline = aggregate_fn(selection).instantiate()
        self._pipeline = feature_processing(pipeline.INPUT).instantiate()

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


    def __call__(self, req):
        return self._pipeline.call(req)



if __name__ == "__main__":
    pipeline = Pipeline()
    for i in range(10):
        print(pipeline(i))