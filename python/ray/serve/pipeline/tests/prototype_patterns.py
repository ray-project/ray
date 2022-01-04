from typing import Any, Callable, Dict
import asyncio
# import statistics
import attr

from ray import serve
# from ray.serve import pipeline
# from ray.serve.pipeline.common import ExecutionMode
# from ray.serve.pipeline.node import INPUT, INJECTED, PipelineNode
# from typing import List


# Pipeline nodes are written from leaf to root (entrypoint)
@serve.deployment
class Model:
    # For backwards compatibility
    _version: int = 1

    # Note there's ZERO pipeline logic here on purpose, just focus on the model
    # Can also be instantiated multiple times with different weights, under
    # same class def & implementation.
    def __init__(self, weight):
        self.weight = weight
        self.policy = 1

    async def __call__(self, req):
        return req * self.weight

@serve.deployment
class FeatureProcessing:
    # For backwards compatibility
    _version: int = 1

    def __init__(self):
        # self.dynamic_dispatch = DynamicDispatch()
        pass

    async def __call__(self, req):
        return max(req, 0)

@serve.deployment
class Pipeline:
    # For backwards compatibility
    _version: int = 1

    def __init__(self):
        # Callable instantiated after forward()
        self.feature_processing = FeatureProcessing()

        # TODO: Add a pipeline container here so we can keep this implementation
        # but also make nodes registered with unique name for each instance
        # self.models = [Model(i) for i in range(3)]

        self.model_1 = Model(1) # What if this is heavy .. use a stub ?
        self.model_2 = Model(2)
        self.model_3 = Model(3)

    def __setattr__(self, __name: str, __value: Any) -> None:
        print("BBBB")

    async def __call__(self, req):
        """
        1) No ray API knowledge is required here, user just provides blocks of
            code. There's even no ray API call made.
        2) Underlying communication is handled by us, where we can decide how
            to make ray actor calls, to which group, on which node.
        3) For scaling and updates, user can opt-in serve deployment as an
            executor type with more dynamic support. Since we know the DAG, we
            can make right update in tandem calls while redirecting traffic
            accordingly on the right path.
        """
        processed_feature = await self.feature_processing(req)

        if processed_feature < 5:
            x = await self.model_1(processed_feature)
        elif processed_feature >= 5 and processed_feature < 10:
            x = await self.model_2(processed_feature)
        else:
            x = await self.model_3(processed_feature)

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

async def main():
    # Solve node init / instantiate
        # maybe just dummy task ?19
    # Solve node DAG tracing
        # See if we can avoid making symbolic calls with pipeline.INPUT
    # Add pprint strings
    # Can be called
    pipeline = Pipeline()
    # Recursively do:
    # add executor for self
    # traces other nodes as instance variables of my class, annotated as "step"
    # add to my node's dictionary
    # pipeline.instantiate(recursive=True)

    for i in range(10):
        print(await pipeline(i))

if __name__ == "__main__":
    asyncio.run(main())