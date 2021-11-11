from ray.serve import pipeline

import ray
ray.init(num_cpus=16)


@pipeline.step
def echo(inp):
    return inp


my_pipeline = echo(pipeline.INPUT).deploy()
assert my_pipeline.call(42) == 42


@pipeline.step(execution_mode="actors", num_replicas=2)
def echo(inp):
    return inp


my_pipeline = echo(pipeline.INPUT).deploy()
assert my_pipeline.call(42) == 42