
from starlette.requests import Request


import ray
from ray import serve
from ray.serve import pipeline
from ray.serve.pipeline import ExecutionMode

# Step 1: Define classes with annotated executor mode
@pipeline.node(node_id=1, execution_mode=ExecutionMode.ACTORS, num_replicas=2)
class Preprocess:
  def __init__(self, constant: float):
    self.constant = constant

  def __call__(self, req: str) -> str:
    return f"Preprocess({self.constant} * {req})"

@pipeline.node(node_id=2, execution_mode=ExecutionMode.ACTORS, num_replicas=2)
class ModelA:
  def __init__(self, weight: float):
    self.weight = weight

  def __call__(self, req: str) -> str:
    return f"ModelA({self.weight} * {req})"

@pipeline.node(node_id=3, execution_mode=ExecutionMode.ACTORS, num_replicas=2)
class ModelB:
  def __init__(self, weight: float):
    self.weight = weight

  def __call__(self, req: str) -> str:
    return f"ModelB({self.weight} * {req})"

@pipeline.node(node_id=4, execution_mode=ExecutionMode.ACTORS, num_replicas=2)
class Ensemble:
  def __init__(self, a_weight: float, b_weight: float):
    self.a_weight = a_weight
    self.b_weight = b_weight

  def __call__(self, a_output: str, b_output: str) -> str:
    return f"Ensemble({self.a_weight} * {a_output} + {self.b_weight} * {b_output})"

# Step 2: Construct pipeline DAG such that it's locally executable.
def build_dag():
  print("Instantiating model classes with pre-assigned weights.. \n")
  preprocess = Preprocess(0.5) # -> 0.5
  a = ModelA(0.2) # -> 0.1
  b = ModelB(0.8) # -> 0.4
  ensemble = Ensemble(1, 2) # -> 0.1 * 1 + 0.4 * 2 = 0.9

  print("Building and instantiating pipeline.. \n")
  dag = ensemble(
    a(preprocess(pipeline.INPUT)),
    b(preprocess(pipeline.INPUT))
  )

  return dag


my_dag = build_dag()
pipeline_id = "jiao_first_pipeline"
config = {
  "Preprocess": {
    "node_id": 1,
    "num_replicas": 2 # Deployment config overrides class decorator
  },
  "ModelA": {
    "node_id": 2,
    "num_replicas": 2
  },
  "ModelB": {
    "node_id": 3,
    "num_replicas": 2
  },
  "Ensemble": {
    "node_id": 4,
    "num_replicas": 2
  }
}
# Step 3: Deploy this pipeline via serve api
def deploy_pipeline():
  return serve.deploy(pipeline_id, config, pipeline_dag=my_dag)

# handle = serve.get_handle(pipeline_name)

# handle.remote(request)

# # serve update my_deployment config.yaml
# serve.deploy(pipeline_id, new_config)

# my_deployment -> driver deployment handle

#  - Preprocess
#     - id: zxc
#     - resources: CPU -> GPU
#     - model_path: s3://zzzz/bucket/aaa.pkl
#
#  - Model
#     - id: xxxxx -> (driver, dag ExecutorPipelineNode)
#     - num_replicas: 10 -> 20
#  - Model
#     - id: yyyy
#     - num_replicas: 5
#  - UnknownClass
#     - id: zzzzz
# dependencies: A -> [B, C] -> D


# Sharing actor group among two driver instances
# Update

# Step: FT


# Step 4: Enable upgradable node. It needs to use deployment handle in this case
# 1) can still be constructed with pipeline API
#     - maybe unified API for both ? like ExecutorNode & Deployment handle
# 2) upgrade API on deployment level
# 3) actor calls among executor nodes & deployments, strip the http part
# #     - seem trivial
# @serve.deployment(route_prefix="/hello")
# class Driver:
#     def __init__(self, downstream_deployment_handle: serve.Deployment["A"]):
#         self._dag = downstream_deployment_handle(preprocess(pipeline.INPUT))

#     async def __call__(self):
#         return await self._dag.call_async(...)

# Step 5: Support simple single node upgrade with no dependencies.

# Step 6: What if driver / actor died ?

# Step 6: Support upgrade-in-tandem with dependencies.

# Step 7: Test with async call

def main():
  # build_dag()
  ray.init(address="auto")
  serve.start(detached=True)
  pipeline_handle = deploy_pipeline()

  print(ray.get(pipeline_handle.remote("1")))

if __name__ == "__main__":
  main()

######################################

# In-flight questions:
"""

"""

######################################

# Conclusions:

"""
Code + DAG + Executor can happen locally on laptop with all supported executors.

So by default a serve pipeline is just a bunch of tasks/actors/actor groups calling each other.

Then, if user wants to have HTTP, use deployment -> This assumes scaling and upgrades are done in tandem.

In addition, if user cares about individual scaling & upgrade, use deployment on the node.


Infer class names in code is great, but needs to take care of one class name with different instantiations.
  - ClassName + Unique id (hash of code + config ?)

"""
