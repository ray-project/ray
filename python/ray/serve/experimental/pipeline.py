
from ray.serve import pipeline
from ray.serve.pipeline import ExecutionMode

# Step 1: Define classes with annotated executor mode
@pipeline.node(execution_mode=ExecutionMode.ACTORS, num_replicas=2)
class Preprocess:
  def __init__(self, constant: float):
    self.constant = constant

  def __call__(self, req: float) -> float:
    return self.constant * req

@pipeline.node(execution_mode=ExecutionMode.ACTORS, num_replicas=2)
class ModelA:
  def __init__(self, weight: float):
    self.weight = weight

  def __call__(self, req: float) -> float:
    return self.weight * req

@pipeline.node(execution_mode=ExecutionMode.ACTORS, num_replicas=2)
class ModelB:
  def __init__(self, weight: float):
    self.weight = weight

  def __call__(self, req: float) -> float:
    return self.weight * req

@pipeline.node(execution_mode=ExecutionMode.ACTORS, num_replicas=2)
class Ensemble:
  def __init__(self, a_weight: float, b_weight: float):
    self.a_weight = a_weight
    self.b_weight = b_weight

  def __call__(self, a_output: float, b_output: float) -> float:
    return self.a_weight * a_output + self.b_weight * b_output

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
  ).deploy()

  print(dag.call(1))
  print(f"DAG output node: {dag._output_node}")

# Step 3: Deploy this pipeline using a Driver class as deployment.
# @serve.deployment(route_prefix="/whatever")
# class Driver:
#     def __init__(self):
#         self._dag = build_dag() # Deploy actors from the driver.

#     async def __call__(self):
#         return await self._dag.call_async(...)

# Step 4: Enable deployment on arbitrary node, support re-size.
# @serve.deployment(route_prefix="/whatever")
# class Driver:
#     def __init__(self, downstream_deployment: serve.Deployment["A"]):
#         self._dag = downstream_deployment(preprocess(pipeline.INPUT))

#     async def __call__(self):
#         return await self._dag.call_async(...)

# Step 5: Support simple single node upgrade with no dependencies.

# Step 6: Support upgrade-in-tandem with dependencies.

def main():
  build_dag()

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
