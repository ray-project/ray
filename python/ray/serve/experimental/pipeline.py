
from ray import serve
from ray.serve import pipeline
from ray.serve.pipeline import ExecutionMode

# Step 1: Define classes with annotated executor mode
@pipeline.step(execution_mode=ExecutionMode.LOCAL)
class Preprocess:
  def __init__(self, constant: float):
    self.constant = constant

  def __call__(self, req: float) -> float:
    return self.constant * req

@pipeline.step(execution_mode=ExecutionMode.LOCAL)
class ModelA:
  def __init__(self, weight: float):
    self.weight = weight

  def __call__(self, req: float) -> float:
    return self.weight * req

@pipeline.step(execution_mode=ExecutionMode.LOCAL)
class ModelB:
  def __init__(self, weight: float):
    self.weight = weight

  def __call__(self, req: float) -> float:
    return self.weight * req

@pipeline.step(execution_mode=ExecutionMode.LOCAL)
class Ensemble:
  def __init__(self, a_weight: float, b_weight: float):
    self.a_weight = a_weight
    self.b_weight = b_weight

  def __call__(self, a_output: float, b_output: float) -> float:
    return self.a_weight * a_output + self.b_weight * b_output

# Step 2: Construct pipeline DAG such that it's locally executable.
def build_dag():
    a = A.deploy() # Just instantiate group of actors.
    dag = a(preprocess(pipeline.INPUT))

# Step 3: Deploy this pipeline using a Driver class as deployment.
@serve.deployment(route_prefix="/whatever")
class Driver:
    def __init__(self):
        self._dag = build_dag() # Deploy actors from the driver.

    async def __call__(self):
        return await self._dag.call_async(...)

# Step 4: Enable deployment on arbitrary node, support re-size.
@serve.deployment(route_prefix="/whatever")
class Driver:
    def __init__(self, downstream_deployment: serve.Deployment["A"]):
        self._dag = downstream_deployment(preprocess(pipeline.INPUT))

    async def __call__(self):
        return await self._dag.call_async(...)

# Step 5: Support simple single node upgrade with no dependencies.

# Step 6: Support upgrade-in-tandem with dependencies.


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
