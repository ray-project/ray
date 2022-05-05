---
jupytext:
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.13.6
kernelspec:
  display_name: Python 3
  language: python
  name: python3
---

(serve-deployment-graph-cookbook)=

# Deployment Graph Cookbook

### Chain Nodes

The example shows how to chain nodes using the same class and having different args passed in

+++

```python
import ray
from ray import serve
from ray.experimental.dag.input_node import InputNode
from ray.serve.drivers import DAGDriver

ray.init(num_cpus=16)
serve.start()

@serve.deployment
class Model:
   def __init__(self, weight):
      self.weight = weight
   def forward(self, input):
      return input +  self.weight


# 10 nodes chain
num_nodes = 10
nodes = [Model.bind(w) for w in range(num_nodes)]
prev_outputs = [None] * num_nodes
with InputNode() as dag_input:
   for i in range(num_nodes):
      if i == 0:
         # first node
         prev_outputs[i] = nodes[i].forward.bind(dag_input)
      else:
         prev_outputs[i] = nodes[i].forward.bind(prev_outputs[i - 1])
   
   serve_dag = DAGDriver.options(route_prefix="/my-dag").bind(prev_outputs[-1])

dag_handle = serve.run(serve_dag)
print(ray.get(dag_handle.predict.remote(0)))
```

+++