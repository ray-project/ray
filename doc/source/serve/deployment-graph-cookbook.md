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

This doc is to provide more common use cases for using the deployment graph.

# Catalog
[Chain nodes with same class and different args](#chain_nodes_same_class_different_args)

## Chain nodes with same class and different args <a name="chain_nodes_same_class_different_args"></a>

The example shows how to chain nodes using the same class and having different args passed in

+++

```python
import ray
from ray import serve
from ray.experimental.dag.input_node import InputNode
from ray.serve.drivers import DAGDriver


#Adjust the num_cpus based on your device 
ray.init(num_cpus=4)
serve.start()

@serve.deployment
class Model:
   def __init__(self, weight):
      self.weight = weight
   def forward(self, input):
      return input +  self.weight


# 10 nodes chain in a line
num_nodes = 10
nodes = [Model.bind(w) for w in range(num_nodes)]
outputs = [None] * num_nodes
with InputNode() as dag_input:
   for i in range(num_nodes):
      if i == 0:
         # first node
         outputs[i] = nodes[i].forward.bind(dag_input)
      else:
         outputs[i] = nodes[i].forward.bind(outputs[i - 1])

print(ray.get(outputs[-1].execute(0)))
```

### Outputs

The graph will add all nodes weights plus the input (which is 0 in this case)

```
45
```

+++