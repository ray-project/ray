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


## Serve Deployment Graph

```{note} 
Note: This feature is still experimental in Alpha release, some APIs are subject to change.
```

### General Motivation

Production machine learning serving pipelines are getting longer and wider. They often consist of multiple, or even tens of models collectively making a final prediction, such as image / video content classification and tagging, fraud detection pipeline with multiple policies and models, multi-stage ranking and recommendation, etc.

Meanwhile, the size of a model is also growing beyond the memory limit of a single machine due to the exponentially growing number of parameters, such as GPT-3, sparse feature embeddings in recsys models such that the ability to do disaggregated and distributed inference is desirable and future proof.

We want to leverage the programmable and general purpose distributed computing ability of Ray, double down on its unique strengths (scheduling, communication and shared memory) to facilitate authoring, orchestrating, scaling and deployment of complex serving pipelines under one set of DAG API, so a user can program & test multiple models or multiple shards of a single large model dynamically, deploy to production at scale, and upgrade individually.

### Key requirements
- Provide the ability to author a DAG of serve nodes to form a complex inference graph.
- Pipeline authoring experience should be fully python programmable with support for dynamic selection, control flows, user business logic, etc.
- DAG can be instantiated and locally executed using tasks and actors API
- DAG can be deployed via declarative and idempotent API, individual nodes can be reconfigured and scaled indepenently.

__[Full Ray Enhancement Proposal, REP-001: Serve Pipeline](https://github.com/ray-project/enhancements/blob/main/reps/2022-03-08-serve_pipeline.md)__

+++

### Concepts

#### Deployment
Upgradeable group of actors managed by the Serve controller. Currently the primary API in Ray Serve. At authoring time it’s the class or function under `@serve.deployment` decorator. 

#### Node
Smallest unit in a graph, typically an annotated class but can also be a function, backed by a group of actors that are scalable and reconfigurable. 

#### Deployment Graph
Collection of nodes that forms a DAG that represents an inference graph for complicated tasks. Ex: ensemble, chaining, dynamic selection. 

#### Bind
A graph building API applicable to decorated class or function.  `decorated_class_or_func.bind(*args, **kwargs)` generates an IR node that can be used to build graph, and bound arguments will be applied at execution time, including dynamic user input.


+++

### Simple End to End Example

Let's start with a simple DAG with the following attributes where each node is empowered by a __[serve deployment](https://docs.ray.io/en/master/serve/core-apis.html#core-api-deployments)__:

- All nodes in the deployment graph naturally forms a DAG structure.
- A node could use or call into other nodes in the deployment graph.
- Deployment graph has mix of class and function as nodes.
- Same input or output can be used in multiple nodes in the DAG.
- A node might access partial user input.
- Control flow is used where dynamic dispatch happens with respect to input value.
- Same class can be constructed, or function bound with different args that generates multiple distinct nodes in DAG.
- A node can be called either sync or async.

```{code-cell} ipython3
# TODO: Add a diagram here
```

```{code-cell} ipython3
import ray
from ray import serve
from ray.serve.pipeline.generate import DeploymentNameGenerator

if ray.is_initialized():
    serve.shutdown()
    DeploymentNameGenerator.reset()
    ray.shutdown()

ray.init(num_cpus=16)
serve.start()
```

```{code-cell} ipython3
import time
import asyncio
import requests
import starlette

from ray.experimental.dag.input_node import InputNode

@serve.deployment
async def preprocessor(input_data: str):
    """Simple feature processing that converts str to int"""
    time.sleep(0.1) # Manual delay for blocking computation
    return int(input_data)

@serve.deployment
async def avg_preprocessor(input_data):
    """Simple feature processing that returns average of input list as float."""
    time.sleep(0.15) # Manual delay for blocking computation
    return sum(input_data) / len(input_data)

@serve.deployment
class Model:
    def __init__(self, weight: int):
        self.weight = weight

    async def forward(self, input: int):
        time.sleep(0.3) # Manual delay for blocking computation 
        return f"({self.weight} * {input})"


@serve.deployment
class Combiner:
    def __init__(self, m1: Model, m2: Model):
        self.m1 = m1
        self.m2 = m2

    async def run(self, req_part_1, req_part_2, operation):
        # Merge model input from two preprocessors  
        req = f"({req_part_1} + {req_part_2})"
        
        # Submit to both m1 and m2 with same req data in parallel
        r1_ref = self.m1.forward.remote(req)
        r2_ref = self.m2.forward.remote(req)
        
        # Async gathering of model forward results for same request data
        rst = await asyncio.gather(*[r1_ref, r2_ref])
        
        # Control flow that determines runtime behavior based on user input
        if operation == "sum":
            return f"sum({rst})"
        else:
            return f"max({rst})"
        
@serve.deployment
class DAGDriver:
    def __init__(self, dag_handle):
        self.dag_handle = dag_handle

    async def predict(self, inp):
        """Perform inference directly without HTTP."""
        return await self.dag_handle.remote(inp)

    async def __call__(self, request: starlette.requests.Request):
        """HTTP endpoint of the DAG."""
        input_data = await request.json()
        return await self.predict(input_data)

# DAG building
with InputNode() as dag_input:
    preprocessed_1 = preprocessor.bind(dag_input[0])  # Partial access of user input by index
    preprocessed_2 = avg_preprocessor.bind(dag_input[1]) # Partial access of user input by index
    m1 = Model.bind(1)
    m2 = Model.bind(2)
    combiner = Combiner.bind(m1, m2)
    dag = combiner.run.bind(
        preprocessed_1, preprocessed_2, dag_input[2]  # Partial access of user input by index
    ) 
    # Each serve dag has a driver deployment as ingress that can be user provided.
    serve_dag = DAGDriver.options(route_prefix="/my-dag").bind(dag)


dag_handle = serve.run(serve_dag)

# Warm up
ray.get(dag_handle.predict.remote(["0", [0, 0], "sum"]))

# Python handle 
cur = time.time()
print(ray.get(dag_handle.predict.remote(["5", [1, 2], "sum"])))
print(f"Time spent: {round(time.time() - cur, 2)} secs.")
# Http endpoint
cur = time.time()
print(requests.post("http://127.0.0.1:8000/my-dag", json=["5", [1, 2], "sum"]).text)
print(f"Time spent: {round(time.time() - cur, 2)} secs.")

# Python handle 
cur = time.time()
print(ray.get(dag_handle.predict.remote(["1", [0, 2], "max"])))
print(f"Time spent: {round(time.time() - cur, 2)} secs.")

# Http endpoint
cur = time.time()
print(requests.post("http://127.0.0.1:8000/my-dag", json=["1", [0, 2], "max"]).text)
print(f"Time spent: {round(time.time() - cur, 2)} secs.")
```

### Outputs

```
sum(['(1 * (5 + 1.5))', '(2 * (5 + 1.5))'])
Time spent: 0.49 secs.
sum(['(1 * (5 + 1.5))', '(2 * (5 + 1.5))'])
Time spent: 0.49 secs.


max(['(1 * (1 + 1.0))', '(2 * (1 + 1.0))'])
Time spent: 0.48 secs.
max(['(1 * (1 + 1.0))', '(2 * (1 + 1.0))'])
Time spent: 0.48 secs.
```


Critical path for each request in the DAG is 

preprocessing: ```max(preprocessor, avg_preprocessor) = 0.15 secs```
<br>
model forward: ```max(m1.forward, m2.forward) = 0.3 secs```
<br>
<br>
Total of `0.45` secs.

+++

### Key APIs Explained

The class and function definition as well as decorator didn't diverage from existing serve API. So in the following section we only need to dive into a few new key APIs used: `bind()`, `InputNode()` and `serve.run()`

+++

**```bind(*args, **kwargs)```**

Once called on supported ray decorated function or class (@ray.remote, @serve.deployment), generates an IR of type DAGNode that acts as the building block of graph building.

+++

#### On function with no args

```{code-cell} ipython3
@serve.deployment
def preprocessor():
    print("hello")

# Produces a DeploymentFunctionNode with no bound args. 
func_node = preprocessor.bind()
# All DAGNode types can be printed and shows its bound args as well as DAG structure based on 
print(func_node)
dag_handle = serve.run(func_node)
# Once executed it will execute and print "hello".
print(ray.get(dag_handle.remote()))
```

#### Output

```
(DeploymentFunctionNode)(
    body=<function my_func at 0x7fe8584ccc80>
    args=[]
    kwargs={}
    options={}
    other_args_to_resolve={
        deployment_schema: name='preprocessor' import_path='dummpy.module' init_args=() init_kwargs={} num_replicas=1 route_prefix='/my_func' max_concurrent_queries=100 user_config=None autoscaling_config=None graceful_shutdown_wait_loop_s=2.0 graceful_shutdown_timeout_s=20.0 health_check_period_s=10.0 health_check_timeout_s=30.0 ray_actor_options=None
        is_from_serve_deployment: True
    }
)
```

```
(my_func pid=15598) hello
```

+++

#### On function with args

```{code-cell} ipython3
@serve.deployment
def preprocessor(val):
    print(val)

# Produces a DeploymentFunctionNode with no bound args.
func_node = preprocessor.bind()

dag_handle = serve.run(func_node)
# Once executed it will execute with arg value of 2.
print(ray.get(dag_handle.remote(2)))
```

#### Output

```
(my_func pid=17060) 2
```

+++

#### On class constructor 

```Class.bind(*args, **kwargs)``` constructs and returns a DAGNode that acts as the instantiated instance of Class, where ```*args``` and ```**kwargs``` are used as init args.

#### On class method

Once a class is bound with its init args, its class methods can be directly accessed, called or bound with other args.

```{code-cell} ipython3
@serve.deployment
class Model:
    def __init__(self, val):
        self.val = val
    def get(self):
        return self.val

# Produces a deployment class node instance initialized with bound args value. 
class_node = Model.bind(5)
print(class_node)
dag_handle = serve.run(class_node)
# Access get() class method on bound Model
print(ray.get(dag_handle.get.remote()))
```

#### Output
```
(DeploymentNode)(
    body=<class '__main__.MyClass'>
    args=[
        5, 
    ]
    kwargs={}
    options={}
    other_args_to_resolve={
        deployment_schema: name='Model' import_path='dummpy.module' init_args=() init_kwargs={} num_replicas=1 route_prefix='/MyClass' max_concurrent_queries=100 user_config=None autoscaling_config=None graceful_shutdown_wait_loop_s=2.0 graceful_shutdown_timeout_s=20.0 health_check_period_s=10.0 health_check_timeout_s=30.0 ray_actor_options=None
        is_from_serve_deployment: True
    }
)
```

get() returns

```
5
```

+++

#### DAGNode as args in bind()

DAGNode can also be passed into other DAGNode in dag binding. In the example above, ```Combiner``` calls into two instantiations of ```Model``` class, which can be bound and passed into ```Combiner```'s constructor as if we're passing in two regular python class instances.

```
m1 = Model.bind(1)
m2 = Model.bind(2)
combiner = Combiner.bind(m1, m2)
```

Similarly, we can also pass and bind upstream DAGNode results that will be resolved upon runtime to downstream DAGNodes, in our example, a `DeploymentMethodNode` that access class method of ```Combiner``` class takes two preprocessing DAGNodes' output as well as part of user input.

```
preprocessed_1 = preprocessor.bind(dag_input[0])
preprocessed_2 = avg_preprocessor.bind(dag_input[1])
...
dag = combiner.run.bind(preprocessed_1, preprocessed_2, dag_input[2])
```

+++

**```InputNode()```**

```InputNode``` is a special singleton in DAG building that's only relevant to it's runtime call behavior. Even though all decorated classes or functions can be reused in arbitrary way to facilitate DAG building where the root DAGNode forms the graph with its children, in each deployment graph there should be one and only one InputNode used.

```InputNode``` value is fulfilled and replaced by user input at runtime, therefore it takes no argument when being constructed.

It's possible to access partial user input by index or key, if some DAGNode in the graph doesn't need the complete user input to run. In the example above, `combiner.run` only needs the element at index 2 to determine it's runtime behavior.

```
dag = combiner.run.bind(preprocessed_1, preprocessed_2, dag_input[2])
```

+++

### Running Deployment Graph

The deployment graph can be deployed with ```serve.run()```. ```serve.run()```
takes in a target DeploymentNode, and it deploys the node's deployments, as
well as all its child nodes' deployments. To deploy your graph, pass in the
driver DeploymentNode into ```serve.run()```.

Tip: You can also use the Serve CLI to run your deployment graph. The CLI was
included with Serve when you did ``pip install "ray[serve]"``. The command
```serve run [node import path]``` will deploy the node and its childrens'
deployments.

+++

### Future improvements

Polishing 

Operationalizing deployment graph

Performance improvements:

```{code-cell} ipython3

```
