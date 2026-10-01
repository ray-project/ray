---
myst:
  html_meta:
    description: "Build lazy computation graphs with the Ray DAG API over functions, classes, and actor methods, using InputNode and MultiOutputNode."
---

(ray-dag-guide)=

# Lazy computation graphs with the Ray DAG API

With `ray.remote`, your application runs its computation remotely at runtime. On a class or function decorated with `ray.remote`, you can also call `.bind` to build a static computation graph.

:::{note}
Ray DAG is a developer-facing API with two recommended use cases:

- Iterate locally on and test applications that you author with higher-level libraries.
- Build libraries on top of the Ray DAG API.
:::


:::{note}
Ray has introduced an experimental API for high-performance workloads that is especially well suited for applications using multiple GPUs. This API is built on top of the Ray DAG API.

See {ref}`Ray Compiled Graph <ray-compiled-graph>` for more details.
:::


Calling `.bind()` on a class or function decorated with `ray.remote` generates an intermediate representation (IR) node. IR nodes are the backbone and building blocks of the DAG, and they statically hold the computation graph together. At execution time, each IR node resolves to a value in topological order.

You can also assign an IR node to a variable and pass it to other nodes as an argument.

## Ray DAG with functions

When you execute it, an IR node that `.bind()` generates from a `ray.remote` decorated function runs as a Ray task and resolves to the task's output.

The following example builds a chain of functions. While you iterate, you can execute each node as the root node, or pass it to other functions as a positional or keyword argument to form more complex DAGs.

To execute any IR node directly as the root of the DAG, call `dag_node.execute()`. Execution ignores every node that isn't reachable from the root.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ./doc_code/ray-dag.py
:language: python
:start-after: __dag_tasks_begin__
:end-before: __dag_tasks_end__
```
:::
::::


## Ray DAG with classes and class methods

When you execute it, an IR node that `.bind()` generates from a `ray.remote` decorated class runs as a Ray actor. Ray instantiates the actor every time you execute the node. Calls to the class methods can form a chain of function calls specific to the parent actor instance.

You can combine IR nodes generated from functions, classes, and class methods to form a DAG.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ./doc_code/ray-dag.py
:language: python
:start-after: __dag_actors_begin__
:end-before: __dag_actors_end__
```
:::
::::



## Ray DAG with custom InputNode

`InputNode` is the singleton node of a DAG that represents the input value at runtime. Use it as a context manager with no arguments, and supply its value as the arguments of `dag_node.execute()`.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ./doc_code/ray-dag.py
:language: python
:start-after: __dag_input_node_begin__
:end-before: __dag_input_node_end__
```
:::
::::

(ray-dag-with-multiple-multioutputnode)=

## Ray DAG with MultiOutputNode

Use `MultiOutputNode` when a DAG has more than one output. `dag_node.execute()` returns a list of the object refs passed to `MultiOutputNode`. The following example shows a `MultiOutputNode` with two outputs.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ./doc_code/ray-dag.py
:language: python
:start-after: __dag_multi_output_node_begin__
:end-before: __dag_multi_output_node_end__
```
:::
::::

## Reuse Ray actors in DAGs

You can make actors part of the DAG definition with the `Actor.bind()` API. However, when a DAG finishes execution, Ray kills actors created with `bind`.

To keep your actors alive after the DAG finishes, create them with `Actor.remote()` instead.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ./doc_code/ray-dag.py
:language: python
:start-after: __dag_actor_reuse_begin__
:end-before: __dag_actor_reuse_end__
```
:::
::::


## More resources

For more application patterns and examples, see the following resources from other Ray libraries built on top of the Ray DAG API:

- [Ray Serve: Deploy compositions of models](https://docs.ray.io/en/master/serve/model_composition.html)
- [Visualization of Ray Compiled Graph](https://docs.ray.io/en/latest/ray-core/compiled-graph/profiling.html#visualization)
