---
myst:
  html_meta:
    description: "First Ray Compiled Graph program: declare data dependencies, use asyncio, set execution timeouts, and move tensors between CPU and GPU."
---

# Quickstart

## Hello World
This "hello world" example uses Ray Compiled Graph. First, install Ray.

```bash
pip install "ray[cgraph]"

# For a ray version before 2.41, use the following instead:
# pip install "ray[adag]"
```


Next, define a simple actor that echoes its argument.

```{literalinclude} ../doc_code/cgraph_quickstart.py
:language: python
:start-after: __simple_actor_start__
:end-before: __simple_actor_end__
```

Then instantiate the actor and use the classic Ray Core APIs `remote` and `ray.get` to execute tasks on the actor.

```{literalinclude} ../doc_code/cgraph_quickstart.py
:language: python
:start-after: __ray_core_usage_start__
:end-before: __ray_core_usage_end__
```

```
Execution takes 969.0364822745323 us
```

Next, create an equivalent program with Ray Compiled Graph. First, define a graph and execute it with classic Ray Core, without any compilation. Then compile the graph to apply optimizations and prevent further changes to it.

First, create a {ref}`Ray DAG <ray-dag-guide>`, which is a lazily executed directed acyclic graph of Ray tasks. A Ray DAG differs from the classic Ray Core APIs in three ways:

1. Use the {class}`ray.dag.InputNode <ray.dag.input_node.InputNode>` context manager to indicate which DAG inputs you provide at run time.
1. Use {func}`bind() <ray.actor.ActorMethod.bind>` instead of {func}`remote() <ray.remote>` to indicate lazily executed Ray tasks.
1. Use {func}`execute() <ray.dag.compiled_dag_node.CompiledDAG.execute>` to execute the DAG.

Define a graph and execute it. This code doesn't compile the graph, and it uses the same execution backend as the preceding example:

```{literalinclude} ../doc_code/cgraph_quickstart.py
:language: python
:start-after: __dag_usage_start__
:end-before: __dag_usage_end__
```

Next, compile the `dag` using the {func}`experimental_compile <ray.dag.DAGNode.experimental_compile>` API. The graph uses the same APIs for execution:

```{literalinclude} ../doc_code/cgraph_quickstart.py
:language: python
:start-after: __cgraph_usage_start__
:end-before: __cgraph_usage_end__
```

```
Execution takes 86.72196418046951 us
```

The same task graph runs 10x faster. The improvement is large because the `echo` function is cheap, so system overhead highly affects it. Because of bookkeeping and distributed protocols, the classic Ray Core APIs usually have 1 ms or more of system overhead.

Because the system knows the task graph ahead of time, Ray Compiled Graph can pre-allocate all necessary resources and greatly reduce the system overhead. For example, if the actor `a` is on the same node as the driver, Ray Compiled Graph uses shared memory instead of RPC to transfer data directly between the driver and the actor.

Currently, the DAG tasks run on a background thread of the involved actors. An actor can only participate in one DAG at a time. Normal tasks can still execute on the actors while the actors participate in a Compiled Graph, but these tasks execute on the main thread.

When you're done, tear down the Compiled Graph by deleting it or by calling `dag.teardown()` explicitly. After teardown, you can reuse the actors in a new Compiled Graph.

```{literalinclude} ../doc_code/cgraph_quickstart.py
:language: python
:start-after: __teardown_start__
:end-before: __teardown_end__
```


## Specifying data dependencies

When you create the DAG, pass a `ray.dag.DAGNode` as an argument to other `.bind` calls to specify data dependencies. For example, the following code builds on the preceding example to create a DAG that passes the same message from one actor to another:

```{literalinclude} ../doc_code/cgraph_quickstart.py
:language: python
:start-after: __cgraph_bind_start__
:end-before: __cgraph_bind_end__
```

```
hello
```

The next example passes the same message to both actors, which can then execute in parallel. It uses {class}`ray.dag.MultiOutputNode <ray.dag.output_node.MultiOutputNode>` to indicate that this DAG returns multiple outputs. Then, {func}`dag.execute() <ray.dag.compiled_dag_node.CompiledDAG.execute>` returns multiple {class}`CompiledDAGRef <ray.experimental.compiled_dag_ref.CompiledDAGRef>` objects, one per node:

```{literalinclude} ../doc_code/cgraph_quickstart.py
:language: python
:start-after: __cgraph_multi_output_start__
:end-before: __cgraph_multi_output_end__
```

```
Execution takes 86.72196418046951 us
```


Keep the following execution behavior in mind:

- On the same actor, a Compiled Graph executes in order. If an actor has multiple tasks in the same Compiled Graph, it executes all of them to completion before executing on the next DAG input.
- Across actors in the same Compiled Graph, the execution might be pipelined. An actor might begin executing on the next DAG input while a downstream actor executes on the current one.
- Compiled Graph currently supports only actor tasks. It doesn't support non-actor tasks.

## `asyncio` support

If your Compiled Graph driver runs in an `asyncio` event loop, use the `async` APIs so that executing the Compiled Graph and getting the results don't block the event loop. First, pass `enable_async=True` to `dag.experimental_compile()`:

```{literalinclude} ../doc_code/cgraph_quickstart.py
:language: python
:start-after: __cgraph_async_compile_start__
:end-before: __cgraph_async_compile_end__
```

Next, use `execute_async` to invoke the Compiled Graph. Calling `await` on `execute_async` returns once the input is submitted, and it returns a future that you can use to get the result. Finally, use `await` to get the result of the Compiled Graph.

```{literalinclude} ../doc_code/cgraph_quickstart.py
:language: python
:start-after: __cgraph_async_execute_start__
:end-before: __cgraph_async_execute_end__
```


## Execution and failure semantics
Like classic Ray Core, Ray Compiled Graph propagates exceptions to the final output. It handles application and system exceptions as follows:

- **Application exceptions**: If an application task throws an exception, Compiled Graph wraps the exception in a {class}`RayTaskError <ray.exceptions.RayTaskError>` and raises it when you call {func}`ray.get() <ray.get>` on the result. The thrown exception inherits from both {class}`RayTaskError <ray.exceptions.RayTaskError>` and the original exception class.

- **System exceptions**: System exceptions include actor death or unexpected errors such as network errors. For actor death, Compiled Graph raises an {class}`ActorDiedError <ray.exceptions.ActorDiedError>`, and for other errors, it raises a {class}`RayChannelError <ray.exceptions.RayChannelError>`.

The graph can still execute after application exceptions. However, the graph shuts down automatically after a system exception. If an actor's death causes the graph to shut down, the remaining actors stay alive.

The following example explicitly destroys an actor while it participates in a Compiled Graph. The remaining actors are reusable:

```{literalinclude} ../doc_code/cgraph_quickstart.py
:language: python
:start-after: __cgraph_actor_death_start__
:end-before: __cgraph_actor_death_end__
```


## Execution timeouts

Some errors, such as NCCL network errors, require additional handling to avoid hanging. Ray might detect such errors in the future. As a fallback, you can configure timeouts for {func}`compiled_dag.execute() <ray.dag.compiled_dag_node.CompiledDAG.execute>` and {func}`ray.get() <ray.get>`.

The default timeout is 10 seconds for both. Set the following two environment variables to change the default timeout:

- `RAY_CGRAPH_submit_timeout`: Timeout for {func}`compiled_dag.execute() <ray.dag.compiled_dag_node.CompiledDAG.execute>`.
- `RAY_CGRAPH_get_timeout`: Timeout for {func}`ray.get() <ray.get>`.

{func}`ray.get() <ray.get>` also has a timeout parameter that sets the timeout for each call.

## CPU to GPU communication

With classic Ray Core, passing `torch.Tensors` between actors can become expensive, especially when transferring between devices, because Ray Core doesn't know the final destination device. As a result, you might see unnecessary copies across devices other than the source and destination devices.

Ray Compiled Graph natively supports passing `torch.Tensors` between actors that execute on different devices. Use type hint annotations in the Compiled Graph declaration to indicate the final destination device of a `torch.Tensor`.

```{literalinclude} ../doc_code/cgraph_nccl.py
:language: python
:start-after: __cgraph_cpu_to_gpu_actor_start__
:end-before: __cgraph_cpu_to_gpu_actor_end__
```

In Ray Core, if you try to pass a CPU tensor from the driver, the GPU actor receives a CPU tensor:

```{testcode}
:skipif: True

# This will fail because the driver passes a CPU copy of the tensor,
# and the GPU actor also receives a CPU copy.
ray.get(actor.process.remote(torch.zeros(10)))
```

With Ray Compiled Graph, you can annotate DAG nodes with type hints to indicate that the value might contain a `torch.Tensor`:

```{literalinclude} ../doc_code/cgraph_nccl.py
:language: python
:start-after: __cgraph_cpu_to_gpu_start__
:end-before: __cgraph_cpu_to_gpu_end__
```

The Ray Compiled Graph backend copies the `torch.Tensor` to the GPU that Ray Core assigns to the `GPUActor`.

You can also copy the tensor yourself, but Compiled Graph has the following advantages:

- Ray Compiled Graph can minimize the number of data copies. For example, passing from one CPU to multiple GPUs requires one copy to a shared memory buffer, and then one host-to-device copy per destination GPU.

- In the future, Ray can further optimize this path through techniques such as [memory pinning](https://docs.pytorch.org/docs/stable/generated/torch.Tensor.pin_memory.html) and zero-copy deserialization when the CPU is the destination.


## GPU to GPU communication
Ray Compiled Graph supports NCCL-based transfers of CUDA `torch.Tensor` objects, avoiding any copies through Ray's CPU-based shared-memory object store. When you provide type hints, Ray prepares NCCL communicators and operation scheduling ahead of time. This avoids deadlock and supports {ref}`overlapping compute and communication <compiled-graph-overlap>`.

Ray Compiled Graph uses [CuPy](https://cupy.dev/) to support NCCL operations. The CuPy version affects the NCCL version. The Ray team also plans to support custom communicators in the future, for example to support collectives across CPUs or to reuse existing collective groups.

First, create sender and receiver actors. This example requires at least two GPUs.

```{literalinclude} ../doc_code/cgraph_nccl.py
:language: python
:start-after: __cgraph_nccl_setup_start__
:end-before: __cgraph_nccl_setup_end__
```

To support GPU-to-GPU communication with NCCL, wrap the DAG node that contains the `torch.Tensor` that you want to transmit using the `with_tensor_transport` API hint:

```{literalinclude} ../doc_code/cgraph_nccl.py
:language: python
:start-after: __cgraph_nccl_exec_start__
:end-before: __cgraph_nccl_exec_end__
```

GPU-to-GPU communication currently has the following limitations:

- It supports only `torch.Tensor` and NVIDIA NCCL.
- It supports peer-to-peer transfers. Collective communication operations are coming soon.
- Communication operations run synchronously. {ref}`Overlapping compute and communication <compiled-graph-overlap>` is an experimental feature.
