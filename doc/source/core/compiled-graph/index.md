---
myst:
  html_meta:
    description: "Ray Compiled Graph (beta) for programming multi-GPU distributed systems with a static execution graph and low per-call overhead."
---

(ray-compiled-graph)=

# Ray Compiled Graph (beta)

:::{warning}
Ray Compiled Graph is currently in beta (since Ray 2.44). The APIs are subject to change and expected to evolve. The API is available from Ray 2.32, but it's recommended to use a version after 2.44.
:::

As large language models (LLMs) become common, programming distributed systems with multiple GPUs is essential. {ref}`Ray Core APIs <core-key-concepts>` facilitate using multiple GPUs but have limitations such as:

* System overhead of about 1 ms per task launch, which is unsuitable for high-performance tasks such as LLM inference.
* Lack of support for direct GPU-to-GPU communication, requiring manual development with external libraries such as NVIDIA Collective Communications Library ([NCCL](https://developer.nvidia.com/nccl)).

Ray Compiled Graph gives you an API similar to Ray Core, with the following advantages:

- **Less than 50 microseconds of system overhead** for workloads that repeatedly execute the same task graph.
- **Native support for GPU-to-GPU communication** with NCCL.

For example, consider the following Ray Core code, which sends data to an actor and gets the result:

```{testcode}
:skipif: True

# Ray Core API for remote execution.
# ~1ms overhead to invoke `recv`.
ref = receiver.recv.remote(data)
ray.get(ref)
```


The following code compiles and executes the same example as a Compiled Graph:

```{testcode}
:skipif: True

# Compiled Graph for remote execution.
# less than 50us overhead to invoke `recv` (during `graph.execute(data)`).
with InputNode() as inp:
    graph = receiver.recv.bind(inp)

graph = graph.experimental_compile()
ref = graph.execute(data)
ray.get(ref)
```

Ray Compiled Graph has a static execution model, while classic Ray APIs are eager. Because its execution model is static, Ray Compiled Graph can perform optimizations such as the following:

- Pre-allocating resources to reduce system overhead.
- Preparing NCCL communicators and applying deadlock-free scheduling.
- Automatically overlapping GPU compute and communication, an experimental feature.
- Improving multi-node performance.

## Use cases

Ray Compiled Graph APIs simplify development of high-performance multi-GPU workloads such as LLM inference or distributed training that require the following:

- Sub-millisecond task orchestration.
- Direct GPU-to-GPU peer-to-peer or collective communication.
- [Heterogeneous](https://www.youtube.com/watch?v=Mg08QTBILWU) or multiple program, multiple data (MPMD) execution.

## More resources

See the following blog post and talks:

- [Ray Compiled Graph blog](https://www.anyscale.com/blog/announcing-compiled-graphs)
- [Ray Compiled Graph talk at Ray Summit](https://www.youtube.com/watch?v=jv58Cpr6SAs)
- [Heterogeneous training with Ray Compiled Graph](https://www.youtube.com/watch?v=Mg08QTBILWU)
- [Distributed LLM inference with Ray Compiled Graph](https://www.youtube.com/watch?v=oMb_WiUwf5o)

## Table of contents

Learn more about Ray Compiled Graph from the following pages.

```{toctree}
:maxdepth: 1

ray-dag
quickstart
profiling
overlap
troubleshooting
../api/compiled-graph
```
