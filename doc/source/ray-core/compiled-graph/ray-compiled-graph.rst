.. _ray-compiled-graph:

Ray Compiled Graph (beta)
=========================

.. warning::

    Ray Compiled Graph is currently in beta (since Ray 2.44). The APIs are subject to change and expected to evolve.
    The API is available from Ray 2.32, but it's recommended to use a version after 2.44.

As large language models (LLMs) become common, programming distributed systems with multiple GPUs is essential.
:ref:`Ray Core APIs <core-key-concepts>` facilitate using multiple GPUs but have limitations such as:

* System overhead of ~1 ms per task launch, which is unsuitable for high-performance tasks like LLM inference.
* Lack support for direct GPU-to-GPU communication, requiring manual development with external libraries like NVIDIA Collective Communications Library (`NCCL <https://developer.nvidia.com/nccl>`_).

Ray Compiled Graph gives you a Ray Core-like API but with:

- **Less than 50us system overhead** for workloads that repeatedly execute the same task graph.
- **Native support for GPU-GPU communication** with NCCL.

For example, consider the following Ray Core code, which sends data to an actor
and gets the result:

.. testcode::

    # Ray Core API for remote execution.
    # ~1ms overhead to invoke `recv`.
    ref = receiver.recv.remote(data)
    ray.get(ref)


This code shows how to compile and execute the same example as a Compiled Graph.

.. testcode::

    # Compiled Graph for remote execution.
    # less than 50us overhead to invoke `recv` (during `graph.execute(data)`).
    with InputNode() as inp:
        graph = receiver.recv.bind(inp)

    graph = graph.experimental_compile()
    ref = graph.execute(data)
    ray.get(ref)

Ray Compiled Graph has a static execution model. It's different from classic Ray APIs, which are eager. Because
of the static nature, Ray Compiled Graph can perform various optimizations such as:

- Pre-allocate resources so that it can reduce system overhead.
- Prepare NCCL communicators and apply deadlock-free scheduling.
- (experimental) Automatically overlap GPU compute and communication.
- Improve multi-node performance.

Use Cases
---------
Ray Compiled Graph APIs simplify development of high-performance multi-GPU workloads such as LLM inference or distributed training that require:

- Sub-millisecond level task orchestration.
- Direct GPU-GPU peer-to-peer or collective communication.
- `Heterogeneous <https://www.youtube.com/watch?v=Mg08QTBILWU>`_ or MPMD (Multiple Program Multiple Data) execution.

More Resources
--------------

- `Ray Compiled Graph blog <https://www.anyscale.com/blog/announcing-compiled-graphs>`_
- `Ray Compiled Graph talk at Ray Summit <https://www.youtube.com/watch?v=jv58Cpr6SAs>`_
- `Heterogenous training with Ray Compiled Graph <https://www.youtube.com/watch?v=Mg08QTBILWU>`_
- `Distributed LLM inference with Ray Compiled Graph <https://www.youtube.com/watch?v=oMb_WiUwf5o>`_

Table of Contents
-----------------

Learn more details about Ray Compiled Graph from the following links.

.. toctree::
    :maxdepth: 1

    quickstart
    profiling
    overlap
    troubleshooting
    compiled-graph-api
