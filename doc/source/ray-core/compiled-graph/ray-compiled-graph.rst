Ray Compiled Graph
==================

.. warning::

    Ray Compiled Graph is currently at a developer preview stage. The APIs are subject to change and expected to evolve.
    The API is available from Ray 2.32.

As large language models (LLM) become common, programming distributed systems with multiple GPUs is essential.
Ray APIs facilitate using multiple GPUs but have limitations such as: 

* having a high system overhead of over 1ms per task launch, which is unsuitable for high-performance tasks like LLM inference
* lack direct GPU-to-GPU RDMA communication, requiring external tools like NCCL. 

Ray Compiled Graph gives you a classic Ray Core-like API but with:

- **Less than 50us system overhead** for workloads that repeatedly execute the same task DAG
- **Native support for GPU-GPU communication**, with NCCL with various optimizations such as overlapping compute and communication.

.. testcode::

    # Ray Core API for remote execution.
    # ~1ms overhead to invoke `recv`.
    ref = receiver.recv.remote(data)
    ray.get(ref)

    # Compiled Graph for remote execution.
    # less than 50us overhead to invoke `recv` (during `dag.execute(data)`).
    with InputNode() as inp:
        dag = receiver.recv.bind(inp)

    dag = dag.experimental_compile()
    ref = dag.execute(data)
    ray.get(ref)

Ray Compiled Graph has a static execution model. It is different from classic Ray APIs, which are eager. Because
of the static nature, Ray Compiled Graph can perform various optimizations such as

- Pre-allocate resources so that it can reduce system overhead.
- Prepare NCCL communicators and schedule it in a way that avoids deadlock.
- Automatically overlap GPU compute and communication.
- Improve multi-node performance.

Use Cases
---------
Ray Compiled Graph APIs simplify high-performance multi-GPU workloads such as:

- Applications with sub-millisecond level runtime requirements (which is highly affected by 1ms+ system overhead). For example, LLM inference.
- Multi GPU applications such as LLM inference or training.
- Multi GPU applications with heterogenous compute patterns. See `Heterogenous training with Ray Compiled Graph <https://www.youtube.com/watch?v=Mg08QTBILWU>`_ for more details.

More Resources
--------------

- `Ray Compiled Graph blog <https://www.anyscale.com/blog/announcing-compiled-graphs>`_
- `Ray Compiled Graph talk at Ray Summit <https://www.youtube.com/watch?v=jv58Cpr6SAs&t=667s>`_
- `Heterogenous training with Ray Compiled Graph <https://www.youtube.com/watch?v=Mg08QTBILWU>`_
- `Distributed LLM inference with Ray Compiled Graph <https://www.youtube.com/watch?v=oMb_WiUwf5o>`_

Table of Contents
-----------------

Learn more details about Ray Compiled Graph from the following links.

.. toctree::
    :maxdepth: 1

    quickstart
