Quickstart
==========

Hello World
-----------
This "hello world" example uses Ray Compiled Graph. First, install Ray.

.. testcode::

    pip install "ray[cgraph]"
    
    # For a ray version before 2.41, use the following instead:
    # pip install "ray[adag]"


First, define a simple actor that echoes its argument.

.. testcode::

    import ray

    @ray.remote
    class SimpleActor:
    def echo(self, msg):
        return msg

Next instantiate the actor and use the classic Ray Core APIs ``remote`` and ``ray.get`` to execute tasks on the actor.

.. testcode::
    import time

    a = SimpleActor.remote()

    # warmup
    for _ in range(5):
        msg_ref = a.echo.remote("hello")
        ray.get(msg_ref)

    start = time.perf_counter()
    msg_ref = a.echo.remote("hello")
    ray.get(msg_ref)
    end = time.perf_counter()
    print(f"Execution takes {(end - start) * 1000 * 1000} us")

.. testoutput::

    Execution takes 969.0364822745323 us

Now, create an equivalent program using Ray Compiled Graph. 
First, define a graph to execute using classic Ray Core, without any compilation.
Later, compile this graph, to apply optimizations and prevent further modifications to the graph.

First, create a :ref:`Ray DAG <ray-dag-guide>` (directed acyclic graph), which is a lazily executed graph of Ray tasks.
Note 3 key differences with the classic Ray Core APIs:

1. Use the :class:`ray.dag.InputNode <ray.dag.input_node.InputNode>` context manager to indicate which inputs to the DAG should be provided at run time.
2. Use :func:`bind() <ray.actor.ActorMethod.bind>` instead of :func:`remote() <ray.remote>` to indicate lazily executed Ray tasks.
3. Use :func:`execute() <ray.dag.compiled_dag_node.CompiledDAG.execute>` to execute the DAG.

Here, define a graph and execute it.
Note that there is **no** compilation happening here. This uses the same execution backend as the preceding example:

.. testcode::

    import ray.dag
    with ray.dag.InputNode() as inp:
        # Note that it uses `bind` instead of `remote`.
        # This returns a ray.dag.DAGNode, instead of the usual ray.ObjectRef.
        dag = a.echo.bind(inp)

    # warmup
    for _ in range(5):
        msg_ref = dag.execute("hello")
        ray.get(msg_ref)

    start = time.perf_counter()
    # `dag.execute` runs the DAG and returns an ObjectRef. You can use `ray.get` API.
    msg_ref = dag.execute("hello")
    ray.get(msg_ref)
    end = time.perf_counter()
    print(f"Execution takes {(end - start) * 1000 * 1000} us")


Next, compile the ``dag`` using the :func:`experimental_compile <ray.dag.DAGNode.experimental_compile>` API.
The graph uses the same APIs for execution:

.. testcode::

    dag = dag.experimental_compile()

    # warmup
    for _ in range(5):
        msg_ref = dag.execute("hello")
        ray.get(msg_ref)

    start = time.perf_counter()
    # `dag.execute` runs the DAG and returns CompiledDAGRef. Similar to
    # ObjectRefs, you can use the ray.get API.
    msg_ref = dag.execute("hello")
    ray.get(msg_ref)
    end = time.perf_counter()
    print(f"Execution takes {(end - start) * 1000 * 1000} us")

.. testoutput::

    Execution takes 86.72196418046951 us

The performance of the same task graph improved by 10X. This is because the function ``echo`` is cheap and thus highly affected by
the system overhead. Due to various bookkeeping and distributed protocols, the classic Ray Core APIs usually have 1 ms+ system overhead.

Because the system knows the task graph ahead of time, Ray Compiled Graphs can pre-allocate all necessary
resources ahead of time and greatly reduce the system overhead.
For example, if the actor ``a`` is on the same node as the driver, Ray Compiled Graphs uses shared memory instead of RPC to transfer data directly between the driver and the actor.

Currently, the DAG tasks run on a **background thread** of the involved actors.
An actor can only participate in one DAG at a time.
Normal tasks can still execute on the actors while the actors participate in a Compiled Graph, but these tasks execute on the main thread.

Once you're done, you can tear down the Compiled Graph by deleting it or explicitly calling ``dag.teardown()``.
This allows reuse of the actors in a new Compiled Graph.

.. testcode::

    dag.teardown()


Specifying data dependencies
----------------------------

When creating the DAG, a ``ray.dag.DAGNode`` can be passed as an argument to other ``.bind`` calls to specify data dependencies.
For example, the following uses the preceding example to create a DAG that passes the same message from one actor to another:

.. testcode::

    import ray.dag

    a = SimpleActor.remote()
    b = SimpleActor.remote()

    with ray.dag.InputNode() as inp:
        # Note that it uses `bind` instead of `remote`.
        # This returns a ray.dag.DAGNode, instead of the usual ray.ObjectRef.
        dag = a.echo.bind(inp)
        dag = b.echo.bind(dag)

    dag = dag.experimental_compile()
    print(ray.get(dag.execute("hello")))

.. testoutput::

    hello

Here is another example that passes the same message to both actors, which can then execute in parallel.
It uses :class:`ray.dag.MultiOutputNode <ray.dag.output_node.MultiOutputNode>` to indicate that this DAG returns multiple outputs.
Then, :func:`dag.execute() <ray.dag.compiled_dag_node.CompiledDAG.execute>` returns multiple :class:`CompiledDAGRef <ray.experimental.compiled_dag_ref.CompiledDAGRef>` objects, one per node:


.. testcode::

    import ray.dag

    a = SimpleActor.remote()
    b = SimpleActor.remote()

    with ray.dag.InputNode() as inp:
        # Note that it uses `bind` instead of `remote`.
        # This returns a ray.dag.DAGNode, instead of the usual ray.ObjectRef.
        dag = ray.dag.MultiOutputNode([a.echo.bind(inp), b.echo.bind(inp)])

    dag = dag.experimental_compile()
    print(ray.get(dag.execute("hello")))

.. testoutput::

    ["hello", "hello"]

Be aware that:
* On the same actor, a Compiled Graph executes in order. If an actor has multiple tasks in the same Compiled Graph, it executes all of them to completion before executing on the next DAG input.
* Across actors in the same Compiled Graph, the execution may be pipelined. An actor may begin executing on the next DAG input while a downstream actor executes on the current one.
* Compiled Graphs currently only supports actor tasks. Non-actor tasks aren't supported.

``asyncio`` support
-------------------

.. warning::

    Under construction.

Execution and failure semantics
-------------------------------
Like classic Ray Core, Ray Compiled Graph propagates exceptions to the final output.
In particular:

- **Application exceptions**: If an application task throws an exception, Compiled Graph
  wraps the exception in a :class:`RayTaskError <ray.exceptions.RayTaskError>` and
  raises it when the caller calls :func:`ray.get() <ray.get>` on the result. The thrown
  exception inherits from both :class:`RayTaskError <ray.exceptions.RayTaskError>`
  and the original exception class.

- **System exceptions**: System exceptions include actor death or unexpected errors
  such as network errors. For actor death, Compiled Graph raises a
  :class:`ActorDiedError <ray.exceptions.ActorDiedError>`, and for other errors, it
  raises a :class:`RayChannelError <ray.exceptions.RayChannelError>`.

The graph can still execute after application exceptions. However, the graph
automatically shuts down in the case of system exceptions. If an actor's death causes
the graph to shut down, the remaining actors stay alive.

For example, this example explicitly destroys an actor while it's participating in a Compiled Graph.
The remaining actors are reusable:

.. testcode::

    import ray
    from ray.dag import InputNode, MultiOutputNode

    @ray.remote
    class EchoActor:
    def echo(self, msg):
        return msg

    actors = [EchoActor.remote() for _ in range(4)]
    with InputNode() as inp:
        outputs = [actor.echo.bind(inp) for actor in actors]
        dag = MultiOutputNode(outputs)

    compiled_dag = dag.experimental_compile()
    # Kill one of the actors to simulate unexpected actor death.
    ray.kill(actors[0])
    ref = compiled_dag.execute(1)

    live_actors = []
    try:
        ray.get(ref)
    except ray.exceptions.ActorDiedError:
        # At this point, the Compiled Graph is shutting down.
        for actor in actors:
            try:
                # Check for live actors.
                ray.get(actor.echo.remote("ping"))
                live_actors.append(actor)
            except ray.exceptions.RayActorError:
                pass

    # Optionally, use the live actors to create a new Compiled Graph.
    assert live_actors == actors[1:]

Execution Timeouts
------------------

Some errors, such as NCCL network errors, require additional handling to avoid hanging.
In the future, Ray may attempt to detect such errors, but currently as a fallback, it allows 
configurable timeouts for
:func:`compiled_dag.execute() <ray.dag.compiled_dag_node.CompiledDAG.execute>` and :func:`ray.get() <ray.get>`.

The default timeout is 10 seconds for both. Set the following environment variables
to change the default timeout:

- ``RAY_CGRAPH_submit_timeout``: Timeout for :func:`compiled_dag.execute() <ray.dag.compiled_dag_node.CompiledDAG.execute>`.
- ``RAY_CGRAPH_get_timeout``: Timeout for :func:`ray.get() <ray.get>`.

:func:`ray.get() <ray.get>` also has a timeout parameter to set timeout on a per-call basis.

GPU to GPU communication
------------------------
Ray Compiled Graphs supports NCCL-based transfers of CUDA ``torch.Tensor`` objects, avoiding any copies through Ray's CPU-based shared-memory object store.
With user-provided type hints, Ray prepares NCCL communicators and
operation scheduling ahead of time, avoiding deadlock and `overlapping compute and communication <compiled-graph-overlap>`.

Ray Compiled Graph uses `cupy <https://cupy.dev/>`_ under the hood to support NCCL operations.
The cupy version affects the NCCL version. The Ray team is also planning to support custom communicators in the future, for example to support collectives across CPUs or to reuse existing collective groups.

First, create sender and receiver actors. Note that this example requires at least 2 GPUs.

.. testcode::

    import ray
    import ray.dag
    import torch
    from ray.experimental.channel.torch_tensor_type import TorchTensorType

    ray.init()
    # Note that the following example requires at least 2 GPUs.
    assert ray.available_resources().get("GPU") >= 2, "At least 2 GPUs are required to run this example."

    @ray.remote(num_gpus=1)
    class GPUSender:
        def send(self, shape):
            return torch.zeros(shape, device="cuda")

    @ray.remote(num_gpus=1)
    class GPUReceiver:
        def recv(self, tensor: torch.Tensor):
            assert tensor.device.type == "cuda"
            return tensor.shape

    sender = GPUSender.remote()
    receiver = GPUReceiver.remote()

To support GPU-to-GPU communication with NCCL, wrap the DAG node that contains the ``torch.Tensor`` that you want to transmit using the ``with_tensor_transport`` API hint:

.. testcode::

    with ray.dag.InputNode() as inp:
        dag = sender.send.bind(inp)
        # Add a type hint that the return value of `send` should use NCCL.
        dag = dag.with_tensor_transport("nccl")
        # NOTE: With ray<2.42, use `with_type_hint()` instead.
        # dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = receiver.recv.bind(dag)

    # Compile API prepares the NCCL communicator across all workers and schedule operations
    # accordingly.
    dag = dag.experimental_compile()
    assert ray.get(dag.execute((10, ))) == (10, )

Current limitations include:
* ``torch.Tensor`` and NVIDIA NCCL only
* Support for peer-to-peer transfers. Collective communication operations are coming soon.
* Communication operations are currently done synchronously. :ref:`Overlapping compute and communication <compiled-graph-overlap>` is an experimental feature.
