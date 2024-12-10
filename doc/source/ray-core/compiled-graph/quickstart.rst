Quickstart
==========

Hello World
-----------
This "hello world" example uses Ray Compiled Graph. First, install Ray.

.. testcode::

    pip install "ray[adag]"

We will define a simple actor.

.. testcode::

    import ray
    import time

    @ray.remote
    class SimpleActor:
    def echo(self, msg):
        return msg

Create a very simple actor that directly returns a given input using classic Ray Core APIs, ``remote`` and ``ray.get``.

.. testcode::

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

Create an equivalent program using Ray Compiled Graph. Note 4 key differences with the classic Ray Core APIs.

- Create a static DAG using ``with InputNode() as inp:`` context manager.
- Use ``bind`` instead of ``remote``.
- Use ``experimental_compile`` API for compilation.
- Use ``execute`` to execute the DAG.

Define a graph and compile it using ``experimental_compile`` API.

.. testcode::

    import ray.dag
    with ray.dag.InputNode() as inp:
        # Note that it uses `bind` instead of `remote`.
        dag = a.echo.bind(inp)
    # experimental_compile is the key for optimizing the performance.
    dag = dag.experimental_compile()

Next, execute the DAG and measure the performance.

.. testcode::

    # warmup
    for _ in range(5):
        msg_ref = dag.execute("hello")
        ray.get(msg_ref)
    
    start = time.perf_counter()
    # `dag.execute` runs the DAG and returns a future. You can use `ray.get` API.
    msg_ref = dag.execute("hello")
    ray.get(msg_ref)
    end = time.perf_counter()
    print(f"Execution takes {(end - start) * 1000 * 1000} us")

.. testoutput::

    Execution takes 86.72196418046951 us

The performance of the same DAG improved by 10X. The explanation for this improvement is because the function ``echo`` is cheap and thus highly affected by
the system overhead. Due to various bookeeping and distributed protocols, the classic Ray Core APIs usually have 1ms+ system overhead. 
Because the DAG is known ahead of time, Compiled Graph can pre-allocate all necessary
resources ahead of time and greatly reduce the system overhead.

GPU to GPU communication
------------------------
Consider a very simple GPU to GPU example. With a type hint, Compiled Graph can prepare NCCL communicator and
proper operations ahead of time, avoiding the deadlock and overlapping the compute and communication.

Ray Compiled Graph uses `cupy library <https://cupy.dev/>`_ under the hood to support NCCL operations.
The version of NCCL is affected by the cupy version. The Ray team is also planning to support custom communicator in the future, for example to support collectives across CPUs or to reuse existing collective groups.

Next, create sender and receiver actors.

.. testcode::

    import ray
    import ray.dag
    import torch
    from ray.experimental.channel.torch_tensor_type import TorchTensorType

    ray.init()
    # Note that the following example requires at least 2 GPUs.
    assert ray.available_resources().get("GPU") >= 2, "At least 2 GPUs are required to run this example."

    import torch

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

To support GPU to GPU RDMA with NCCL, you can use ``with_type_hint`` API with Compiled Graph.

.. testcode::

    with ray.dag.InputNode() as inp:
        dag = sender.send.bind(inp)
        # It gives a type hint that the return value of `send` should use
        # NCCL.
        dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
        dag = receiver.recv.bind(dag)

    # Compile API prepares the NCCL communicator across all workers and schedule operations
    # accordingly.
    dag = dag.experimental_compile()
    assert ray.get(dag.execute((10, ))) == (10, )
