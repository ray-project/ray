Profiling
=========

Ray Compiled Graph provides both PyTorch-based and Nsight-based profiling functionalities to better understand the performance
of individual tasks, systems overhead, and performance bottlenecks. You can pick your favorite profiler based on your preference.

PyTorch profiler
----------------

To run PyTorch Profiling on Compiled Graph, simply set the environment variable ``RAY_CGRAPH_ENABLE_TORCH_PROFILING=1``
when running the script. For example, for a Compiled Graph script in ``example.py``, run the following command:

.. testcode::

    RAY_CGRAPH_ENABLE_TORCH_PROFILING=1 python3 example.py

After execution, Compiled Graph generates the profiling results in the `compiled_graph_torch_profiles` directory
under the current working directory. Compiled Graph generates one trace file per actor.

Traces can be visualized using https://ui.perfetto.dev/.


Nsight system profiler
----------------------

Compiled Graph builds on top of Ray's profiling capabilities, and leverages Nsight
system profiling. 

To run Nsight Profiling on Compiled Graph, specify the runtime_env for the involved actors
as described in :ref:`Run Nsight on Ray <run-nsight-on-ray>`. For example,

.. testcode::
    import ray
    import torch
    from ray.dag import InputNode

    @ray.remote(num_gpus=1, runtime_env={"nsight": "default"})
    class RayActor:
        def send(self, shape, dtype, value: int):
            return torch.ones(shape, dtype=dtype, device=self.device) * value

        def recv(self, tensor):
            return (tensor[0].item(), tensor.shape, tensor.dtype)

    sender = RayActor.remote()
    receiver = RayActor.remote()

Then, create a Compiled Graph as usual.

.. testcode::

    shape = (10,)
    dtype = torch.float16

    # Test normal execution.
    with InputNode() as inp:
        dag = sender.send.bind(inp.shape, inp.dtype, inp[0])
        dag = dag.with_tensor_transport(transport="nccl")
        dag = receiver.recv.bind(dag)

    compiled_dag = dag.experimental_compile()

    for i in range(3):
        shape = (10 * (i + 1),)
        ref = compiled_dag.execute(i, shape=shape, dtype=dtype)
        assert ray.get(ref) == (i, shape, dtype)

Finally, run the script as usual.

.. testcode::

    python3 example.py

After execution, Compiled Graph generates the profiling results under the `/tmp/ray/session_*/logs/{profiler_name}`
directory.

For fine-grained performance analysis of method calls and system overhead, set the environment variable
``RAY_CGRAPH_ENABLE_NVTX_PROFILING=1`` when running the script:

.. testcode::

    RAY_CGRAPH_ENABLE_NVTX_PROFILING=1 python3 example.py


This command leverages the `NVTX library <https://nvtx.readthedocs.io/en/latest/index.html#>`_ under the hood to automatically
annotate all methods called in the execution loops of compiled graph.

To visualize the profiling results, follow the same instructions as described in 
:ref:`Nsight Profiling Result <profiling-result>`.

Visualization
-------------
To visualize the graph structure, call the :func:`visualize <ray.dag.compiled_dag_node.CompiledDAG.visualize>` method after calling :func:`experimental_compile <ray.dag.DAGNode.experimental_compile>`
on the graph.

.. testcode::

    import ray
    from ray.dag import InputNode, MultiOutputNode

    @ray.remote
    class Worker:
        def inc(self, x):
            return x + 1

        def double(self, x):
            return x * 2

        def echo(self, x):
            return x

    sender1 = Worker.remote()
    sender2 = Worker.remote()
    receiver = Worker.remote()

    with InputNode() as inp:
        w1 = sender1.inc.bind(inp)
        w1 = receiver.echo.bind(w1)
        w2 = sender2.double.bind(inp)
        w2 = receiver.echo.bind(w2)
        dag = MultiOutputNode([w1, w2])

    compiled_dag = dag.experimental_compile()
    compiled_dag.visualize()

By default, Ray generates a PNG image named ``compiled_graph.png`` and saves it in the current working directory.
Note that this requires ``graphviz``.

The visualization for the preceding code is shown below.
Tasks of the same actor are shown in the same color.

.. image:: ../../images/compiled_graph_viz.png
    :alt: Visualization of Graph Structure
    :align: center


