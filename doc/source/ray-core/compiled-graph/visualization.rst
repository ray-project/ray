Visualization
=============

Ray Compiled Graph provides visualization functionalities to help users with development and debugging.

Graph structure
---------------

To visualize the graph structure, call the ``visualize`` method after calling ``experimental_compile``
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
Note that this requires ``graphviz`` to be installed.

The visualization for the preceding code is shown below:

.. image:: ../../images/compiled_graph.png
    :alt: Visualization of Graph Structure
    :align: center

Note that tasks of the same actor are shown in the same color.

Execution Schedule
------------------

Compiled Graph generates an execution schedule for each of the participating actors. This is very useful information
to understand how the graph is executed at runtime.

To visualize the execution schedule, set the environment variable ``RAY_CGRAPH_VISUALIZE_SCHEDULE=1``
when running the script. For example, save the preceding code in a script named ``example.py``,
and run the following command:

.. testcode::

    RAY_CGRAPH_VISUALIZE_SCHEDULE=1 python3 example.py

When ``experimental_compile`` is executed, Ray generates a PNG image named ``compiled_graph_schedule.png`` and
saves it in the current working directory. The visualization for the preceding code is shown below:

.. image:: ../../images/compiled_graph_schedule.png
    :alt: Visualization of Execution Schedule
    :align: center
