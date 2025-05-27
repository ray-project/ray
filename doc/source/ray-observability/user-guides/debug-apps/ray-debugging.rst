.. _ray-debugger:

Using the Ray Debugger
======================

Ray has a built in debugger that allows you to debug your distributed applications. It allows
to set breakpoints in your Ray tasks and actors and when hitting the breakpoint you can
drop into a PDB session that you can then use to:

- Inspect variables in that context
- Step within that task or actor
- Move up or down the stack

.. warning::

    The Ray Debugger is deprecated. Use the :doc:`Ray Distributed Debugger <../../ray-distributed-debugger>` instead.
    Starting with Ray 2.39, the new debugger is the default and you need to set the environment variable `RAY_DEBUG=legacy` to
    use the old debugger (e.g. by using a runtime environment).

Getting Started
---------------

Take the following example:

.. testcode::
    :skipif: True

    import ray

    ray.init(runtime_env={"env_vars": {"RAY_DEBUG": "legacy"}})

    @ray.remote
    def f(x):
        breakpoint()
        return x * x

    futures = [f.remote(i) for i in range(2)]
    print(ray.get(futures))

Put the program into a file named ``debugging.py`` and execute it using:

.. code-block:: bash

    python debugging.py


Each of the 2 executed tasks will drop into a breakpoint when the line
``breakpoint()`` is executed. You can attach to the debugger by running
the following command on the head node of the cluster:

.. code-block:: bash

    ray debug

The ``ray debug`` command will print an output like this:

.. code-block:: text

    2021-07-13 16:30:40,112	INFO scripts.py:216 -- Connecting to Ray instance at 192.168.2.61:6379.
    2021-07-13 16:30:40,112	INFO worker.py:740 -- Connecting to existing Ray cluster at address: 192.168.2.61:6379
    Active breakpoints:
    index | timestamp           | Ray task | filename:lineno
    0     | 2021-07-13 23:30:37 | ray::f() | debugging.py:6
    1     | 2021-07-13 23:30:37 | ray::f() | debugging.py:6
    Enter breakpoint index or press enter to refresh:


You can now enter ``0`` and hit Enter to jump to the first breakpoint. You will be dropped into PDB
at the break point and can use the ``help`` to see the available actions. Run ``bt`` to see a backtrace
of the execution:

.. code-block:: text

    (Pdb) bt
      /home/ubuntu/ray/python/ray/workers/default_worker.py(170)<module>()
    -> ray.worker.global_worker.main_loop()
      /home/ubuntu/ray/python/ray/worker.py(385)main_loop()
    -> self.core_worker.run_task_loop()
    > /home/ubuntu/tmp/debugging.py(7)f()
    -> return x * x

You can inspect the value of ``x`` with ``print(x)``. You can see the current source code with ``ll``
and change stack frames with ``up`` and ``down``. For now let us continue the execution with ``c``.

After the execution is continued, hit ``Control + D`` to get back to the list of break points. Select
the other break point and hit ``c`` again to continue the execution.

The Ray program ``debugging.py`` now finished and should have printed ``[0, 1]``. Congratulations, you
have finished your first Ray debugging session!

Running on a Cluster
--------------------

The Ray debugger supports setting breakpoints inside of tasks and actors that are running across your
Ray cluster. In order to attach to these from the head node of the cluster using ``ray debug``, you'll
need to make sure to pass in the ``--ray-debugger-external`` flag to ``ray start`` when starting the
cluster (likely in your ``cluster.yaml`` file or k8s Ray cluster spec).

Note that this flag will cause the workers to listen for PDB commands on an external-facing IP address,
so this should *only* be used if your cluster is behind a firewall.

Debugger Commands
-----------------

The Ray debugger supports the
`same commands as PDB
<https://docs.python.org/3/library/pdb.html#debugger-commands>`_.

Stepping between Ray tasks
--------------------------

You can use the debugger to step between Ray tasks. Let's take the
following recursive function as an example:

.. testcode::
    :skipif: True

    import ray

    ray.init(runtime_env={"env_vars": {"RAY_DEBUG": "legacy"}})

    @ray.remote
    def fact(n):
        if n == 1:
            return n
        else:
            n_ref = fact.remote(n - 1)
            return n * ray.get(n_ref)

    @ray.remote
    def compute():
        breakpoint()
        result_ref = fact.remote(5)
        result = ray.get(result_ref)

    ray.get(compute.remote())


After running the program by executing the Python file and calling
``ray debug``, you can select the breakpoint by pressing ``0`` and
enter. This will result in the following output:

.. code-block:: shell

    Enter breakpoint index or press enter to refresh: 0
    > /home/ubuntu/tmp/stepping.py(16)<module>()
    -> result_ref = fact.remote(5)
    (Pdb)

You can jump into the call with the ``remote`` command in Ray's debugger.
Inside the function, print the value of `n` with ``p(n)``, resulting in
the following output:

.. code-block:: shell

    -> result_ref = fact.remote(5)
    (Pdb) remote
    *** Connection closed by remote host ***
    Continuing pdb session in different process...
    --Call--
    > /home/ubuntu/tmp/stepping.py(5)fact()
    -> @ray.remote
    (Pdb) ll
      5  ->	@ray.remote
      6  	def fact(n):
      7  	    if n == 1:
      8  	        return n
      9  	    else:
     10  	        n_ref = fact.remote(n - 1)
     11  	        return n * ray.get(n_ref)
    (Pdb) p(n)
    5
    (Pdb)

Now step into the next remote call again with
``remote`` and print `n`. You an now either continue recursing into
the function by calling ``remote`` a few more times, or you can jump
to the location where ``ray.get`` is called on the result by using the
``get`` debugger command. Use ``get`` again to jump back to the original
call site and use ``p(result)`` to print the result:

.. code-block:: shell

    Enter breakpoint index or press enter to refresh: 0
    > /home/ubuntu/tmp/stepping.py(14)<module>()
    -> result_ref = fact.remote(5)
    (Pdb) remote
    *** Connection closed by remote host ***
    Continuing pdb session in different process...
    --Call--
    > /home/ubuntu/tmp/stepping.py(5)fact()
    -> @ray.remote
    (Pdb) p(n)
    5
    (Pdb) remote
    *** Connection closed by remote host ***
    Continuing pdb session in different process...
    --Call--
    > /home/ubuntu/tmp/stepping.py(5)fact()
    -> @ray.remote
    (Pdb) p(n)
    4
    (Pdb) get
    *** Connection closed by remote host ***
    Continuing pdb session in different process...
    --Return--
    > /home/ubuntu/tmp/stepping.py(5)fact()->120
    -> @ray.remote
    (Pdb) get
    *** Connection closed by remote host ***
    Continuing pdb session in different process...
    --Return--
    > /home/ubuntu/tmp/stepping.py(14)<module>()->None
    -> result_ref = fact.remote(5)
    (Pdb) p(result)
    120
    (Pdb)


Post Mortem Debugging
---------------------

Often we do not know in advance where an error happens, so we cannot set a breakpoint. In these cases,
we can automatically drop into the debugger when an error occurs or an exception is thrown. This is called *post-mortem debugging*.

Copy the following code into a file called ``post_mortem_debugging.py``. The flag ``RAY_DEBUG_POST_MORTEM=1`` will have the effect
that if an exception happens, Ray will drop into the debugger instead of propagating it further.

.. testcode::
    :skipif: True

    import ray

    ray.init(runtime_env={"env_vars": {"RAY_DEBUG": "legacy", "RAY_DEBUG_POST_MORTEM": "1"}})

    @ray.remote
    def post_mortem(x):
        x += 1
        raise Exception("An exception is raised.")
        return x

    ray.get(post_mortem.remote(10))

Let's start the program:

.. code-block:: bash

    python post_mortem_debugging.py

Now run ``ray debug``. After we do that, we see an output like the following:

.. code-block:: text

    Active breakpoints:
    index | timestamp           | Ray task                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | filename:lineno
    0     | 2024-11-01 20:14:00 | /Users/pcmoritz/ray/python/ray/_private/workers/default_worker.py --node-ip-address=127.0.0.1 --node-manager-port=49606 --object-store-name=/tmp/ray/session_2024-11-01_13-13-51_279910_8596/sockets/plasma_store --raylet-name=/tmp/ray/session_2024-11-01_13-13-51_279910_8596/sockets/raylet --redis-address=None --metrics-agent-port=58655 --runtime-env-agent-port=56999 --logging-rotate-bytes=536870912 --logging-rotate-backup-count=5 --runtime-env-agent-port=56999 --gcs-address=127.0.0.1:6379 --session-name=session_2024-11-01_13-13-51_279910_8596 --temp-dir=/tmp/ray --webui=127.0.0.1:8265 --cluster-id=6d341469ae0f85b6c3819168dde27cceda12e95c8efdfc256e0fd8ce --startup-token=12 --worker-launch-time-ms=1730492039955 --node-id=0d43573a606286125da39767a52ce45ad101324c8af02cc25a9fbac7 --runtime-env-hash=-1746935720 | /Users/pcmoritz/ray/python/ray/_private/worker.py:920
    Traceback (most recent call last):

    File "python/ray/_raylet.pyx", line 1856, in ray._raylet.execute_task

    File "python/ray/_raylet.pyx", line 1957, in ray._raylet.execute_task

    File "python/ray/_raylet.pyx", line 1862, in ray._raylet.execute_task

    File "/Users/pcmoritz/ray-debugger-test/post_mortem_debugging.py", line 8, in post_mortem
        raise Exception("An exception is raised.")

    Exception: An exception is raised.

    Enter breakpoint index or press enter to refresh:

We now press ``0`` and then Enter to enter the debugger. With ``ll`` we can see the context and with
``print(x)`` we an print the value of ``x``.

In a similar manner as above, you can also debug Ray actors. Happy debugging!

Debugging APIs
--------------

See :ref:`package-ref-debugging-apis`.
