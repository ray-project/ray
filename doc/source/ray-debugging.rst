Ray Debugger
=============

Ray has a built in debugger that allows you to debug your distributed applications. It allows
to set breakpoints in your Ray tasks and actors and when hitting the breakpoint you can
drop into a PDB session that you can then use to:

- Inspect variables in that context
- Step within that task or actor
- Move up or down the stack

.. note::

    It is currently an experimental feature and under active development. Interfaces are subject to change.

Getting Started
---------------

.. note::

    On Python 3.6, the ``breakpoint()`` function is not supported and you need to use
    ``ray.util.pdb.set_trace()`` instead.

Take the following example:

.. code-block:: python

    import ray
    ray.init()

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

.. code-block:: python

    import ray

    ray.init()

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

.. code-block:: python

    Enter breakpoint index or press enter to refresh: 0
    > /home/ubuntu/tmp/stepping.py(16)<module>()
    -> result_ref = fact.remote(5)
    (Pdb)

You can jump into the call with the ``remote`` command in Ray's debugger.
Inside the function, print the value of `n` with ``p(n)``, resulting in
the following output:

.. code-block:: python

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
``get`` debugger comand. Use ``get`` again to jump back to the original
call site and use ``p(result)`` to print the result:

.. code-block:: python

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

We will show how this works using a Ray serve application. Copy the following code into a file called
``serve_debugging.py``:

.. code-block:: python

    import time

    from sklearn.datasets import load_iris
    from sklearn.ensemble import GradientBoostingClassifier

    import ray
    from ray import serve

    serve.start()

    # Train model
    iris_dataset = load_iris()
    model = GradientBoostingClassifier()
    model.fit(iris_dataset["data"], iris_dataset["target"])

    # Define Ray Serve model,
    @serve.deployment(route_prefix="/iris")
    class BoostingModel:
        def __init__(self):
            self.model = model
            self.label_list = iris_dataset["target_names"].tolist()

        await def __call__(self, starlette_request):
            payload = await starlette_request.json()["vector"]
            print(f"Worker: received request with data: {payload}")

            prediction = self.model.predict([payload])[0]
            human_name = self.label_list[prediction]
            return {"result": human_name}

    # Deploy model
    serve.start()
    BoostingModel.deploy()

    time.sleep(3600.0)

Let's start the program with the post-mortem debugging activated (``RAY_PDB=1``):

.. code-block:: bash

    RAY_PDB=1 python serve_debugging.py

The flag ``RAY_PDB=1`` will have the effect that if an exception happens, Ray will
drop into the debugger instead of propagating it further. Let's see how this works!
First query the model with an invalid request using

.. code-block:: bash

    python -c 'import requests; response = requests.get("http://localhost:8000/iris", json={"vector": [1.2, 1.0, 1.1, "a"]})'

When the ``serve_debugging.py`` driver hits the breakpoint, it will tell you to run
``ray debug``. After we do that, we see an output like the following:

.. code-block:: text

    Active breakpoints:
    index | timestamp           | Ray task                                     | filename:lineno
    0     | 2021-07-13 23:49:14 | ray::RayServeWrappedReplica.handle_request() | /home/ubuntu/ray/python/ray/serve/backend_worker.py:249
    Traceback (most recent call last):

      File "/home/ubuntu/ray/python/ray/serve/backend_worker.py", line 242, in invoke_single
        result = await method_to_call(*args, **kwargs)

      File "serve_debugging.py", line 24, in __call__
        prediction = self.model.predict([payload])[0]

      File "/home/ubuntu/anaconda3/lib/python3.7/site-packages/sklearn/ensemble/_gb.py", line 1188, in predict
        raw_predictions = self.decision_function(X)

      File "/home/ubuntu/anaconda3/lib/python3.7/site-packages/sklearn/ensemble/_gb.py", line 1143, in decision_function
        X = check_array(X, dtype=DTYPE, order="C", accept_sparse='csr')

      File "/home/ubuntu/anaconda3/lib/python3.7/site-packages/sklearn/utils/validation.py", line 63, in inner_f
        return f(*args, **kwargs)

      File "/home/ubuntu/anaconda3/lib/python3.7/site-packages/sklearn/utils/validation.py", line 673, in check_array
        array = np.asarray(array, order=order, dtype=dtype)

      File "/home/ubuntu/anaconda3/lib/python3.7/site-packages/numpy/core/_asarray.py", line 83, in asarray
        return array(a, dtype, copy=False, order=order)

    ValueError: could not convert string to float: 'a'

    Enter breakpoint index or press enter to refresh:

We now press ``0`` and then Enter to enter the debugger. With ``ll`` we can see the context and with
``print(a)`` we an print the array that causes the problem. As we see, it contains a string (``'a'``)
instead of a number as the last element.

In a similar manner as above, you can also debug Ray actors. Happy debugging!

Debugging APIs
--------------

See :ref:`package-ref-debugging-apis`.
