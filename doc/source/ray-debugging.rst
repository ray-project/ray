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

Take the following example:

.. code-block:: python

    import ray
    ray.init()

    @ray.remote
    def f(x):
        ray.util.pdb.set_trace()
        return x * x

    futures = [f.remote(i) for i in range(2)]
    print(ray.get(futures))

Put the program into a file named ``debugging.py`` and execute it using:

.. code-block:: bash

    python debugging.py


Each of the 4 executed tasks will drop into a breakpoint when the line
``ray.util.pdb.set_trace()`` is executed. You can attach to the debugger by running
the following command on the head node of the cluster:

.. code-block:: bash

    ray debug

The ``ray debug`` command will print an output like this:

.. code-block:: text

    2020-11-04 15:35:50,011	INFO worker.py:672 -- Connecting to existing Ray cluster at address: 192.168.1.105:6379
    Active breakpoints:
    0: ray::f() | debugging.py:6

    1: ray::f() | debugging.py:6

    Enter breakpoint index or press enter to refresh:


You can now enter ``0`` and hit Enter to jump to the first breakpoint. You will be dropped into PDB
at the break point and can use the ``help`` to see the available actions. Run ``bt`` to see a backtrace
of the execution:

.. code-block:: text

    (Pdb) bt
      /Users/pcmoritz/ray/python/ray/workers/default_worker.py(170)<module>()
    -> ray.worker.global_worker.main_loop()
      /Users/pcmoritz/ray/python/ray/worker.py(385)main_loop()
    -> self.core_worker.run_task_loop()
    > /Users/pcmoritz/tmp/debugging.py(7)f()
    -> return x * x

You can inspect the value of ``x`` with ``print(x)``. You can see the current source code with ``ll``
and change stack frames with ``up`` and ``down``. For now let us continue the execution with ``c``.

After the execution is continued, hit ``Control + D`` to get back to the list of break points. Select
the other break point and hit ``c`` again to continue the execution.

The Ray program ``debugging.py`` now finished and should have printed ``[0, 1]``. Congratulations, you
have finished your first Ray debugging session!

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
            n_id = fact.remote(n - 1)
            return n * ray.get(n_id)

    ray.util.pdb.set_trace()
    result_ref = fact.remote(5)
    result = ray.get(result_ref)


After running the program by executing the Python file and calling
``ray debug``, you can select the breakpoint by pressing ``0`` and
enter. This will result in the following output:

.. code-block:: python

    Enter breakpoint index or press enter to refresh: 0
    > /Users/pcmoritz/tmp/stepping.py(14)<module>()
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
    > /Users/pcmoritz/tmp/stepping.py(5)fact()
    -> @ray.remote
    (Pdb) ll
      5  ->	@ray.remote
      6  	def fact(n):
      7  	    if n == 1:
      8  	        return n
      9  	    else:
     10  	        n_id = fact.remote(n - 1)
     11  	        return n * ray.get(n_id)
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
    > /Users/pcmoritz/tmp/stepping.py(14)<module>()
    -> result_ref = fact.remote(5)
    (Pdb) remote
    *** Connection closed by remote host ***
    Continuing pdb session in different process...
    --Call--
    > /Users/pcmoritz/tmp/stepping.py(5)fact()
    -> @ray.remote
    (Pdb) p(n)
    5
    (Pdb) remote
    *** Connection closed by remote host ***
    Continuing pdb session in different process...
    --Call--
    > /Users/pcmoritz/tmp/stepping.py(5)fact()
    -> @ray.remote
    (Pdb) p(n)
    4
    (Pdb) get
    *** Connection closed by remote host ***
    Continuing pdb session in different process...
    --Return--
    > /Users/pcmoritz/tmp/stepping.py(5)fact()->120
    -> @ray.remote
    (Pdb) get
    *** Connection closed by remote host ***
    Continuing pdb session in different process...
    --Return--
    > /Users/pcmoritz/tmp/stepping.py(14)<module>()->None
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

    import ray
    from ray import serve
    from sklearn.datasets import load_iris
    from sklearn.ensemble import GradientBoostingClassifier

    # Train model
    iris_dataset = load_iris()
    model = GradientBoostingClassifier()
    model.fit(iris_dataset["data"], iris_dataset["target"])

    # Define Ray Serve model,
    class BoostingModel:
        def __init__(self):
            self.model = model
            self.label_list = iris_dataset["target_names"].tolist()

        def __call__(self, flask_request):
            payload = flask_request.json["vector"]
            print("Worker: received flask request with data", payload)

            prediction = self.model.predict([payload])[0]
            human_name = self.label_list[prediction]
            return {"result": human_name}

    # Deploy model
    client = serve.start()
    client.create_backend("iris:v1", BoostingModel)
    client.create_endpoint("iris_classifier", backend="iris:v1", route="/iris")

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
    0: ray::RayServeWorker_BoostingModel.handle_request() | /Users/pcmoritz/ray/python/ray/serve/backend_worker.py:249
    Traceback (most recent call last):

      File "/Users/pcmoritz/ray/python/ray/serve/backend_worker.py", line 244, in invoke_single
        result = await method_to_call(arg)

      File "/Users/pcmoritz/ray/python/ray/async_compat.py", line 29, in wrapper
        return func(*args, **kwargs)

      File "serve_debugging.py", line 23, in __call__
        prediction = self.model.predict([payload])[0]

      File "/Users/pcmoritz/anaconda3/lib/python3.7/site-packages/sklearn/ensemble/_gb.py", line 2165, in predict
        raw_predictions = self.decision_function(X)

      File "/Users/pcmoritz/anaconda3/lib/python3.7/site-packages/sklearn/ensemble/_gb.py", line 2120, in decision_function
        X = check_array(X, dtype=DTYPE, order="C", accept_sparse='csr')

      File "/Users/pcmoritz/anaconda3/lib/python3.7/site-packages/sklearn/utils/validation.py", line 531, in check_array
        array = np.asarray(array, order=order, dtype=dtype)

      File "/Users/pcmoritz/anaconda3/lib/python3.7/site-packages/numpy/core/_asarray.py", line 83, in asarray
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