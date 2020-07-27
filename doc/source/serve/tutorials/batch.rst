.. _serve-batch-tutorial:

Batching Tutorial
=================

In this guide, we will deploy a simple vectorized adder that takes
a batch of queries and add them at once. In particular, we show:

- How to implement and deploy Ray Serve model that accepts batches.
- How to configure the batch size.
- How to query the model in Python.

This tutorial should help the following use cases:

- You want to perform offline batch inference on a cluster of machines.
- You want to serve online queries and your model can take advantage of batching.
  For example, linear regressions and neural networks use CPU and GPU's
  vectorized instructions to perform computation in parallel. Performing
  inference with batching can increase the *throughput* of the model as well as
  *utilization* of the hardware.


Let's import Ray Serve and some other helpers.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_batch.py
    :start-after: __doc_import_begin__
    :end-before: __doc_import_end__

You can use the ``@serve.accept_batch`` decorator to annotate a function or a class.
This annotation is needed because batched backends have different APIs compared
to single request backends. In a batched backend, the inputs are a list of values.

For single query backend, the input types are single flask request or Python
argument:

.. code-block:: python

    def single_request(
        flask_request: Flask.Request,
        *,
        python_arg: int = 0
    ):
        pass

For batched backend, the inputs types are converted to list of their original
types:

.. code-block:: python

    @serve.accept_batch
    def batched_request(
        flask_request: List[Flask.Request],
        *,
        python_arg: List[int]
    ):
        pass

Let's define the backend function. We will take in a list of requests, extract
the input value, convert them into an array, and use NumPy to add 1 to each element.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_batch.py
    :start-after: __doc_define_servable_v0_begin__
    :end-before: __doc_define_servable_v0_end__

Let's deploy it. Note that in the ``config`` section of ``create_backend``, we
are specifying the maximum batch size via ``config={"max_batch_size": 4}``. This
configuration option limits the maximum possible batch size send to the backend.

.. note::
    Ray Serve performs *opportunistic batching*. When a worker is free to evaluate
    the next batch, Ray Serve will look at the pending queries and take
    ``max(number_of_pending_queries, max_batch_size)`` queries to form a batch.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_batch.py
    :start-after: __doc_deploy_begin__
    :end-before: __doc_deploy_end__

Let's define a :ref:`Ray remote task<ray-remote-functions>` to send queries in
parallel. As you can see, the first batch has a batch size of 1, and the subsequent
queries have a batch size of 4. Even though each query is issued independently,
Ray Serve was able to evaluate them in batches.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_batch.py
    :start-after: __doc_query_begin__
    :end-before: __doc_query_end__

What if you want to evaluate a whole batch in Python? Ray Serve allows you to send
queries via the Python API. You can use the boolean value ``serve.context.web`` to
distinguish the origin of the queries. A batch of queries can either come from
the web server or the Python API. Ray Serve will guarantee there won't be queries
with mixed origins.

When the batch of requests comes from the web API, Ray Serve will fill the first
argument ``flask_requests`` with a list of ``Flask.Request`` objects and set
``serve.context.web = True``. When the batch of requests comes from the Python API,
Ray Serve will fill ``flask_requests`` arguments with placeholders, and directly inject
Python objects into the keyword arguments. In this case, the ``numbers`` argument
will be a list of Python integers.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_batch.py
    :start-after: __doc_define_servable_v1_begin__
    :end-before: __doc_define_servable_v1_end__

Let's deploy the new version to the same endpoint. Don't forget to set
``max_batch_size``!

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_batch.py
    :start-after: __doc_deploy_v1_begin__
    :end-before: __doc_deploy_v1_end__

To query the backend via Python API, we can use ``serve.get_handle`` to receive
a handle to the corresponding "endpoint". To enqueue a query, you can call
``handle.remote(argument_name=argument_value)``. This call returns immediately
with a :ref:`Ray ObjectRef<ray-object-ids>`. You can call `ray.get` to retrieve
the result.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_batch.py
    :start-after: __doc_query_handle_begin__
    :end-before: __doc_query_handle_end__