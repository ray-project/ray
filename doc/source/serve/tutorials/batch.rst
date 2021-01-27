.. _serve-batch-tutorial:

Batching Tutorial
=================

In this guide, we will deploy a simple vectorized adder that takes
a batch of queries and adds them at once. In particular, we show:

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

For single query backend, the input type is a single Starlette request or
:mod:`ServeRequest <ray.serve.utils.ServeRequest>`:

.. code-block:: python

    def single_request(
        request: Union[starlette.requests.Request, ServeRequest],
    ):
        pass

For batched backends, the input types are converted to list of their original
types:

.. code-block:: python

    @serve.accept_batch
    def batched_request(
        request: List[Union[starlette.requests.Request, ServeRequest]],
    ):
        pass

Let's define the backend function. We will take in a list of requests, extract
the input value, convert them into an array, and use NumPy to add 1 to each element.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_batch.py
    :start-after: __doc_define_servable_v0_begin__
    :end-before: __doc_define_servable_v0_end__

Let's deploy it. Note that in the ``config`` section of ``create_backend``, we
are specifying the maximum batch size via ``config={"max_batch_size": 4}``. This
configuration option limits the maximum possible batch size sent to the backend.

.. note::
    Ray Serve performs *opportunistic batching*. When a replica is free to evaluate
    the next batch, Ray Serve will look at the pending queries and try to take
    :mod:`max_batch_size <ray.serve.BackendConfig>` queries to form a batch.
    If the batch has less than ``max_batch_size`` queries, the backend will
    wait for :mod:`batch_wait_timeout <ray.serve.BackendConfig>`
    seconds to wait for a full batch to arrive. The default wait is ``0s`` to
    minimize query latency. You can increase the timeout to improve throughput
    and increase utilization at the cost of some additional latency.

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
queries via the Python API. A batch of queries can either come from the web server
or the Python API. Requests coming from the Python API will have a similar API
to Starlette Request. See more on the API :ref:`here<serve-handle-explainer>`.

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
``handle.remote(data, argument_name=argument_value)``. This call returns immediately
with a :ref:`Ray ObjectRef<ray-object-refs>`. You can call `ray.get` to retrieve
the result.

.. literalinclude:: ../../../../python/ray/serve/examples/doc/tutorial_batch.py
    :start-after: __doc_query_handle_begin__
    :end-before: __doc_query_handle_end__
