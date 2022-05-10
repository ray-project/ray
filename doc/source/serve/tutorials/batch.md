(serve-batch-tutorial)=

# Batching Tutorial

In this guide, we will deploy a simple vectorized adder that takes
a batch of queries and adds them at once. In particular, we show:

- How to implement and deploy a Ray Serve deployment that accepts batches.
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

```{literalinclude} ../../../../python/ray/serve/examples/doc/tutorial_batch.py
:end-before: __doc_import_end__
:start-after: __doc_import_begin__
```

You can use the `@serve.batch` decorator to annotate a function or a method.
This annotation will automatically cause calls to the function to be batched together.
The function must handle a list of objects and will be called with a single object.
This function must also be `async def` so that you can handle multiple queries concurrently:

```python
@serve.batch
async def my_batch_handler(self, requests: List):
    pass
```

This batch handler can then be called from another `async def` method in your deployment.
These calls will be batched and executed together, but return an individual result as if
they were a normal function call:

```python
class MyBackend:
    @serve.batch
    async def my_batch_handler(self, requests: List):
        results = []
        for request in requests:
            results.append(request.json())
        return results

    async def __call__(self, request):
        await self.my_batch_handler(request)
```

:::{note}
By default, Ray Serve performs *opportunistic batching*. This means that as
soon as the batch handler is called, the method will be executed without
waiting for a full batch. If there are more queries available after this call
finishes, a larger batch may be executed. This behavior can be tuned using the
`batch_wait_timeout_s` option to `@serve.batch` (defaults to 0). Increasing this
timeout may improve throughput at the cost of latency under low load.
:::

Let's define a deployment that takes in a list of requests, extracts the input value,
converts them into an array, and uses NumPy to add 1 to each element.

```{literalinclude} ../../../../python/ray/serve/examples/doc/tutorial_batch.py
:end-before: __doc_define_servable_end__
:start-after: __doc_define_servable_begin__
```

Let's deploy it. Note that in the `@serve.batch` decorator, we are specifying
specifying the maximum batch size via `max_batch_size=4`. This option limits
the maximum possible batch size that will be executed at once.

```{literalinclude} ../../../../python/ray/serve/examples/doc/tutorial_batch.py
:end-before: __doc_deploy_end__
:start-after: __doc_deploy_begin__
```

Let's define a [Ray remote task](ray-remote-functions) to send queries in
parallel. As you can see, the first batch has a batch size of 1, and the subsequent
queries have a batch size of 4. Even though each query is issued independently,
Ray Serve was able to evaluate them in batches.

```{literalinclude} ../../../../python/ray/serve/examples/doc/tutorial_batch.py
:end-before: __doc_query_end__
:start-after: __doc_query_begin__
```

What if you want to evaluate a whole batch in Python? Ray Serve allows you to send
queries via the Python API. A batch of queries can either come from the web server
or the Python API. Learn more [here](serve-handle-explainer).

To query the deployment via the Python API, we can use `Deployment.get_handle` to receive
a handle to the corresponding deployment. To enqueue a query, you can call
`handle.method.remote(data)`. This call returns immediately
with a [Ray ObjectRef](ray-object-refs). You can call `ray.get` to retrieve
the result.

```{literalinclude} ../../../../python/ray/serve/examples/doc/tutorial_batch.py
:end-before: __doc_query_handle_end__
:start-after: __doc_query_handle_begin__
```
