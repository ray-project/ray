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


## Define the Deployment
Open a new Python file called `tutorial_batch.py`. First, let's import Ray Serve and some other helpers.

```{literalinclude} ../doc_code/tutorial_batch.py
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

Let's define a deployment that takes in a list of vectors, combines them into a 
matrix, and multiples the combined vectors by a another fixed matrix.

```{literalinclude} ../doc_code/tutorial_batch.py
:end-before: __doc_define_servable_end__
:start-after: __doc_define_servable_begin__
```

Let's prepare to deploy the deployment. Note that in the `@serve.batch` decorator, we
are specifying the maximum batch size via `max_batch_size=4`. This option limits
the maximum possible batch size that will be executed at once.

```{literalinclude} ../doc_code/tutorial_batch.py
:end-before: __doc_deploy_end__
:start-after: __doc_deploy_begin__
```

## Deploy the Deployment
Deploy the deployment by running the following through the terminal.
```console
$ serve run tutorial_batch:adder
```

Let's define a [Ray remote task](ray-remote-functions) to send queries in
parallel. While Serve is running, open a separate terminal window, and run the 
following in an interactive Python shell or a separate Python script:

```python
import ray
import requests
import numpy as np

@ray.remote
def send_query(array):
    resp = requests.get("http://localhost:8000/", json=array.tolist())
    return resp.json()

# Let's use Ray to send all queries in parallel
results = ray.get([send_query.remote(np.random.rand(50)) for _ in range(9)])
print("Result returned:", results)
```

You should get an output like the following. As you can see, the first batch has a 
batch size of 1, and the subsequent queries have a batch size of 4. Even though each 
query is issued independently, Ray Serve was able to evaluate them in batches.
```bash
(pid=...) Our input array has shape: (50, 1)
(pid=...) Our input array has shape: (50, 4)
(pid=...) Our input array has shape: (50, 4)
Result returned: [[13.17880051093083, 11.36916172468337, ...],[12.725841867183606,11.44933768139967, ...], ...]
```

## Deploy the Deployment using Python API
What if you want to evaluate a whole batch in Python? Ray Serve allows you to send
queries via the Python API. A batch of queries can either come from the web server
or the Python API.

To query the deployment via the Python API, we can use `serve.run()`, which is part
of the Python API, instead of running `serve run` from the console. Add the following
to the Python script `tutorial_batch.py`:

```python
handle = serve.run(adder)
```

Generally, to enqueue a query, you can call `handle.method.remote(data)`. This call 
returns immediatelywith a [Ray ObjectRef](ray-object-refs). You can call `ray.get` to 
retrieve the result. Add the following to the same Python script.

```python
input_batch = [np.random.rand(50) for _ in range(9)]
print("Input batch is", input_batch)

import ray
result_batch = ray.get([handle.handle_batch.remote(batch) for batch in input_batch])
print("Result batch is", result_batch)
```

Finally, let's run the script.
```console
$ python tutorial_batch.py
```

You should get an output like the following.
```bash
(pid=...) Our input array has shape: (50, 1)
(pid=...) Our input array has shape: (50, 4)
(pid=...) Our input array has shape: (50, 4)
Result returned: [[13.43188362773026, 10.635865255218478, ...], [13.161842646470665, 11.92617474588417, ...], ...]
```