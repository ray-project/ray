---
orphan: true
---

(serve-batch-tutorial)=

# Serve a Text Generator with Request Batching

This example deploys a simple text generator that takes in
a batch of queries and processes them at once. In particular, it shows:

- How to implement and deploy a Ray Serve deployment that accepts batches.
- How to configure the batch size.
- How to query the model in Python.

This tutorial is a guide for serving online queries when your model can take advantage of batching. For example, linear regressions and neural networks use CPU and GPU's vectorized instructions to perform computation in parallel. Performing inference with batching can increase the *throughput* of the model as well as *utilization* of the hardware.

For _offline_ batch inference with large datasets, see [batch inference with Ray Data](batch_inference_home).

## Prerequisites

```python
pip install "ray[serve] transformers"
```

## Define the Deployment
Open a new Python file called `tutorial_batch.py`. First, import Ray Serve and some other helpers.

```{literalinclude} ../doc_code/tutorial_batch.py
:end-before: __doc_import_end__
:start-after: __doc_import_begin__
```

You can use the `@serve.batch` decorator to annotate a function or a method.
This annotation automatically causes calls to the function to be batched together.
The function must handle a list of objects and is called with a single object.
This function must also be `async def` so that you can handle multiple queries concurrently:

```python
@serve.batch
async def my_batch_handler(self, requests: List):
    pass
```

The batch handler can then be called from another `async def` method in your deployment.
These calls together are batched and executed together, but return an individual result as if
they were a normal function call:

```python
class BatchingDeployment:
    @serve.batch
    async def my_batch_handler(self, requests: List):
        results = []
        for request in requests:
            results.append(request.json())
        return results

    async def __call__(self, request):
        return await self.my_batch_handler(request)
```

:::{note}
By default, Ray Serve performs *opportunistic batching*. This means that as
soon as the batch handler is called, the method is executed without
waiting for a full batch. If there are more queries available after this call
finishes, the larger batch may be executed. You can tune this behavior using the
`batch_wait_timeout_s` option to `@serve.batch` (defaults to 0). Increasing this
timeout may improve throughput at the cost of latency under low load.
:::

Next, define a deployment that takes in a list of input strings and runs 
vectorized text generation on the inputs.

```{literalinclude} ../doc_code/tutorial_batch.py
:end-before: __doc_define_servable_end__
:start-after: __doc_define_servable_begin__
```

Next, prepare to deploy the deployment. Note that in the `@serve.batch` decorator, you
are specifying the maximum batch size with `max_batch_size=4`. This option limits
the maximum possible batch size that Ray Serve executes at once.

```{literalinclude} ../doc_code/tutorial_batch.py
:end-before: __doc_deploy_end__
:start-after: __doc_deploy_begin__
```

## Deploying the Serve Application

### Option 1: Deploying using the Serve CLI
Deploy the deployment by running the following through the terminal.
```console
$ serve run tutorial_batch:generator --name "Text-Completion-App"
```

Note that the `--name` argument is optional. If you do not specify a name, the app will be named `default`. Naming the app is useful if you are running multiple apps on the same machine, and also for being able to create a new handle to the app.

### Option 2: Deploying using the Python API
You can also deploy the application using Serve's Python API using the `serve.run()` function. This is creates a serve handle that you can use to query the model. Add the following to the Python script `tutorial_batch.py`:

```python
from ray.serve.handle import DeploymentHandle

handle: DeploymentHandle = serve.run(generator, name="Text-Completion-App")
```

This handle can now be used to query the model-- see the "Query using Serve handle" section below. Giving the app a name makes it easier to create a new handle to the app by running the following:

```python
handle = serve.get_deployment_handle("BatchTextGenerator", app_name="Text-Completion-App")
```

## Querying the Model


#### Using curl (single request)
```console
$ curl "http://localhost:8000/?text=Once+upon+a+time"
```

However, this will return a single response, not a batch. In order to query the model in parallel, we'll need to use the Python API.

#### Using Python requests (batching)
Let's extend the previous example to send HTTP requests in parallel using [Ray remote tasks](ray-remote-functions). First, define a remote task that sends a single request to localhost:

```python
import ray
import requests
import numpy as np

@ray.remote
def send_query(text):
    resp = requests.post("http://localhost:8000/", params={"text": text})
    return resp.text

# Use Ray to send all queries in parallel
texts = [
    'Once upon a time,',
    'Hi my name is Lewis and I like to',
    'My name is Mary, and my favorite',
    'My name is Clara and I am',
    'My name is Julien and I like to',
    'Today I accidentally',
    'My greatest wish is to',
    'In a galaxy far far away',
    'My best talent is',
]
results = ray.get([send_query.remote(text) for text in texts])
print("Result returned:", results)
```
One advantage of this batching approach is that you do not need to have the Serve handle object, you can just send requests to the HTTP endpoint.

### Option 2: Query using Serve handle (batching)
You can query the model directly using the deployment handle:

```python
import ray
from ray import serve

input_batch = [
    'Once upon a time,',
    'Hi my name is Lewis and I like to',
    'My name is Mary, and my favorite',
    'My name is Clara and I am',
    'My name is Julien and I like to',
    'Today I accidentally',
    'My greatest wish is to',
    'In a galaxy far far away',
    'My best talent is',
]
print("Input batch is", input_batch)

# initialize using the 'auto' option to connect to the already-running Ray cluster
ray.init(address="auto")

handle = serve.get_deployment_handle("BatchTextGenerator", app_name="Text-Completion-App")
responses = [handle.handle_batch.remote(batch) for batch in input_batch]
results = [r.result() for r in responses]
print("Result batch is", results)
```

Both querying approaches will produce similar output, showing how the requests are processed in batches:
```python
Result returned: [
    'Once upon a time, when I got to...',
    'Hi my name is Lewis and I like to...',
    'My name is Mary, and my favorite...',
    'My name is Clara and I am...',
    'My name is Julien and I like to...',
    'Today I accidentally...',
    'My greatest wish is to...',
    'In a galaxy far far away...',
    'My best talent is...',
]
```