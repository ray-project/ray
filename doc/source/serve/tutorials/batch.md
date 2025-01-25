---
orphan: true
---

(serve-batch-tutorial)=

# Serve a Text Generator with Request Batching

This tutorial shows how to deploy a text generator that processes multiple queries simultaneously using batching. 
Learn how to:

- Implement a Ray Serve deployment that handles batched requests
- Configure and optimize batch processing
- Query the model from HTTP and Python

Batching can significantly improve performance when your model supports parallel processing like GPU acceleration or vectorized operations.
It increases both throughput and hardware utilization by processing multiple requests together.

:::{note}
This tutorial focuses on online serving with batching. For offline batch processing of large datasets, see [batch inference with Ray Data](batch_inference_home).
:::

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

Ray Serve provides the `@serve.batch` decorator to automatically batch individual requests to
a function or class method.

The decorated method:
- Must be `async def` to handle concurrent requests
- Receives a list of requests to process together
- Returns a list of results of equal length, one for each request

```python
@serve.batch
async def my_batch_handler(self, requests: List):
    # Process multiple requests together
    results = []
    for request in requests:
        results.append(request)  # processing logic here
    return results
```

You can call the batch handler from another `async def` method in your deployment.
Ray Serve batches and executes these calls together, but returns individual results just like
normal function calls:

```python
class BatchingDeployment:
    @serve.batch
    async def my_batch_handler(self, requests: List):
        results = []
        for request in requests:
            results.append(request.json())  # processing logic here
        return results

    async def __call__(self, request):
        return await self.my_batch_handler(request)
```

:::{note}
Ray Serve uses *opportunistic batching* by default - executing requests as 
soon as they arrive without waiting for a full batch. You can adjust this behavior using 
`batch_wait_timeout_s` in the `@serve.batch` decorator to trade increased latency
for increased throughput (defaults to 0). Increasing this value may improve throughput
at the cost of latency under low load.
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

## Deployment Options

You can deploy your app in two ways:

### Option 1: Deploying with the Serve Command-Line Interface
```console
$ serve run tutorial_batch:generator --name "Text-Completion-App"
```

### Option 2: Deploying with the Python API

Alternatively, you can deploy the app using the Python API using the `serve.run` function. 
This command returns a handle that you can use to query the deployment.

```python
from ray.serve.handle import DeploymentHandle

handle: DeploymentHandle = serve.run(generator, name="Text-Completion-App")
```

You can now use this handle to query the model. See the [Querying the Model](#querying-the-model) section below.


## Querying the Model

There are multiple ways to interact with your deployed model:

### 1. Simple HTTP Queries
For basic testing, use curl:

```console
$ curl "http://localhost:8000/?text=Once+upon+a+time"
```

### 2. Send HTTP requests in parallel with Ray
For higher throughput, use [Ray remote tasks](ray-remote-functions) to send parallel requests:

```python
import ray
import requests

@ray.remote
def send_query(text):
    resp = requests.post("http://localhost:8000/", params={"text": text})
    return resp.text

# Example batch of queries
texts = [
    'Once upon a time,',
    'Hi my name is Lewis and I like to',
    'In a galaxy far far away',
]

# Send all queries in parallel
results = ray.get([send_query.remote(text) for text in texts])
```

### 3. Sending requests using DeploymentHandle
For a more Pythonic way to query the model, you can use the deployment handle directly:

```python
import ray
from ray import serve

input_batch = [
    'Once upon a time,',
    'Hi my name is Lewis and I like to',
    'In a galaxy far far away',
]

# initialize using the 'auto' option to connect to the already-running Ray cluster
ray.init(address="auto")

handle = serve.get_deployment_handle("BatchTextGenerator", app_name="Text-Completion-App")
responses = [handle.handle_batch.remote(text) for text in input_batch]
results = [r.result() for r in responses]
```

## Performance Considerations

- Increase `max_batch_size` if you have sufficient memory and want higher throughput - this may increase latency
- Increase `batch_wait_timeout_s` if throughput is more important than latency