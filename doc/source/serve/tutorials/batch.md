(serve-batch-tutorial)=

# Batching Tutorial

In this guide, we will deploy a simple text generator that takes in
a batch of queries and processes them at once. In particular, we show:

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

Let's define a deployment that takes in a list of input strings and runs 
vectorized text generation on the inputs.

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
$ serve run tutorial_batch:generator
```

Let's define a [Ray remote task](ray-remote-functions) to send queries in
parallel. While Serve is running, open a separate terminal window, and run the 
following in an interactive Python shell or a separate Python script:

```python
import ray
import requests
import numpy as np

@ray.remote
def send_query(text):
    resp = requests.get("http://localhost:8000/?text={}".format(text))
    return resp.text

# Let's use Ray to send all queries in parallel
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

You should get an output like the following. As you can see, the first batch has a 
batch size of 1, and the subsequent queries have a batch size of 4. Even though each 
query is issued independently, Ray Serve was able to evaluate them in batches.
```python
(pid=...) Our input array has length: 1
(pid=...) Our input array has length: 4
(pid=...) Our input array has length: 4
Result returned: [
    'Once upon a time, when I got to look at and see the work of my parents (I still can\'t stand them,) they said, "Boys, you\'re going to like it if you\'ll stay away from him or make him look',

    "Hi my name is Lewis and I like to look great. When I'm not playing against, it's when I play my best and always feel most comfortable. I get paid by the same people who make my games, who work hardest for me.", 

    "My name is Mary, and my favorite person in these two universes, the Green Lantern and the Red Lantern, are the same, except they're two of the Green Lanterns, but they also have their own different traits. Now their relationship is known", 

    'My name is Clara and I am married and live in Philadelphia. I am an English language teacher and translator. I am passionate about the issues that have so inspired me and my journey. My story begins with the discovery of my own child having been born', 

    'My name is Julien and I like to travel with my son on vacations... In fact I really prefer to spend more time with my son."\n\nIn 2011, the following year he was diagnosed with terminal Alzheimer\'s disease, and since then,', 

    "Today I accidentally got lost and went on another tour in August. My story was different, but it had so many emotions that it made me happy. I'm proud to still be able to go back to Oregon for work.\n\nFor the longest", 

    'My greatest wish is to return your loved ones to this earth where they can begin their own free and prosperous lives. This is true only on occasion as it is not intended or even encouraged to be so.\n\nThe Gospel of Luke 8:29', 

    'In a galaxy far far away, the most brilliant and powerful beings known would soon enter upon New York, setting out to restore order to the state. When the world turned against them, Darth Vader himself and Obi-Wan Kenobi, along with the Jedi', 

    'My best talent is that I can make a movie with somebody who really has a big and strong voice. I do believe that they would be great writers. I can tell you that to make sure."\n\n\nWith this in mind, "Ghostbusters'
]
```

## Deploy the Deployment using Python API
What if you want to evaluate a whole batch in Python? Ray Serve allows you to send
queries via the Python API. A batch of queries can either come from the web server
or the Python API.

To query the deployment via the Python API, we can use `serve.run()`, which is part
of the Python API, instead of running `serve run` from the console. Add the following
to the Python script `tutorial_batch.py`:

```python
handle = serve.run(generator)
```

Generally, to enqueue a query, you can call `handle.method.remote(data)`. This call 
returns immediately with a [Ray ObjectRef](ray-object-refs). You can call `ray.get` to 
retrieve the result. Add the following to the same Python script.

```python
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

import ray
result_batch = ray.get([handle.handle_batch.remote(batch) for batch in input_batch])
print("Result batch is", result_batch)
```

Finally, let's run the script.
```console
$ python tutorial_batch.py
```

You should get a similar output like before!