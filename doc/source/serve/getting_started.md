(getting-started)=

# Getting Started

This tutorial will walk you through the process of using Ray Serve to deploy a single model behind HTTP locally.

We'll be using [HuggingFace's SummarizationPipeline](https://huggingface.co/docs/transformers/main_classes/pipelines#transformers.SummarizationPipeline) to deploy a model that summarizes text.

:::{tip}
If you have suggestions on how to improve this tutorial,
    please [let us know](https://github.com/ray-project/ray/issues/new/choose)!
:::

To run this example, you will need to install the following:

```bash
$ pip install "ray[serve]" transformers
```


## Example Model

Let's first take a look at how the model works without using Ray Serve.
This is the code for the model:

```{literalinclude} ../../../python/ray/serve/examples/doc/e2e_local.py
:end-before: __local_model_end__
:language: python
:linenos: true
:start-after: __local_model_start__
```

The Python file, called `local_model.py` uses the `summarize` function to
generate summaries of text.

- The `summarizer` variable on line 7 inside `summarize` points to a
  function that uses the [t5-small](https://huggingface.co/t5-small)
  model to summarize text.
- When `summarizer` is called on a Python String, it returns summarized text
  inside a dictionary formatted as `[{"summary_text": "...", ...}, ...]`.
- `summarize` then extracts the summarized text on line 13 by indexing into
  the dictionary.

The file can be run locally by executing the Python script, which uses the
model to summarize an article about the Apollo 11 moon landing [^f1].

```bash
$ python local_model.py

"two astronauts steered their fragile lunar module safely and smoothly to the
historic landing . the first men to reach the moon -- Armstrong and his
co-pilot, col. Edwin E. Aldrin Jr. of the air force -- brought their ship to
rest on a level, rock-strewn plain ."
```

Keep in mind that the `SummarizationPipeline` is an example machine learning
model for this tutorial. You can follow along using arbitrary models in any
framework that has a Python API. Check out our tutorials on sckit-learn,
PyTorch, and Tensorflow for more info and examples:

- {ref}`serve-sklearn-tutorial`
- {ref}`serve-pytorch-tutorial`
- {ref}`serve-tensorflow-tutorial`

## Converting to a Ray Serve Deployment

This tutorial's goal is to deploy this model using Ray Serve, so it can be
scaled up and queried over HTTP. We'll start by converting the above Python
function into a Ray Serve deployment that can be launched locally on a laptop.

We start by opening a new Python file. First, we need to import `ray` and
`ray serve`, to use features in Ray Serve such as `deployments`, which
provide HTTP access to our model.

```{literalinclude} ../../../python/ray/serve/examples/doc/e2e_deployment.py
:end-before: __import_end__
:language: python
:start-after: __import_start__
```

After these imports, we can include our model code from above.
We won't call our `summarize` function just yet though!
We will soon add logic to handle HTTP requests, so the `summarize` function
can operate on article text sent via HTTP request.

```{literalinclude} ../../../python/ray/serve/examples/doc/e2e_deployment.py
:end-before: __local_model_end__
:language: python
:start-after: __local_model_start__
```

Ray Serve needs to run on top of a Ray cluster, so we connect to a local one.
See {ref}`serve-deploy-tutorial` to learn more about starting a Ray Serve
instance and deploying to a Ray cluster.

```{literalinclude} ../../../python/ray/serve/examples/doc/e2e_deployment.py
:end-before: __start_ray_cluster_end__
:language: python
:start-after: __start_ray_cluster_start__
```

The `address` parameter in `ray.init()` connects your Serve script to a
running local Ray cluster. Later, we'll discuss how to start a local Ray
cluster.

:::{note}
`ray.init()` connects to or starts a single-node Ray cluster on your
local machine,  which allows you to use all your CPU cores to serve
requests in parallel. To start a multi-node cluster, see
{ref}`serve-deploy-tutorial`.
:::

Next, we start the Ray Serve runtime:

```{literalinclude} ../../../python/ray/serve/examples/doc/e2e_deployment.py
:end-before: __start_serve_end__
:language: python
:start-after: __start_serve_start__
```

:::{note}
`detached=True` means Ray Serve will continue running even when the Python
script exits. If you would rather stop Ray Serve after the script exits, use
`serve.start()` instead (see {ref}`ray-serve-instance-lifetime` for
details).
:::

Now that we have defined our `summarize` function, connected to a Ray
Cluster, and started the Ray Serve runtime, we can define a function that
accepts HTTP requests and routes them to the `summarize` function. We
define a function called `router` that takes in a Starlette `request`
object [^f2]:

```{literalinclude} ../../../python/ray/serve/examples/doc/e2e_deployment.py
:end-before: __router_end__
:language: python
:linenos: true
:start-after: __router_start__
```

- In line 1, we add the decorator `@serve.deployment`
  to the `router` function to turn the function into a Serve `Deployment`
  object.
- In line 3, `router` uses the `"txt"` query parameter in the `request`
  to get the article text to summarize.
- In line 4, it then passes this article text into the `summarize` function
  and returns the value.

:::{note}
Lines 3 and 4 define our HTTP request schema. The HTTP requests sent to this
endpoint must have a `"txt"` query parameter that contains a string.
In general, you can accept HTTP data using query parameters or the
request body. Additionally, you can add other Serve deployments with
different names to create more endpoints that can accept different schemas.
For more complex validation, you can also use FastAPI (see
{ref}`serve-fastapi-http` for more info).
:::

:::{tip}
This routing function's name doesn't have to be `router`.
It can be any function name as long as the corresponding name is present in
the HTTP request. If you want the function name to be different than the name
in the HTTP request, you can add the `name` keyword parameter to the
`@serve.deployment` decorator to specify the name sent in the HTTP request.

For example, if the decorator is `@serve.deployment(name="responder")` and
the function signature is `def request_manager(request)`, the HTTP request
should use `responder`, not `request_manager`. If no `name` is passed
into `@serve.deployment`, the `request` uses the function's name by
default. For example, if the decorator is `@serve.deployment` and the
function's signature is `def manager(request)`, the HTTP request should use
`manager`.
:::

Since `@serve.deployment` makes `router` a `Deployment` object, it can be
deployed using `router.deploy()`:

```{literalinclude} ../../../python/ray/serve/examples/doc/e2e_deployment.py
:end-before: __router_deploy_end__
:language: python
:start-after: __router_deploy_start__
```

Once we deploy `router`, we can query the model over HTTP.
With that, we can run our model on Ray Serve!
Here's the full Ray Serve deployment script that we built for our model:

```{literalinclude} ../../../python/ray/serve/examples/doc/e2e_deployment_full.py
:end-before: __deployment_full_end__
:language: python
:linenos: true
:start-after: __deployment_full_start__
```

To deploy `router`, we first start a local Ray cluster:

```bash
$ ray start --head
```

The Ray cluster that this command launches is the same Ray cluster that the
Python code connects to using `ray.init(address="auto", namespace="serve")`.
It is also the same Ray cluster that keeps Ray Serve (and any deployments on
it, such as `router`) alive even after the Python script exits as long as
`detached=True` inside `serve.start()`.

:::{tip}
To stop the Ray cluster, run the command `ray stop`.
:::

After starting the Ray cluster, we can run the Python file to deploy `router`
and begin accepting HTTP requests:

```bash
$ python model_on_ray_serve.py
```

## Testing the Ray Serve Deployment

We can now test our model over HTTP. The structure of our HTTP query is:

`http://127.0.0.1:8000/[Deployment Name]?[Parameter Name-1]=[Parameter Value-1]&[Parameter Name-2]=[Parameter Value-2]&...&[Parameter Name-n]=[Parameter Value-n]`

Since the cluster is deployed locally in this tutorial, the `127.0.0.1:8000`
refers to a localhost with port 8000. The `[Deployment Name]` refers to
either the name of the function that we called `.deploy()` on (in our case,
this is `router`), or the `name` keyword parameter's value in
`@serve.deployment` (see the Tip under the `router` function definition
above for more info).

Each `[Parameter Name]` refers to a field's name in the
request's `query_params` dictionary for our deployed function. In our
example, the only parameter we need to pass in is `txt`. This parameter is
referenced in the `txt = request.query_params["txt"]` line in the `router`
function. Each \[Parameter Name\] object has a corresponding \[Parameter Value\]
object. The `txt`'s \[Parameter Value\] is a string containing the article
text to summarize. We can chain together any number of the name-value pairs
using the `&` symbol in the request URL.

Now that the `summarize` function is deployed on Ray Serve, we can make HTTP
requests to it. Here's a client script that requests a summary from the same
article as the original Python script:

```{literalinclude} ../../../python/ray/serve/examples/doc/e2e_router_client.py
:end-before: __client_function_end__
:language: python
:start-after: __client_function_start__
```

We can run this script while the model is deployed to get a response over HTTP:

```bash
$ python router_client.py

"two astronauts steered their fragile lunar module safely and smoothly to the
historic landing . the first men to reach the moon -- Armstrong and his
co-pilot, col. Edwin E. Aldrin Jr. of the air force -- brought their ship to
rest on a level, rock-strewn plain ."
```

## Using Classes in the Ray Serve Deployment

Our application is still a bit inefficient though. In particular, the
`summarize` function loads the model on each call when it sets the
`summarizer` variable. However, the model never changes, so it would be more
efficient to define `summarizer` only once and keep its value in memory
instead of reloading it for each HTTP query.

We can achieve this by converting our `summarize` function into a class:

```{literalinclude} ../../../python/ray/serve/examples/doc/e2e_class_deployment.py
:end-before: __deployment_class_end__
:language: python
:linenos: true
:start-after: __deployment_class_start__
```

In this configuration, we can query the `Summarizer` class directly.
The `Summarizer` is initialized once (after calling `Summarizer.deploy()`).
In line 13, its `__init__` function loads and stores the model in
`self.summarize`. HTTP queries for the `Summarizer` class are routed to its
`__call__` method by default, which takes in the Starlette `request`
object. The `Summarizer` class can then take the request's `txt` data and
call the `self.summarize` function on it without loading the model on each
query.

:::{tip}
Instance variables can also store state. For example, to
count the number of requests served, a `@serve.deployment` class can define
a `self.counter` instance variable in its `__init__` function and set it
to 0. When the class is queried, it can increment the `self.counter`
variable inside of the function responding to the query. The `self.counter`
will keep track of the number of requests served across requests.
:::

HTTP queries for the Ray Serve class deployments follow a similar format to Ray
Serve function deployments. Here's an example client script for the
`Summarizer` class. Notice that the only difference from the `router`'s
client script is that the URL uses the `Summarizer` path instead of
`router`.

```{literalinclude} ../../../python/ray/serve/examples/doc/e2e_summarizer_client.py
:end-before: __client_class_end__
:language: python
:start-after: __client_class_start__
```

We can deploy the class-based model on Serve without stopping the Ray cluster.
However, for the purposes of this tutorial, let's restart the cluster, deploy
the model, and query it over HTTP:

```bash
$ ray stop
$ ray start --head
$ python summarizer_on_ray_serve.py
$ python summarizer_client.py

"two astronauts steered their fragile lunar module safely and smoothly to the
historic landing . the first men to reach the moon -- Armstrong and his
co-pilot, col. Edwin E. Aldrin Jr. of the air force -- brought their ship to
rest on a level, rock-strewn plain ."
```

## Advanced HTTP Functionality with FastAPI

Now suppose we want to expose additional functionality in our model. In
particular, the `summarize` function also has `min_length` and
`max_length` parameters. Although we could expose these options as additional
parameters in URL, Ray Serve also allows us to add more route options to the
URL itself and handle each route separately.

Because this logic can get complex, Serve integrates with
[FastAPI](https://fastapi.tiangolo.com/). This allows us to define a Serve
deployment by adding the `@serve.ingress` decorator to a FastAPI app. For
more info about FastAPI with Serve, please see {ref}`serve-fastapi-http`.

As an example of FastAPI, here's a modified version of our `Summarizer` class
with route options to request a minimum or maximum length of ten words in the
summaries:

```{literalinclude} ../../../python/ray/serve/examples/doc/e2e_fastapi_deployment.py
:end-before: __fastapi_end__
:language: python
:linenos: true
:start-after: __fastapi_start__
```

The class now exposes three routes:

- `/Summarizer`: As before, this route takes in article text and returns
  a summary.
- `/Summarizer/min10`: This route takes in article text and returns a
  summary with at least 10 words.
- `/Summarizer/max10`: This route takes in article text and returns a
  summary with at most 10 words.

Notice that `Summarizer`'s methods no longer take in a Starlette `request`
object. Instead, they take in the URL's `txt` parameter directly with FastAPI's
[query parameter](https://fastapi.tiangolo.com/tutorial/query-params/)
feature.

Since we still deploy our model locally, the full URL still uses the
localhost IP. This means each of our three routes comes after the
`http://127.0.0.1:8000` IP and port address. As an example, we can make
requests to the `max10` route using this client script:

```{literalinclude} ../../../python/ray/serve/examples/doc/e2e_fastapi_client.py
:end-before: __client_fastapi_end__
:language: python
:start-after: __client_fastapi_start__
```

```bash
$ ray stop
$ ray start --head
$ python serve_with_fastapi.py
$ python fastapi_client.py

"two astronauts steered their fragile lunar"
```

Congratulations! You just built and deployed a machine learning model on Ray
Serve! You should now have enough context to dive into the {doc}`key-concepts` to
get a deeper understanding of Ray Serve.


## Next Steps

- Dive into the {doc}`key-concepts` to get a deeper understanding of Ray Serve.
- Learn more about how to deploy your Ray Serve application to a multi-node cluster: {ref}`serve-deploy-tutorial`.
- Check more in-depth tutorials for popular machine learning frameworks: {doc}`tutorials/index`.

```{rubric} Footnotes
```

[^f1]: The article text comes from the New York Times article "Astronauts
    Land on Plain; Collect Rocks, Plant Flag" archived
    [here](https://archive.nytimes.com/www.nytimes.com/library/national/science/nasa/072169sci-nasa.html).

[^f2]: [Starlette](https://www.starlette.io/) is a web server framework
    used by Ray Serve. Its [Request](https://www.starlette.io/requests/) class
    provides a nice interface for incoming HTTP requests.
