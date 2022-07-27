(getting-started)=

# Getting Started

This tutorial will walk you through the process of deploying a model with Ray Serve.

We'll use [HuggingFace's TranslationPipeline](https://huggingface.co/docs/transformers/main_classes/pipelines#transformers.TranslationPipeline) to deploy a text-translation model, and we'll test it with HTTP requests.

:::{tip}
If you have suggestions on how to improve this tutorial,
    please [let us know](https://github.com/ray-project/ray/issues/new/choose)!
:::

To run this example, you will need to install the following:

```bash
$ pip install "ray[serve]" transformers requests
```


## Example Model

Let's first take a look at how the model works. Here's it's code:

```{literalinclude} ../serve/doc_code/getting_started/model.py
:start-after: __start__
:end-before: __end__
:language: python
:linenos: true
```

The Python file, called `model.py` uses the `Translator` class to translate English text to French.

- The `self.model` variable on line 8 inside `Translator`'s `__init__` method
  stores a function that uses the [t5-small](https://huggingface.co/t5-small)
  model to translate text.
- When `self.model` is called on English text, it returns translated French text
  inside a dictionary formatted as `[{"translation_text": "..."}]`.
- The `Translator`'s `translate` method extracts the translated text on
  line 15 by indexing into the dictionary.

You can copy-paste this script and run it locally. It translates `"Hello world!"`
into `"Bonjour Monde!"`.

```console
$ python model.py

Bonjour Monde!
```

Keep in mind that the `TranslationPipeline` is an example ML model for this
tutorial. You can follow along using arbitrary models from any
Python framework. Check out our tutorials on scikit-learn,
PyTorch, and Tensorflow for more info and examples:

- {ref}`serve-sklearn-tutorial`
- {ref}`serve-pytorch-tutorial`
- {ref}`serve-tensorflow-tutorial`

(converting-to-ray-serve-deployment)=
## Converting to a Ray Serve Deployment

This tutorial's goal is to deploy this model using Ray Serve, so it can be
scaled up and queried over HTTP. We'll start by converting `Translator` into a
Ray Serve deployment that runs locally on your computer.

First, we open a new Python file and import `ray` and `ray serve`:

```{literalinclude} ../serve/doc_code/getting_started/model_deployment.py
:start-after: __import_start__
:end-before: __import_end__
:language: python
```

After these imports, we can include our model code from above:

```{literalinclude} ../serve/doc_code/getting_started/model_deployment.py
:start-after: __model_start__
:end-before: __model_end__
:language: python
```

The `Translator` class has two modifications:
1. It has a decorator, `@serve.deployment`.
2. It has a new method, `__call__`.

The decorator converts `Translator` from a Python class into a Ray Serve
`Deployment` object.

Each deployment stores a single Python function or class that you write and uses
it to serve requests. You can scale and configure each of your deployments independently using
parameters in the `@serve.deployment` decorator.

Deployments receive Starlette HTTP `request` objects [^f1]. If your deployment stores a Python function, the function is called on this `request` object. If you deployment stores a class, the class's `__call__` method is called on this `request` object. The return value is sent back in the HTTP response body.

This is why `Translator` needs a new `__call__` method. The method processes the incoming HTTP request by reading its JSON data and forwarding it to the `translate` method. The translated text is returned and sent back through the HTTP response. You can also use Ray Serve's FastAPI integration to avoid working with raw HTTP requests. Check out {ref}`serve-fastapi-http` for more info about FastAPI with Serve.


Next, we need to connect to a Ray cluster that can run our Ray Serve application.
`ray.init(address=auto)` will try connecting to a local Ray cluster:

```{literalinclude} ../serve/doc_code/getting_started/model_deployment.py
:start-after: __connect_to_ray_cluster_start__
:end-before: __connect_to_ray_cluster_end__
:language: python
```

Later, we'll discuss how to start a local Ray cluster, so we can run this script.

:::{note}
`ray.init()` connects to or starts a single-node Ray cluster on your
local machine,  which allows you to use all your CPU cores to serve
requests in parallel. To start a multi-node cluster, see
{ref}`serve-deploy-tutorial`.
:::

Lastly, we need to run our `Translator` deployment on the Ray cluster,
so it can receive and serve HTTP traffic:

```{literalinclude} ../serve/doc_code/getting_started/model_deployment.py
:start-after: __model_deploy_start__
:end-before: __model_deploy_end__
:language: python
```

`translator = Translator.bind()` binds our deployment to constructor `args` and
`kwargs`. Ray Serve uses these to initialize the `Translator` class using its
`__init__` method before it starts serving traffic. The `translator` variable
is a `DeploymentNode` object. As we'll see later in the tutorial, you can also
connect `DeploymentNodes` together to form a `Deployment Graph`
that serves multiple models instead of just one.

Then, `serve.run(translator)` deploys `Translator` to the Ray cluster.

With that, we can run our model on Ray Serve!
Here's the full Ray Serve script that we built:

```{literalinclude} ../serve/doc_code/getting_started/model_deployment_full.py
:start-after: __deployment_full_start__
:end-before: __deployment_full_end__
:language: python
:linenos: true
```

To run our script, we first start a local Ray cluster:

```bash
$ ray start --head
```

The Ray cluster that this command launches is the cluster that the
Python code connects to with `ray.init(address="auto")`.

:::{tip}
To stop the Ray cluster, run the command `ray stop`.
:::

After starting the Ray cluster, we can run the Python file to deploy `Translator`
and begin accepting HTTP requests:

```bash
$ python model_on_ray_serve.py
```

## Testing the Ray Serve Deployment

We can now test our model over HTTP. It can be reached at `http://127.0.0.1:8000/`

Since the cluster is deployed locally in this tutorial, the `127.0.0.1:8000`
refers to a localhost with port 8000.

We'll send a POST request with JSON data containing our English text.
`Translator`'s `__call__` method will unpack this text and forward it to the
`translate` method. Here's a client script that requests a translation for "Hello world!":

```{literalinclude} ../serve/doc_code/getting_started/model_deployment.py
:start-after: __client_function_start__
:end-before: __client_function_end__
:language: python
```

We can run this script while the model is deployed to get a response over HTTP:

```console
$ python model_client.py

Bonjour monde!
```

## Deployment Graphs and HTTP Adapters

We can simplify our HTTP-handling code using Ray Serve's HTTP adapters, which
provide out-of-the-box HTTP parsing. To use these adapters, we need to refactor
the deployment graph code a bit:

```python
from ray.serve.drivers import DAGDriver
from ray.serve.http_adapters import json_request
from ray.serve.deployment_graph import InputNode

with InputNode() as json_data:
  translator_node = Translator.bind()
  translate_method_node = translator_node.translate.bind(json_data)

deployment_graph = DAGDriver.bind(translate_method_node, http_adapter=json_request)
serve.run(deployment_graph)
```

Let's first take a closer look at the line where we set `deployment_graph`. The
`DAGDriver` is a `DeploymentNode` provided out-of-the-box with Ray Serve. It's
meant to be the root of your graph. It's the node that HTTP requests are
sent to, and it forwards the requests to the node passed in as its first argument.

The `DAGDriver`'s `http_adapter` keyword argument takes in
a function that can preprocess the HTTP request before forwarding it to the next node.
We're using Ray Serve's built-in `json_request` adapter, which returns the JSON data
from the HTTP request. That way, the next node can process the JSON directly instead
of munging the HTTP request.

Next, let's take a look at the context manager (i.e. the `with` statement). It
initializes an `InputNode` object as `json_data`. `InputNode` represents the object that
the `DAGDriver` will pass to the next node in the graph. Since `DAGDriver` is using a
`json_request` adapter, it will pass JSON data to the next node, which is why we refer to
the `InputNode` as `json_data` in this snippet.

Inside the context manager, we define the rest of the graph. First, we initialize any `DeploymentNodes` that contain Python classes by calling `bind` on their constructor's `args` and `kwargs`. Since our `Translator` class doesn't take in any `args` or `kwargs` in its `__init__` method, we call `bind` without passing anything in, and create a `translator_node`.

Similarly, we can `bind` method calls for these deployment nodes, which creates new nodes representing these calls. In the code snippet, we `bind` the `translator_node`'s `translate` method to the `json_data`. When the `DAGDriver` receives an HTTP request, it will retrieve the request's JSON data with the `json_request` and then call the `translator_node`'s `translate` method on the data. This means, we also don't need to add a new `__call__` method to the `Translate` class. This graph allows us to call the `translate` method directly.

Here's the full script with the refactored code:

```{literalinclude} ../serve/doc_code/getting_started/graph_and_adapter.py
:start-after: __start__
:end-before: __end__
:language: python
```

## Porting FastAPI Applications to Ray Serve

Ray Serve also lets you port your existing
[FastAPI](https://fastapi.tiangolo.com/) Applications. You can also
integrate your Ray Serve applications with FastAPI to access features like
advanced HTTP parsing, automatic UI and documentation generation, and Pydantic
type-checking. For more info about FastAPI with Serve, please see
{ref}`serve-fastapi-http`.

You can define a Serve deployment by adding the `@serve.ingress` decorator to
your FastAPI app. As an example of FastAPI, here's a modified version of our
`Translator` class:

```{literalinclude} ../serve/doc_code/getting_started/fastapi_model.py
:start-after: __fastapi_start__
:end-before: __fastapi_end__
:language: python
```

We can run this script and then send HTTP requests to the deployment:

```{literalinclude} ../serve/doc_code/getting_started/fastapi_model.py
:start-after: __fastapi_client_start__
:end-before: __fastapi_client_end__
:language: python
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

[^f1]: [Starlette](https://www.starlette.io/) is a web server framework
    used by Ray Serve. Its [Request](https://www.starlette.io/requests/) class
    provides a nice interface for incoming HTTP requests.
