(getting-started)=

# Getting Started

This tutorial will walk you through the process of deploying models with Ray Serve. It will show you how to

* Expose your models over HTTP using Ray Serve [deployments](serve-managing-deployments-guide)
* Scale your deployments to meet your workload's requirements
* Allocate resources like fractional GPUs and CPUs to your deployments
* Compose multiple-model machine learning pipelines with Ray Serve [deployment graphs](deployment-graph-e2e-tutorial)
* Port your FastAPI applications to Ray Serve

We'll use two models in this tutorial:

* [HuggingFace's TranslationPipeline](https://huggingface.co/docs/transformers/main_classes/pipelines#transformers.TranslationPipeline) as a text-translation model
* [HuggingFace's SummarizationPipeline](https://huggingface.co/docs/transformers/v4.21.0/en/main_classes/pipelines#transformers.SummarizationPipeline) as a text-summarizer model

After deploying these models, we'll test them with HTTP requests.

:::{tip}
If you have suggestions on how to improve this tutorial,
    please [let us know](https://github.com/ray-project/ray/issues/new/choose)!
:::

To run this example, you will need to install the following:

```bash
pip install "ray[serve]" transformers requests
```


## Model Example: Before Ray Serve

First, let's take a look at our text-translation model. Here's its code:

```{literalinclude} ../serve/doc_code/getting_started/models.py
:start-after: __start_translation_model__
:end-before: __end_translation_model__
:language: python
:linenos: true
```

The Python file, called `model.py`, uses the `Translator` class to translate English text to French.

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

- {ref}`serve-ml-models-tutorial`

(converting-to-ray-serve-deployment)=
## Converting to a Ray Serve Deployment

In this section, we'll deploy the text translation model using Ray Serve, so
it can be scaled up and queried over HTTP. We'll start by converting
`Translator` into a Ray Serve deployment that runs locally on your computer.

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

Deployments receive Starlette HTTP `request` objects [^f1]. If your deployment stores a Python function, the function is called on this `request` object. If your deployment stores a class, the class's `__call__` method is called on this `request` object. The return value is sent back in the HTTP response body.

This is why `Translator` needs a new `__call__` method. The method processes the incoming HTTP request by reading its JSON data and forwarding it to the `translate` method. The translated text is returned and sent back through the HTTP response. You can also use Ray Serve's FastAPI integration to avoid working with raw HTTP requests. Check out {ref}`serve-fastapi-http` for more info about FastAPI with Serve.

Next, we need to `bind` our `Translator` deployment to arguments that Ray Serve can pass into its constructor. This will let Ray Serve initialize a `Translator` object that can serve requests. Since `Translator`'s constructor doesn't take in any arguments, we can call the deployment's `bind` method without passing anything in:

```{literalinclude} ../serve/doc_code/getting_started/model_deployment.py
:start-after: __model_deploy_start__
:end-before: __model_deploy_end__
:language: python
```

With that, we can run our model on Ray Serve!
Here's the full Ray Serve script that we built:

```{literalinclude} ../serve/doc_code/getting_started/model_deployment_full.py
:start-after: __deployment_full_start__
:end-before: __deployment_full_end__
:language: python
:linenos: true
```

We can run our script with the `serve run` CLI command. This command takes in an import path
to our deployment formatted as `module:bound_deployment`. Make sure to run the command from a directory containing a local copy of this script, so it can find the bound deployment:

```console
$ serve run serve_deployment:translator
```

This command will start running `Translator` and then block. It can be killed with `ctrl-C` in the terminal.

## Testing Ray Serve Deployments

We can now test our model over HTTP. It can be reached at the following URL:

```
http://127.0.0.1:8000/
```

Since the cluster is deployed locally in this tutorial, the `127.0.0.1:8000`
refers to a localhost with port 8000 (the default port where you can reach
Serve deployments).

We'll send a POST request with JSON data containing our English text.
`Translator`'s `__call__` method will unpack this text and forward it to the
`translate` method. Here's a client script that requests a translation for "Hello world!":

```{literalinclude} ../serve/doc_code/getting_started/model_deployment.py
:start-after: __client_function_start__
:end-before: __client_function_end__
:language: python
```

To test our deployment, first make sure `Translator` is running:

```
$ serve run serve_deployment:translator
```

While `Translator` is running, we can open a separate terminal window and run the client script. This will get a response over HTTP:

```console
$ python model_client.py

Bonjour monde!
```

## Scaling Ray Serve Deployments

We can scale Ray Serve deployments up and down to meet our workload's requirements. Ray Serve offers a parameter in the `@serve.deployment` decorator called `num_replicas`. `num_replicas` is an integer that determines how many copies of our deployment process run in Ray. By default, it's set to 1. If we set it to a higher number, we can create more copies (called `replicas`) of our deployment. When many clients make requests to our deployments at the same time, their requests are routed to different replicas, allowing us to split our workload across the replicas. This lets us horizontally scale our deployments to take full advantage of our computing resources. Check out our guide on [scaling out a deployment](scaling-out-a-deployment) for more info.

As an example, we can rewrite our `Translator` class to use 3 replicas to handle more client requests smoothly. All we need to do is set `num_replicas` in the decorator:

```python
...

@serve.deployment(
  num_replicas=3
)
class Translator:
    ...

...
```

We can also manually tune the number of replicas for our deployments in production. See the guide on [putting Ray Serve in production](serve-in-production) to learn more.

Ray Serve also offers autoscaling, allowing you to set `min_replicas` and `max_replicas` on your deployments. Ray Serve will automatically scale your deployments to fit their usage. This feature also lets you scale to zero, so your deployments can have 0 replicas during periods of zero usage, allowing you to automatically save resources. See the guide on [Ray Serve autoscaling](ray-serve-autoscaling) to learn more.

## Reserving Fine-Grained Resources including Fractional GPUs and CPUs

Ray Serve allows us to reserve fine-grained resources for each of our deployment's replicas. These resources include the number of CPUs and GPUs that we'd like to allocate per replica. The `@serve.deployment` decorator offers a parameter called `ray_actor_options`, which is a dictionary of settings for each of our replicas. Two of these settings are `num_cpus` and `num_gpus`, which control the number of CPUs and the number of GPUs reserved for each deployment replica.

For example, we can rewrite `Translator`, so each replica has access to 2 CPUs and 1 GPU. All we need to do is set `num_cpus` and `num_gpus` in the decorator:

```python
...

@serve.deployment(
  ray_actor_options={
    "num_cpus": 2,
    "num_gpus": 1,
  }
)
class Translator:
    ...

...
```

Note that these settings represent logical CPUs and GPUs, so we can also set them to fractions. Fractional GPUs and CPUs allow us to pack multiple deployment replicas together on the same machine, so they can share the available GPUs and CPUs.

For example, if we have a machine with 2 CPUs and 1 GPU, we can rewrite `Translator` to reserve all the available resources with 2 replicas:

```python
...

@serve.deployment(
  num_replicas=2,
  ray_actor_options={
    "num_cpus": 1,
    "num_gpus": 0.5,
  }
)
class Translator:
    ...

...
```

See the guides on [Ray Serve resource management](serve-cpus-gpus) and [Ray Serve's fractional resources](serve-fractional-resources-guide) to learn more.

## Composing Machine Learning Models with Deployment Graphs

Ray Serve's Deployment Graph API allows us to compose multiple machine learning models together into a single Ray Serve application. We can use parameters like `num_replicas`, `num_cpus`, and `num_gpus` to independently configure and scale each deployment in the graph.

For example, let's deploy a machine learning pipeline with two steps:

1. Summarize English text
2. Translate the summary into French

`Translator` already performs step 2. We can use [HuggingFace's SummarizationPipeline](https://huggingface.co/docs/transformers/v4.21.0/en/main_classes/pipelines#transformers.SummarizationPipeline) to accomplish step 1. Here's an example of the `SummarizationPipeline` that runs locally:

```{literalinclude} ../serve/doc_code/getting_started/models.py
:start-after: __start_summarization_model__
:end-before: __end_summarization_model__
:language: python
```

You can copy-paste this script and run it locally. It summarizes the snippet from _A Tale of Two Cities_ to `it was the best of times, it was worst of times .`

```console
$ python model.py

it was the best of times, it was worst of times .
```

Here's a Ray Serve deployment graph that chains the two models together. The graph takes English text, summarizes it, and then translates it:

```{literalinclude} ../serve/doc_code/getting_started/model_graph.py
:start-after: __start_graph__
:end-before: __end_graph__
:language: python
:linenos: true
```

This script contains our `Summarizer` class converted to a deployment and our `Translator` class with some modifications. In this script, the `Summarizer` class contains the `__call__` method since requests are sent to it first. It also takes in the `Translator` as one of its constructor arguments, so it can forward summarized texts to the `Translator` deployment. The `__call__` method also contains some new code on lines 44 and 45:

```python
translation_ref = self.translator.translate.remote(summary)
translation = ray.get(translation_ref)
```

`self.translator.translate.remote(summary)` issues an asynchronous call to the `Translator`'s `translate` method. Essentially, this line tells Ray to schedule a request to the `Translator` deployment's `translate` method, which can be fulfilled asynchronously. The line immediately returns a reference to the method's output. The next line `ray.get(translation_ref)` waits for `translate` to execute and returns the value of that execution.

We compose our graph in line 50:

```python
deployment_graph = Summarizer.bind(Translator.bind())
```

Here, we bind `Translator` to its (empty) constructor arguments, and then we pass in the bound `Translator` as the constructor argument for the `Summarizer`. We can run this deployment graph using the `serve run` CLI command. Make sure to run this command from a directory containing a local copy of the `graph.py` code:

```console
$ serve run graph:deployment_graph
```

We can use this client script to make requests to the graph:

```{literalinclude} ../serve/doc_code/getting_started/model_graph.py
:start-after: __start_client__
:end-before: __end_client__
:language: python
```

While the graph is running, we can open a separate terminal window and run the client script:

```console
$ python graph_client.py

c'était le meilleur des temps, c'était le pire des temps .
```

Deployment graphs are useful since they let you deploy each part of your machine learning pipeline, such as inference and business logic steps, in separate deployments. Each of these deployments can be individually configured and scaled, ensuring you get maximal performance from your resources.

## Next Steps

- Dive into the {doc}`key-concepts` to get a deeper understanding of Ray Serve.
- Learn more about how to deploy your Ray Serve application to a multi-node cluster: {ref}`serve-deploy-tutorial`.
- See the guide on [putting Ray Serve in production](serve-in-production) to learn more about how to manage your deployments.
- Check more in-depth tutorials for popular machine learning frameworks: {doc}`tutorials/index`.

```{rubric} Footnotes
```

[^f1]: [Starlette](https://www.starlette.io/) is a web server framework
    used by Ray Serve. Its [Request](https://www.starlette.io/requests/) class
    provides a nice interface for incoming HTTP requests.
