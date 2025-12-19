(serve-getting-started)=

# Getting Started

This tutorial walks you through the process of writing and testing a Ray Serve application. It shows you how to

* convert a machine learning model to a Ray Serve deployment
* test a Ray Serve application locally over HTTP
* compose multi-model machine learning models together into a single application

This tutorial uses two models:

* [HuggingFace's TranslationPipeline](https://huggingface.co/docs/transformers/main_classes/pipelines#transformers.TranslationPipeline) as a text-translation model
* [HuggingFace's SummarizationPipeline](https://huggingface.co/docs/transformers/v4.21.0/en/main_classes/pipelines#transformers.SummarizationPipeline) as a text-summarizer model

You can also follow along using your own models from any Python framework.

After deploying those two models, test them with HTTP requests.

:::{tip}
If you have suggestions on how to improve this tutorial,
    [let the Ray team know](https://github.com/ray-project/ray/issues/new/choose)!
:::

To run this example, you need to install the following:

```bash
pip install "ray[serve]" transformers requests torch
```


## Text Translation Model (before Ray Serve)

First, take a look at the text-translation model. Here's its code:

```{literalinclude} ../serve/doc_code/getting_started/models.py
:start-after: __start_translation_model__
:end-before: __end_translation_model__
:language: python
```

The Python file, called `model.py`, uses the `Translator` class to translate English text to French.

- The `self.model` variable inside `Translator`'s `__init__` method
  stores a function that uses the [t5-small](https://huggingface.co/t5-small)
  model to translate text.
- When `self.model` is called on English text, it returns translated French text
  inside a dictionary formatted as `[{"translation_text": "..."}]`.
- The `Translator`'s `translate` method extracts the translated text by indexing into the dictionary.

You can copy-paste this script and run it locally. It translates `"Hello world!"`
into `"Bonjour Monde!"`.

```console
$ python model.py

Bonjour Monde!
```

Keep in mind that the `TranslationPipeline` is an example ML model for this
tutorial. You can follow along using arbitrary models from any
Python framework. Check out the tutorials on scikit-learn,
PyTorch, and TensorFlow for more info and examples:

- {ref}`serve-ml-models-tutorial`

(converting-to-ray-serve-application)=
## Converting to a Ray Serve Application

In this section, deploy the text translation model using Ray Serve, so
it can be scaled up and queried over HTTP. Start by converting
`Translator` into a Ray Serve deployment.

First, open a new Python file and import `ray` and `ray.serve`:

```{literalinclude} ../serve/doc_code/getting_started/model_deployment.py
:start-after: __import_start__
:end-before: __import_end__
:language: python
```

After these imports, include the model code from preceding:

```{literalinclude} ../serve/doc_code/getting_started/model_deployment.py
:start-after: __model_start__
:end-before: __model_end__
:language: python
```

The `Translator` class has two modifications:
1. It has a decorator, `@serve.deployment`.
2. It has a new method, `__call__`.

The decorator converts `Translator` from a Python class into a Ray Serve `Deployment` object.

Each deployment stores a single Python function or class that you write and uses
it to serve requests. You can scale and configure each of your deployments independently using
parameters in the `@serve.deployment` decorator. The example configures a few common parameters:

* `num_replicas`: an integer that determines how many copies of the deployment process run in Ray. Requests are load balanced across these replicas, allowing you to scale your deployments horizontally.
* `ray_actor_options`: a dictionary containing configuration options for each replica.
    * `num_cpus`: a float representing the logical number of CPUs each replica should reserve. You can make this a fraction to PACK multiple replicas together on a machine with fewer CPUs than replicas.
    * `num_gpus`: a float representing the logical number of GPUs each replica should reserve. You can make this a fraction to PACK multiple replicas together on a machine with fewer GPUs than replicas.
    * `resources`: a dictionary containing other resource requirements for the replica, such as non-GPU accelerators like HPUs or TPUs.

All these parameters are optional, so feel free to omit them:

```python
...
@serve.deployment
class Translator:
  ...
```

Deployments receive Starlette HTTP `request` objects [^f1]. By default, the deployment class's `__call__` method is called on this `request` object. The return value is sent back in the HTTP response body.

This is why `Translator` needs a new `__call__` method. The method processes the incoming HTTP request by reading its JSON data and forwarding it to the `translate` method. The translated text is returned and sent back through the HTTP response. You can also use Ray Serve's FastAPI integration to avoid working with raw HTTP requests. Check out {ref}`serve-fastapi-http` for more info about FastAPI with Serve.

Next, `bind` the `Translator` deployment to arguments that are passed into its constructor. This defines a Ray Serve application that can be run locally or deployed to production (you see later that applications can consist of multiple deployments). Since `Translator`'s constructor doesn't take in any arguments, call the deployment's `bind` method without passing anything in:

```{literalinclude} ../serve/doc_code/getting_started/model_deployment.py
:start-after: __model_deploy_start__
:end-before: __model_deploy_end__
:language: python
```

With that, you're ready to test the application locally.

## Running a Ray Serve Application

Here's the full Ray Serve script built preceding:

```{literalinclude} ../serve/doc_code/getting_started/model_deployment_full.py
:start-after: __deployment_full_start__
:end-before: __deployment_full_end__
:language: python
```

To test locally, run the script with the `serve run` CLI command. This command takes in an import path
to the deployment formatted as `module:application`. Make sure to run the command from a directory containing a local copy of this script saved as `serve_quickstart.py`, so it can import the application:

```console
$ serve run serve_quickstart:translator_app
```

This command runs the `translator_app` application and then blocks, streaming logs to the console. It can be killed with `Ctrl-C`, which tears down the application.

Test the model over HTTP. It can be reached at the following URL by default:

```
http://127.0.0.1:8000/
```

Send a POST request with JSON data containing English text.
`Translator`'s `__call__` method unpacks this text and forwards it to the
`translate` method. Here's a client script that requests a translation for "Hello world!":

```{literalinclude} ../serve/doc_code/getting_started/model_deployment.py
:start-after: __client_function_start__
:end-before: __client_function_end__
:language: python
```

To test the deployment, first make sure `Translator` is running:

```
$ serve run serve_deployment:translator_app
```

While `Translator` is running, open a separate terminal window and run the client script. This gets a response over HTTP:

```console
$ python model_client.py

Bonjour monde!
```

## Composing Multiple Models

Ray Serve allows you to compose multiple deployments into a single Ray Serve application. This makes it easy to combine multiple machine learning models along with business logic to serve a single request.
Use parameters like `autoscaling_config`, `num_replicas`, `num_cpus`, and `num_gpus` to independently configure and scale each deployment in the application.

For example, deploy a machine learning pipeline with two steps:

1. Summarize English text
2. Translate the summary into French

`Translator` already performs step 2. Use [HuggingFace's SummarizationPipeline](https://huggingface.co/docs/transformers/v4.21.0/en/main_classes/pipelines#transformers.SummarizationPipeline) to accomplish step 1. Here's an example of the `SummarizationPipeline` that runs locally:

```{literalinclude} ../serve/doc_code/getting_started/models.py
:start-after: __start_summarization_model__
:end-before: __end_summarization_model__
:language: python
```

You can copy-paste this script and run it locally. It summarizes the snippet from _A Tale of Two Cities_ to `it was the best of times, it was the worst of times .`

```console
$ python summary_model.py

it was the best of times, it was the worst of times .
```

Here's an application that chains the two models together. The graph takes English text, summarizes it, and then translates it:

```{literalinclude} ../serve/doc_code/getting_started/translator.py
:start-after: __start_graph__
:end-before: __end_graph__
:language: python
```

This script contains the `Summarizer` class converted to a deployment and the `Translator` class with some modifications. In this script, the `Summarizer` class contains the `__call__` method since requests are sent to it first. It also takes in a handle to the `Translator` as one of its constructor arguments, so it can forward summarized texts to the `Translator` deployment. The `__call__` method also contains some new code:

```python
translation = await self.translator.translate.remote(summary)
```

`self.translator.translate.remote(summary)` issues an asynchronous call to the `Translator`'s `translate` method and returns a `DeploymentResponse` object immediately. Calling `await` on the response waits for the remote method call to execute and returns its return value. The response could also be passed directly to another `DeploymentHandle` call.

Define the full application as follows:

```python
app = Summarizer.bind(Translator.bind())
```

Here, bind `Translator` to its (empty) constructor arguments, and then pass in the bound `Translator` as the constructor argument for the `Summarizer`. Run this deployment graph using the `serve run` CLI command. Make sure to run this command from a directory containing a local copy of the `serve_quickstart_composed.py` code:

```console
$ serve run serve_quickstart_composed:app
```

Use this client script to make requests to the graph:

```{literalinclude} ../serve/doc_code/getting_started/translator.py
:start-after: __start_client__
:end-before: __end_client__
:language: python
```

While the application is running, open a separate terminal window and query it:

```console
$ python composed_client.py

c'était le meilleur des temps, c'était le pire des temps .
```

Composed Ray Serve applications let you deploy each part of your machine learning pipeline, such as inference and business logic steps, in separate deployments. Each of these deployments can be individually configured and scaled, ensuring you get maximal performance from your resources. See the guide on [model composition](serve-model-composition) to learn more.

## Next Steps

- Dive into the {doc}`key-concepts` to get a deeper understanding of Ray Serve.
- View details about your Serve application in the Ray Dashboard: {ref}`dash-serve-view`.
- Learn more about how to deploy your Ray Serve application to production: {ref}`serve-in-production`.
- Check more in-depth tutorials for popular machine learning frameworks: {doc}`examples`.

```{rubric} Footnotes
```

[^f1]: [Starlette](https://www.starlette.io/) is a web server framework used by Ray Serve.
