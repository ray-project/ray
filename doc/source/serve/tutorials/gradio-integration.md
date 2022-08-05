# Scaling your Gradio app with Ray Serve

In this guide, we will show you how to scale up your [Gradio](https://gradio.app/) application using Ray Serve. There is no need to change the internal architecture of your Gradio app; instead, we will neatly wrap it with Ray Serve and then scale it up to access more resources.

## Dependencies

To follow this tutorial, you will need Ray Serve and Gradio. If you haven't already, install them by running:
```console
$ pip install "ray[serve]"
$ pip install gradio
```
For the purposes of this tutorial, we will be working with Gradio apps that run text summarization and text generation models. **Note that you can substitute this Gradio app for any Gradio app of your own!**

We will be using [HuggingFace's Pipelines](https://huggingface.co/docs/transformers/main_classes/pipelines) to access the model. First, let's install the transformers module.
```console
$ pip install transformers
```

## Quickstart: Deploy your Gradio app with Ray Serve

This example will show you an easy, straightforward way to deploy your app onto Ray Serve. Start by creating a new Python file named `demo.py` and import `GradioServer` from Ray Serve for deploying your Gradio app, `gradio`, and `transformers.pipeline` for loading text summarization models.
```{literalinclude} ../../../../python/ray/serve/examples/doc/gradio-integration.py
:start-after: __doc_import_begin__
:end-before: __doc_import_end__
```

Then, we construct the (optional) Gradio app `io`:
:::{note} 
Remember you can substitute this with your own Gradio app if you want to try scaling up your own Gradio app!
:::
```{literalinclude} ../../../../python/ray/serve/examples/doc/gradio-integration.py
:start-after: __doc_gradio_app_begin__
:end-before: __doc_gradio_app_end__
```


### Understanding `GradioServer`
In order to deploy your Gradio app onto Ray Serve, you need to wrap your Gradio app in a Serve [deployment](serve-key-concepts-deployment). `GradioServer` acts as that wrapper. It serves your Gradio app remotely on Ray Serve so that it can process and respond to HTTP requests.
:::{note} 
`GradioServer` is simply `GradioIngress` but wrapped in a Serve deployment.
:::
```{literalinclude} ../../../../python/ray/serve/gradio_integrations.py
:start-after: __doc_gradio_ingress_begin__
:end-before: __doc_gradio_ingress_end__
```

### Deploy your Gradio Server
Replicas in a deployment are copies of your program living on Ray Serve, and more replicas means your deployment can serve more client requests. You can increase the number of replicas of your application or increase the number of CPUs and/or GPUs available to each replica.

Then, using either the example we created above, or an existing Gradio app (of type `Interface`, `Block`, `Parallel`, etc.), wrap it in your Gradio Server.

```{literalinclude} ../../../../python/ray/serve/examples/doc/gradio-integration.py
:start-after: __doc_app_begin__
:end-before: __doc_app_end__
```

Finally, deploy your Gradio Server! Run the following in your terminal:
```console
$ serve run demo:app
```

Now you can access your Gradio app at `http://localhost:8000`! This is what it should look like:
![Gradio Result](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/gradio_result.png)

See [Putting Ray Serve Deployment Graphs in Production](https://docs.ray.io/en/master/serve/production.html#id1) for more information on how to deploy your app in production.


## Parallelizing models with Ray Serve
You can run multiple models in parallel with Ray Serve by utilizing the [deployment graph](deployment-graph-e2e-tutorial) in Ray Serve.

### Original Approach
Suppose you want to run the following program.

1. Take two text generation models, [`gpt2`](https://huggingface.co/gpt2) and [`EleutherAI/gpt-neo-125M`](https://huggingface.co/EleutherAI/gpt-neo-125M).
2. Run the two models on the same input text, such that the generated text has a minimum length of 20 and maximum length of 100.
3. Display the outputs of both models using Gradio.

This is how you would do it normally:

```{literalinclude} ../../../../python/ray/serve/examples/doc/gradio-original.py
:start-after: __doc_code_begin__
:end-before: __doc_code_end__
```

### Parallelize using Ray Serve

With Ray Serve, we can parallelize the two text generation models by wrapping each model in a separate Ray Serve [deployment](serve-key-concepts-deployment). Deployments are defined by decorating a Python class or function with `@serve.deployment`, and usually wrap the models that you want to deploy on Ray Serve and handle incoming requests.

First, let's import our dependencies. Note that we need to import `GradioIngress` instead of `GradioServer` like before since we're now building a customized `MyGradioServer` that can run models in parallel.

```{literalinclude} ../../../../python/ray/serve/examples/doc/gradio-integration-parallel.py
:start-after: __doc_import_begin__
:end-before: __doc_import_end__
```

Then, let's wrap our `gpt2` and `EleutherAI/gpt-neo-125M` models in Serve deployments, named `TextGenerationModel`.
```{literalinclude} ../../../../python/ray/serve/examples/doc/gradio-integration-parallel.py
:start-after: __doc_models_begin__
:end-before: __doc_models_end__
```

Next, instead of simply wrapping our Gradio app in a `GradioServer` deployment, we can build our own `MyGradioServer` that reroutes the Gradio app so that it runs the `TextGenerationModel` deployments:

```{literalinclude} ../../../../python/ray/serve/examples/doc/gradio-integration-parallel.py
:start-after: __doc_gradio_server_begin__
:end-before: __doc_gradio_server_end__
```

Lastly, we link everything together:
```{literalinclude} ../../../../python/ray/serve/examples/doc/gradio-integration-parallel.py
:start-after: __doc_app_begin__
:end-before: __doc_app_end__
```

:::{note} 
This will bind your two text generation models (wrapped in Serve deployments) to `MyGradioServer._d1` and `MyGradioServer._d2`, forming a [deployment graph](deployment-graph-e2e-tutorial). Thus, we have built our Gradio Interface `io` such that it calls `MyGradioServer.fanout()`, which simply sends requests to your two text generation models that are deployed on Ray Serve.
:::

Now, you can run your scalable app, and the two text generation models will run in parallel on Ray Serve! Run your Gradio app:

```console
$ serve run demo:app
```

Access your Gradio app at http://localhost:8000. This is what it should look like:
![Gradio Result](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/gradio_result_parallel.png)

See [Putting Ray Serve Deployment Graphs in Production](https://docs.ray.io/en/master/serve/production.html#id1) for more information on how to deploy your app in production.