# Scale a Gradio App with Ray Serve

This guide shows how to scale up your [Gradio](https://gradio.app/) application using Ray Serve. Keep the internal architecture of your Gradio app intact, with no code changes. Simply wrap the app within Ray Serve as a deployment and scale it to access more resources.
## Dependencies

To follow this tutorial, you need Ray Serve and Gradio. If you haven't already, install them by running:
```console
$ pip install "ray[serve]"
$ pip install gradio==3.19
```
This tutorial uses Gradio apps that run text summarization and generation models and use [Hugging Face's Pipelines](https://huggingface.co/docs/transformers/main_classes/pipelines) to access these models. **Note that you can substitute this Gradio app for any Gradio app of your own.**

First, install the transformers module.
```console
$ pip install transformers
```

## Quickstart: Deploy your Gradio app with Ray Serve

This section shows an easy way to deploy your app onto Ray Serve. First, create a new Python file named `demo.py`. Second, import `GradioServer` from Ray Serve to deploy your Gradio app later, `gradio`, and `transformers.pipeline` to load text summarization models.
```{literalinclude} ../doc_code/gradio-integration.py
:start-after: __doc_import_begin__
:end-before: __doc_import_end__
```

Then, write a builder function that constructs the Gradio app `io`. This application takes in text and uses the [T5 Small](https://huggingface.co/t5-small) text summarization model loaded using [Hugging Face's Pipelines](https://huggingface.co/docs/transformers/main_classes/pipelines) to summarize that text.
:::{note} 
Remember you can substitute this app with your own Gradio app if you want to try scaling up your own Gradio app.
:::
```{literalinclude} ../doc_code/gradio-integration.py
:start-after: __doc_gradio_app_begin__
:end-before: __doc_gradio_app_end__
```

### Deploying Gradio Server
In order to deploy your Gradio app onto Ray Serve, you need to wrap your Gradio app in a Serve [deployment](serve-key-concepts-deployment). `GradioServer` acts as that wrapper. It serves your Gradio app remotely on Ray Serve so that it can process and respond to HTTP requests. 

By wrapping your application in `GradioServer`, you can increase the number of CPUs and/or GPUs available to the application.
:::{note}
Ray Serve doesn't support routing requests to multiple replicas of `GradioServer`, so you should only have a single replica.
:::

:::{note} 
`GradioServer` is simply `GradioIngress` but wrapped in a Serve deployment. You can use `GradioServer` for the simple wrap-and-deploy use case, but in the next section, you can use `GradioIngress` to define your own Gradio Server for more customized use cases.
:::

:::{note} 
Ray can’t pickle Gradio. Instead, pass a builder function that constructs the Gradio interface.
:::

Using either the Gradio app `io`, which the builder function constructed, or your own Gradio app of type `Interface`, `Block`, `Parallel`, etc., wrap the app in your Gradio Server. Pass the builder function as input to your Gradio Server. Ray Serves uses the builder function to construct your Gradio app on the Ray cluster.

```{literalinclude} ../doc_code/gradio-integration.py
:start-after: __doc_app_begin__
:end-before: __doc_app_end__
```

Finally, deploy your Gradio Server. Run the following in your terminal:
```console
$ serve run demo:app
```

Access your Gradio app at `http://localhost:8000` The output should look like the following image:
![Gradio Result](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/gradio_result.png)

See the [Production Guide](serve-in-production) for more information on how to deploy your app in production.


## Parallelizing models with Ray Serve
You can run multiple models in parallel with Ray Serve by using [model composition](serve-model-composition) in Ray Serve.

### Original approach
Suppose you want to run the following program.

1. Take two text generation models, [`gpt2`](https://huggingface.co/gpt2) and [`distilgpt2`](https://huggingface.co/distilgpt2).
2. Run the two models on the same input text, so that the generated text has a minimum length of 20 and maximum length of 100.
3. Display the outputs of both models using Gradio.

This code is a typical implementation:

```{literalinclude} ../doc_code/gradio-original.py
:start-after: __doc_code_begin__
:end-before: __doc_code_end__
```
Launch the Gradio app with this command:
```
demo.launch()
```

### Parallelize using Ray Serve

With Ray Serve, you can parallelize the two text generation models by wrapping each model in a separate Ray Serve [deployment](serve-key-concepts-deployment). You can define deployments by decorating a Python class or function with `@serve.deployment`. The deployments usually wrap the models that you want to deploy on Ray Serve to handle incoming requests.

Follow these steps to achieve parallelism. First, import the dependencies. Note that you need to import `GradioIngress` instead of `GradioServer` like before because in this case, you're building a customized `MyGradioServer` that can run models in parallel.

```{literalinclude} ../doc_code/gradio-integration-parallel.py
:start-after: __doc_import_begin__
:end-before: __doc_import_end__
```

Then, wrap the `gpt2` and `distilgpt2` models in Serve deployments, named `TextGenerationModel`.
```{literalinclude} ../doc_code/gradio-integration-parallel.py
:start-after: __doc_models_begin__
:end-before: __doc_models_end__
```

Next, instead of simply wrapping the Gradio app in a `GradioServer` deployment, build your own `MyGradioServer` that reroutes the Gradio app so that it runs the `TextGenerationModel` deployments.

```{literalinclude} ../doc_code/gradio-integration-parallel.py
:start-after: __doc_gradio_server_begin__
:end-before: __doc_gradio_server_end__
```

Lastly, link everything together:
```{literalinclude} ../doc_code/gradio-integration-parallel.py
:start-after: __doc_app_begin__
:end-before: __doc_app_end__
```

:::{note} 
This step binds the two text generation models, which you wrapped in Serve deployments, to `MyGradioServer._d1` and `MyGradioServer._d2`, forming a [model composition](serve-model-composition). In the example, the Gradio Interface `io` calls `MyGradioServer.fanout()`, which sends requests to the two text generation models that you deployed on Ray Serve.
:::

Now, you can run your scalable app, to serve the two text generation models in parallel on Ray Serve.
Run your Gradio app with the following command:

```console
$ serve run demo:app
```

Access your Gradio app at `http://localhost:8000`, and you should see the following interactive interface:
![Gradio Result](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/gradio_result_parallel.png)

See the [Production Guide](serve-in-production) for more information on how to deploy your app in production.
