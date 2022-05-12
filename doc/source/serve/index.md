```{eval-rst}
.. include:: /_includes/serve/announcement.rst
```

(rayserve)=

# Serve: Scalable and Programmable Serving

:::{tip}
Get in touch with us if you're using or considering using [Ray Serve](https://docs.google.com/forms/d/1l8HT35jXMPtxVUtQPeGoe09VGp5jcvSv0TqPgyz6lGU).
:::

```{image} logo.svg
:align: center
:height: 250px
:width: 400px
```

(rayserve-overview)=

Ray Serve is an easy-to-use scalable model serving library built on Ray.  Ray Serve is:

- **Framework-agnostic**: Use a single toolkit to serve everything from deep learning models
  built with frameworks like [PyTorch](serve-pytorch-tutorial),
  [Tensorflow, and Keras](serve-tensorflow-tutorial), to [Scikit-Learn](serve-sklearn-tutorial) models, to arbitrary Python business logic.
- **Python-first**: Configure your model serving declaratively in pure Python, without needing YAML or JSON configs.

Ray Serve enables composing multiple ML models into a [deployment graph](serve-deployment-graph). This allows you to write a complex inference service consisting of multiple ML models and business logic all in Python code.

Since Ray Serve is built on Ray, it allows you to easily scale to many machines, both in your datacenter and in the cloud.

Ray Serve can be used in two primary ways to deploy your models at scale:

1. Have Python functions and classes automatically placed behind HTTP endpoints.
2. Alternatively, call them from [within your existing Python web server](serve-web-server-integration-tutorial) using the Python-native {ref}`servehandle-api`.

:::{note}
Serve recently added an experimental API for building deployment graphs of multiple models.
Please take a look at the [Deployment Graph API](serve-deployment-graph) and try it out!
:::

:::{tip}
Chat with Ray Serve users and developers on our [forum](https://discuss.ray.io/)!
:::

(serve-quickstart)=

## Ray Serve Quickstart

First install Ray Serve and all of its dependencies by running the following
command in your terminal:

```bash
pip install "ray[serve]"
```

:::{note}
Ray Serve supports the same Python versions as Ray. See {ref}`installation`
for a list of supported Python versions.
:::

Now we will write a Python script to serve a simple "Counter" class over HTTP.  You may open an interactive Python terminal and copy in the lines below as we go.

First, import Ray and Ray Serve:

```python
import ray
from ray import serve
```

Ray Serve runs on top of a Ray cluster, so the next step is to start a local Ray cluster:

```python
ray.init()
```

:::{note}
`ray.init()` will start a single-node Ray cluster on your local machine, which will allow you to use all your CPU cores to serve requests in parallel.  To start a multi-node cluster, see {doc}`../cluster/index`.
:::

Next, start the Ray Serve runtime:

```python
serve.start()
```

:::{warning}
When the Python script exits, Ray Serve will shut down.
If you would rather keep Ray Serve running in the background you can use `serve.start(detached=True)` (see {doc}`deployment` for details).
:::

Now we will define a simple Counter class. The goal is to serve this class behind an HTTP endpoint using Ray Serve.

By default, Ray Serve offers a simple HTTP proxy that will send requests to the class' `__call__` method. The argument to this method will be a Starlette `Request` object.

```python
@serve.deployment
class Counter:
  def __init__(self):
      self.count = 0

  def __call__(self, request):
      self.count += 1
      return {"count": self.count}
```

:::{note}
Besides classes, you can also serve standalone functions with Ray Serve in the same way.
:::

Notice that we made this class into a `Deployment` with the {mod}`@serve.deployment <ray.serve.api.deployment>` decorator.
This decorator is where we could set various configuration options such as the number of replicas, unique name of the deployment (it defaults to the class name), or the HTTP route prefix to expose the deployment at.
See the {mod}`Deployment package reference <ray.serve.api.Deployment>` for more details.
In order to deploy this, we simply need to call `Counter.deploy()`.

```python
Counter.deploy()
```

:::{note}
Deployments can be configured to improve performance, for example by increasing the number of replicas of the class being served in parallel.  For details, see {ref}`configuring-a-deployment`.
:::

Now that our deployment is up and running, let's test it out by making a query over HTTP.
In your browser, simply visit `http://127.0.0.1:8000/Counter`, and you should see the output `{"count": 1"}`.
If you keep refreshing the page, the count should increase, as expected.

Now let's say we want to update this deployment to add another method to decrement the counter.
Here, because we want more flexible HTTP configuration we'll use Serve's FastAPI integration.
For more information on this, please see {ref}`serve-fastapi-http`.

```python
from fastapi import FastAPI

app = FastAPI()

@serve.deployment
@serve.ingress(app)
class Counter:
  def __init__(self):
      self.count = 0

  @app.get("/")
  def get(self):
      return {"count": self.count}

  @app.get("/incr")
  def incr(self):
      self.count += 1
      return {"count": self.count}

  @app.get("/decr")
  def decr(self):
      self.count -= 1
      return {"count": self.count}
```

We've now redefined the `Counter` class to wrap a `FastAPI` application.
This class is exposing three HTTP routes: `/Counter` will get the current count, `/Counter/incr` will increment the count, and `/Counter/decr` will decrement the count.

To redeploy this updated version of the `Counter`, all we need to do is run `Counter.deploy()` again.
Serve will perform a rolling update here to replace the existing replicas with the new version we defined.

```python
Counter.deploy()
```

If we test out the HTTP endpoint again, we can see this in action.
Note that the count has been reset to zero because the new version of `Counter` was deployed.

```bash
> curl -X GET localhost:8000/Counter/
{"count": 0}
> curl -X GET localhost:8000/Counter/incr
{"count": 1}
> curl -X GET localhost:8000/Counter/decr
{"count": 0}
```

Congratulations, you just built and ran your first Ray Serve application! You should now have enough context to dive into the {doc}`core-apis` to get a deeper understanding of Ray Serve.
For more interesting example applications, including integrations with popular machine learning frameworks and Python web servers, be sure to check out {doc}`tutorials/index`.
For a high-level view of the architecture underlying Ray Serve, see {doc}`architecture`.

## Why Ray Serve?

There are generally two ways of serving machine learning applications, both with serious limitations:
you can use a **traditional web server**---your own Flask app---or you can use a cloud-hosted solution.

The first approach is easy to get started with, but it's hard to scale each component. The second approach
requires vendor lock-in (SageMaker), framework-specific tooling (TFServing), and a general
lack of flexibility.

Ray Serve solves these problems by giving you a simple web server (and the ability to [use your own](serve-web-server-integration-tutorial)) while still handling the complex routing, scaling, and testing logic
necessary for production deployments.

Beyond scaling up your deployments with multiple replicas, Ray Serve also enables:

- {ref}`serve-model-composition`---ability to flexibly compose multiple models and independently scale and update each.
- {ref}`serve-batching`---built in request batching to help you meet your performance objectives.
- {ref}`serve-cpus-gpus`---specify fractional resource requirements to fully saturate each of your GPUs with several models.

For more on the motivation behind Ray Serve, check out these [meetup slides](https://tinyurl.com/serve-meetup) and this [blog post](https://medium.com/distributed-computing-with-ray/machine-learning-serving-is-broken-f59aff2d607f).

### When should I use Ray Serve?

Ray Serve is a flexible tool that's easy to use for deploying, operating, and monitoring Python-based machine learning applications.
Ray Serve excels when you want to mix business logic with ML models and scaling out in production is a necessity. This might be because of large-scale batch processing
requirements or because you want to scale up a deployment graph consisting of many individual models with different performance properties.

If you plan on running on multiple machines, Ray Serve will serve you well!

## What's next?

Check out {ref}`getting-started` and {doc}`core-apis`, look at the {ref}`serve-faq`,
or head over to the {doc}`tutorials/index` to get started building your Ray Serve applications.

For more, see the following blog posts about Ray Serve:

- [Serving ML Models in Production: Common Patterns](https://www.anyscale.com/blog/serving-ml-models-in-production-common-patterns) by Simon Mo, Edward Oakes, and Michael Galarnyk
- [How to Scale Up Your FastAPI Application Using Ray Serve](https://medium.com/distributed-computing-with-ray/how-to-scale-up-your-fastapi-application-using-ray-serve-c9a7b69e786) by Archit Kulkarni
- [Machine Learning is Broken](https://medium.com/distributed-computing-with-ray/machine-learning-serving-is-broken-f59aff2d607f) by Simon Mo
- [The Simplest Way to Serve your NLP Model in Production with Pure Python](https://medium.com/distributed-computing-with-ray/the-simplest-way-to-serve-your-nlp-model-in-production-with-pure-python-d42b6a97ad55) by Edward Oakes and Bill Chambers

```{eval-rst}
.. include:: /_includes/serve/announcement_bottom.rst
```
