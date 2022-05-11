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

Ray Serve is a scalable model serving library for building online inference APIs.
Serve is framework agnostic, so you can use a single toolkit to serve everything from deep learning models built with frameworks like [PyTorch](serve-pytorch-tutorial),
  [Tensorflow, and Keras](serve-tensorflow-tutorial), to [Scikit-Learn](serve-sklearn-tutorial) models, to arbitrary Python business logic.
You can also write a complex inference service consisting of multiple ML models and business logic all in Python code using the [deployment graph](serve-deployment-graph) API.
Serve is built on Ray, so it easily scales to many machines and offers flexible scheduling support such as fractional GPUs so you can share resources and serve many machine learning models at low cost.

:::{tip}
Chat with Ray Serve users and developers on our [forum](https://discuss.ray.io/)!
:::

:::{tabbed} Installation

Install Ray Serve and its dependencies:

```bash
pip install "ray[serve]"
```
:::

:::{tabbed} Quickstart

To run this example, install the following: ``pip install ray["serve"]``.

In this quick-start example we will define a simple "hello world" deployment, deploy it behind HTTP locally, and query it.

```{literalinclude} doc_code/quickstart.py
:language: python
```
:::

:::{tabbed} FastAPI integration

To run this example, install the following: ``pip install ray["serve"]``.

In this example we will use Serve's [FastAPI](https://fastapi.tiangolo.com/) integration to make use of more advanced HTTP functionality.

```{literalinclude} doc_code/fastapi_example.py
:language: python
```
:::

:::{tabbed} Serving a Hugging Face NLP model

To run this example, install the following: ``pip install ray["serve"] transformers``.

In this example we will serve a pre-trained [Hugging Face transformers](https://huggingface.co/docs/transformers/index) model using Ray Serve.
The model we'll use is a sentiment analysis model: it will take a text string as input and return if the text was "POSITIVE" or "NEGATIVE."

```{literalinclude} doc_code/transformers_example.py
:language: python
```
:::

## Why choose Serve?

:::{dropdown} TODO: Testing this out

Here's the relevant text.

:::

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

Check out the {ref}`end-to-end-tutorial` and {doc}`core-apis`, look at the {ref}`serve-faq`,
or head over to the {doc}`tutorials/index` to get started building your Ray Serve applications.

For more, see the following blog posts about Ray Serve:

- [Serving ML Models in Production: Common Patterns](https://www.anyscale.com/blog/serving-ml-models-in-production-common-patterns) by Simon Mo, Edward Oakes, and Michael Galarnyk
- [How to Scale Up Your FastAPI Application Using Ray Serve](https://medium.com/distributed-computing-with-ray/how-to-scale-up-your-fastapi-application-using-ray-serve-c9a7b69e786) by Archit Kulkarni
- [Machine Learning is Broken](https://medium.com/distributed-computing-with-ray/machine-learning-serving-is-broken-f59aff2d607f) by Simon Mo
- [The Simplest Way to Serve your NLP Model in Production with Pure Python](https://medium.com/distributed-computing-with-ray/the-simplest-way-to-serve-your-nlp-model-in-production-with-pure-python-d42b6a97ad55) by Edward Oakes and Bill Chambers

```{eval-rst}
.. include:: /_includes/serve/announcement_bottom.rst
```
