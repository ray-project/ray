```{eval-rst}
.. include:: /_includes/serve/announcement.rst
```

(rayserve)=

# Serve: Scalable and Programmable Serving

:::{tip}
[Get in touch with us](https://docs.google.com/forms/d/1l8HT35jXMPtxVUtQPeGoe09VGp5jcvSv0TqPgyz6lGU) if you're using or considering using Ray Serve.

Chat with Ray Serve users and developers on our [forum](https://discuss.ray.io/).
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

Serve is particularly well suited for {ref}`serve-model-composition`, enabling you to build a complex inference service consisting of multiple ML models and business logic all in Python code.

Serve is built on top of Ray, so it easily scales to many machines and offers flexible scheduling support such as fractional GPUs so you can share resources and serve many machine learning models at low cost.

:::{tabbed} Installation

Install Ray Serve and its dependencies:

```bash
pip install "ray[serve]"
```
:::

:::{tabbed} Quickstart

To run this example, install the following: ``pip install ray["serve"]``

In this quick-start example we will define a simple "hello world" deployment, deploy it behind HTTP locally, and query it.

```{literalinclude} doc_code/quickstart.py
:language: python
```
:::

:::{tabbed} FastAPI integration

To run this example, install the following: ``pip install ray["serve"]``

In this example we will use Serve's [FastAPI](https://fastapi.tiangolo.com/) integration to make use of more advanced HTTP functionality.

```{literalinclude} doc_code/fastapi_example.py
:language: python
```
:::

:::{tabbed} Serving a Hugging Face NLP model

To run this example, install the following: ``pip install ray["serve"] transformers``

In this example we will serve a pre-trained [Hugging Face transformers](https://huggingface.co/docs/transformers/index) model using Ray Serve.
The model we'll use is a sentiment analysis model: it will take a text string as input and return if the text was "POSITIVE" or "NEGATIVE."

```{literalinclude} doc_code/transformers_example.py
:language: python
```
:::

## Why choose Serve?

:::{dropdown} Build end-to-end ML-powered applications
:animate: fade-in-slide-down

Many solutions for ML serving focus on "tensor-in, tensor-out" serving: that is, they wrap ML models behind a predefined, structured endpoint.
However, machine learning isn't useful in isolation!
It's often important to combine machine learning with business logic and traditional web serving logic such as database queries.

Ray Serve is unique in that it allows you to build and deploy an end-to-end distributed serving application in a single framework.
You can combine multiple ML models, business logic, and expressive HTTP handling using Serve's FastAPI integration (see {ref}`serve-fastapi-http`) to build your entire application as one Python program.

:::

:::{dropdown} Combine multiple models using a programmable API
:animate: fade-in-slide-down

Often solving a problem requires more than just a single machine learning model.
For instance, image processing applications typically require a multi-stage pipeline consisting of steps like preprocessing, segmentation, and filtering in order to achieve their end goal.
In many cases each model may use a different architecture or framework and require different resources (e.g., CPUs vs GPUs).

Many other solutions support defining a static graph in YAML or some other configuration language.
This can be limiting and hard to work with.
Ray Serve, on the other hand, supports multi-model composition using a programmable API where calls to different models look just like function calls.
The models can use different resources and run across different machines in the cluster, but to the developer it's just like writing a regular program (see {ref}`serve-model-composition` for more details).

:::

:::{dropdown} Flexibly scale up and allocate resources
:animate: fade-in-slide-down

Machine learning models are compute-intensive and therefore can be very expensive to operate.
A key requirement for any ML serving system is being able to dynamically scale up and down and allocate the right resources for each model to handle the request load while saving cost.

Serve offers a number of built-in primitives to help make your ML serving application efficient.
It supports dynamically scaling the resources for a model up and down by adjusting the number of replicas, batching requests to take advantage of efficient vectorized operations (especially important on GPUs), and a flexible resource allocation model that enables you to serve many models on limited hardware resources.

:::

:::{dropdown} Avoid framework or vendor lock-in
:animate: fade-in-slide-down

Machine learning moves fast, with new libraries and model architectures being released all the time, it's important to avoid locking yourself into a solution that is tied to a specific framework.
This is particularly important in serving, where making changes to your infrastructure can be time consuming, expensive, and risky.
Additionally, many hosted solutions are limited to a single cloud provider which can be a problem in today's multi-cloud world.

Ray Serve is not tied to any specific machine learning library or framework, but rather provides a general-purpose scalable serving layer.
Because it's built on top of Ray, you can run it anywhere Ray can: on your laptop, Kubernetes, any major cloud provider, or even on-premise.

:::

## Learn More

Check out {ref}`getting-started` and {doc}`core-apis`, look at the {ref}`serve-faq`,
or head over to the {doc}`tutorials/index` to get started building your Ray Serve applications.

For more, see the following blog posts about Ray Serve:

- [Serving ML Models in Production: Common Patterns](https://www.anyscale.com/blog/serving-ml-models-in-production-common-patterns) by Simon Mo, Edward Oakes, and Michael Galarnyk
- [The Simplest Way to Serve your NLP Model in Production with Pure Python](https://medium.com/distributed-computing-with-ray/the-simplest-way-to-serve-your-nlp-model-in-production-with-pure-python-d42b6a97ad55) by Edward Oakes and Bill Chambers
- [Machine Learning Serving is Broken](https://medium.com/distributed-computing-with-ray/machine-learning-serving-is-broken-f59aff2d607f) by Simon Mo
- [How to Scale Up Your FastAPI Application Using Ray Serve](https://medium.com/distributed-computing-with-ray/how-to-scale-up-your-fastapi-application-using-ray-serve-c9a7b69e786) by Archit Kulkarni

```{eval-rst}
.. include:: /_includes/serve/announcement_bottom.rst
```
