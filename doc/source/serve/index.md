```{eval-rst}
.. include:: /_includes/serve/announcement.rst
```

(rayserve)=

# Serve: Scalable and Programmable Serving

:::{tip}
[Get in touch with us](https://docs.google.com/forms/d/1l8HT35jXMPtxVUtQPeGoe09VGp5jcvSv0TqPgyz6lGU) if you're using or considering using Ray Serve.
:::

```{image} logo.svg
:align: center
:height: 250px
:width: 400px
```

(rayserve-overview)=

Ray Serve is a scalable model serving library for building online inference APIs.
Serve is framework agnostic, so you can use a single toolkit to serve everything from deep learning models built with frameworks like PyTorch, Tensorflow, and Keras, to Scikit-Learn models, to arbitrary Python business logic.

Serve is particularly well suited for {ref}`serve-model-composition`, enabling you to build a complex inference service consisting of multiple ML models and business logic all in Python code.

Serve is built on top of Ray, so it easily scales to many machines and offers flexible scheduling support such as fractional GPUs so you can share resources and serve many machine learning models at low cost.

## Quickstart

Install Ray Serve and its dependencies:

```bash
pip install "ray[serve]"
```

To run this example, install the following: ``pip install ray["serve"]``

In this quick-start example we will define a simple "hello world" deployment, deploy it behind HTTP locally, and query it.

```{literalinclude} doc_code/quickstart.py
:language: python
```

:::{tabbed} More examples
For more examples, select from the tabs.
:::

:::{tabbed} Model composition

In this example we demonstrate Serve's model composition API allowing you to express complex computation graph and deploy them as a unit.

```{literalinclude} doc_code/quickstart_graph.py
:language: python
```
:::

:::{tabbed} FastAPI integration

In this example we will use Serve's [FastAPI](https://fastapi.tiangolo.com/) integration to make use of more advanced HTTP functionality.

```{literalinclude} doc_code/fastapi_example.py
:language: python
```
:::

:::{tabbed} HuggingFace model

To run this example, install the following: ``pip install transformers``

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


## How can Serve help me as a...

:::{dropdown} Data scientist
:animate: fade-in-slide-down

Serve is easy to use. You can test your models (and your entire deployment graph) on your local machine before deploying it to production on a cluster. You don't need to know heavyweight Kubernetes concepts or cloud providers to use Serve.

:::

:::{dropdown} ML engineer
:animate: fade-in-slide-down

Serve helps you scale out the deployment and run them reliably in an efficient, cost-saving manner. Serve offers a first-class model composition API to help you combine models together to build end-to-end application. Additionally, Serve natively run on Kubernetes which minimal operation overhead.

:::

:::{dropdown} ML Platform engineer
:animate: fade-in-slide-down

Serve specializes in ML model serving and can be a reliably component in your ML Platform stack.
Serve supports arbitrary Python code and therefore integrates well with the MLOps ecosystem. You can use it with model optimizers (ONNX, TVM), model monitoring systems (Seldon Alibi, Arize), model registries (MLFlow, Weights and Biases), machine learning frameworks (XGBoost, Scikit-learn), data app UIs (Gradio, Streamlit), and Web API frameworks (FastAPI, gRPC).

:::


## How does Serve compare to X

:::{dropdown} TFServing, TorchServe, ONNXRuntime
:animate: fade-in-slide-down

Ray Serve is *framework agnostic*, you can use any Python framework and libraries.
We believe data scientists are not bounded a particular machine learning framework.
They use the best tool available for the job.

Compared to these framework specific solution, Ray Serve doesn't perform any optimizations
to make your ML model run faster. However, you can still optimize the models yourself
and run them in Ray Serve: for example, you can run a model compiled by
[PyTorch JIT](https://pytorch.org/docs/stable/jit.html).
:::

:::{dropdown} AWS SageMaker, Azure ML, Google Vertex AI
:animate: fade-in-slide-down

Ray Serve brings the scalability and parallelism of these hosted offering to
your own infrastructure. You can use our [cluster launcher](cluster-cloud)
to deploy Ray Serve to all major public clouds, K8s, as well as on bare-metal, on-premise machines.

Compared to these offerings, Ray Serve lacks a unified user interface and functionality
let you manage the lifecycle of the models, visualize it's performance, etc. Ray
Serve focuses on just model serving and provides the primitives for you to
build your own ML platform on top.
:::

:::{dropdown} Seldon, KServe, Cortex
:animate: fade-in-slide-down

You can develop Ray Serve on your laptop, deploy it on a dev box, and scale it out
to multiple machines or K8s cluster without changing one lines of code. It's a lot
easier to get started with when you don't need to provision and manage K8s cluster.
When it's time to deploy, you can use the our native Kubernetes Operator
to transparently put your Ray Serve application in K8s.
:::

:::{dropdown} BentoML, Comet.ml, MLflow
:animate: fade-in-slide-down

Ray Serve is a special purpose distributed model server built for large scale applications.
This means we can work with any model packaging and registry format. Many of the tools are
focused on serving one models. Ray Serve is built for end-to-end machine learning application
in mind with our unique model composition API and advanced autoscaling capabilities.

:::

We truly believe Serve is unique as it gives you end to end control
over the API while delivering scalability and high performance. To achieve
something like what Serve offers, you often need to glue together multiple
frameworks like Tensorflow Serving, SageMaker, or even roll your own
batching server.

## Learn More

Check out {ref}`getting-started` and {ref}`serve-key-concepts`,
or head over to the {doc}`tutorials/index` to get started building your Ray Serve applications.


```{eval-rst}
.. panels::
    :container: text-center
    :column: col-lg-6 px-2 py-2
    :card:

    **Getting Started**
    ^^^

    Start with our quick start tutorials for :ref:`deploying a single model locally<getting-started>` and how to :ref:`convert an existing model into a Ray Serve deployment<converting-to-ray-serve-deployment>` .

    +++
    .. link-button:: getting-started
        :type: ref
        :text: Get Started with Ray Serve
        :classes: btn-outline-info btn-block
    ---

    **Key Concepts**
    ^^^

    Understand the key concepts behind Ray Serve.
    Learn about :ref:`Deployments<serve-key-concepts-deployment>`, :ref:`how to query them<serve-key-concepts-query-deployment>`, and the :ref:`Deployment Graph<serve-key-concepts-deployment-graph>` API for composing models into a graph structure.

    +++
    .. link-button:: serve-key-concepts
        :type: ref
        :text: Learn Key Concepts
        :classes: btn-outline-info btn-block
    ---

    **User Guides**
    ^^^
    Learn best practices for common patterns like :doc:`managing deployments<managing-deployments>`, how to call deployments :ref:`via HTTP<serve-http>` or :ref:`from Python<serve-handle-explainer>`.
    Learn how to serve multiple ML models with :ref:`Model Ensemble<serve-model-ensemble>`, and how to :ref:`monitor your Serve applications<serve-monitoring>`.

    +++
    .. link-button:: serve-user-guides
        :type: ref
        :text: Start Using Ray Serve
        :classes: btn-outline-info btn-block
    ---

    **Examples**
    ^^^

    Follow the tutorials to learn how to integrate Ray Serve with :ref:`TensorFlow<serve-ml-models-tutorial>`, :ref:`Scikit-Learn<serve-ml-models-tutorial>`, and :ref:`RLlib<serve-rllib-tutorial>`.

    +++
    .. link-button:: serve-examples
        :type: ref
        :text: Serve Examples
        :classes: btn-outline-info btn-block
    ---

    **API Reference**
    ^^^

    Get more in-depth information about the Ray Serve API.

    +++
    .. link-button:: serve-api
        :type: ref
        :text: Read the API Reference
        :classes: btn-outline-info btn-block

    ---

    **Serve Architecture**
    ^^^

    Understand how each component in Ray Serve works.

    +++
    .. link-button:: serve-architecture
        :type: ref
        :text: Understand Serve Architecture
        :classes: btn-outline-info btn-block
```

For more, see the following blog posts about Ray Serve:

- [Serving ML Models in Production: Common Patterns](https://www.anyscale.com/blog/serving-ml-models-in-production-common-patterns) by Simon Mo, Edward Oakes, and Michael Galarnyk
- [The Simplest Way to Serve your NLP Model in Production with Pure Python](https://medium.com/distributed-computing-with-ray/the-simplest-way-to-serve-your-nlp-model-in-production-with-pure-python-d42b6a97ad55) by Edward Oakes and Bill Chambers
- [Machine Learning Serving is Broken](https://medium.com/distributed-computing-with-ray/machine-learning-serving-is-broken-f59aff2d607f) by Simon Mo
- [How to Scale Up Your FastAPI Application Using Ray Serve](https://medium.com/distributed-computing-with-ray/how-to-scale-up-your-fastapi-application-using-ray-serve-c9a7b69e786) by Archit Kulkarni

```{eval-rst}
.. include:: /_includes/serve/announcement_bottom.rst
```
