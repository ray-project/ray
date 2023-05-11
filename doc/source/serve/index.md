```{eval-rst}
.. include:: /_includes/serve/announcement.rst
```

(rayserve)=

# Ray Serve: Scalable and Programmable Serving

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
Serve is framework-agnostic, so you can use a single toolkit to serve everything from deep learning models built with frameworks like PyTorch, Tensorflow, and Keras, to Scikit-Learn models, to arbitrary Python business logic.

Serve is particularly well suited for [model composition](serve-model-composition), enabling you to build a complex inference service consisting of multiple ML models and business logic all in Python code.

Serve is built on top of Ray, so it easily scales to many machines and offers flexible scheduling support such as fractional GPUs so you can share resources and serve many machine learning models at low cost.

## Quickstart

Install Ray Serve and its dependencies:

```bash
pip install "ray[serve]"
```
Define a simple "hello world" application, run it locally, and query it over HTTP.

```{literalinclude} doc_code/quickstart.py
:language: python
```

::::{tab-set} 

:::{tab-item} More examples

For more examples, select from the tabs.

:::

::::

::::{tab-set}

:::{tab-item} Model composition

Use Serve's model composition API to combine multiple deployments into a single application.

```{literalinclude} doc_code/quickstart_composed.py
:language: python
```

:::

:::{tab-item} FastAPI integration

Use Serve's [FastAPI](https://fastapi.tiangolo.com/) integration to elegantly handle HTTP parsing and validation.

```{literalinclude} doc_code/fastapi_example.py
:language: python
```

:::

:::{tab-item} Hugging Face Transformers model

To run this example, install the following: ``pip install transformers``

Serve a pre-trained [Hugging Face Transformers](https://huggingface.co/docs/transformers/index) model using Ray Serve.
The model we'll use is a sentiment analysis model: it will take a text string as input and return if the text was "POSITIVE" or "NEGATIVE."

```{literalinclude} doc_code/transformers_example.py
:language: python
```

:::

::::

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

Serve makes it easy to go from a laptop to a cluster. You can test your models (and your entire deployment graph) on your local machine before deploying it to production on a cluster. You don't need to know heavyweight Kubernetes concepts or cloud configurations to use Serve.

:::

:::{dropdown} ML engineer
:animate: fade-in-slide-down

Serve helps you scale out your deployment and runs them reliably and efficiently to save costs. With Serve's first-class model composition API, you can combine models together with business logic and build end-to-end user-facing applications. Additionally, Serve runs natively on Kubernetes with minimal operation overhead.
:::

:::{dropdown} ML platform engineer
:animate: fade-in-slide-down

Serve specializes in scalable and reliable ML model serving. As such, it can be an important plug-and-play component of your ML platform stack.
Serve supports arbitrary Python code and therefore integrates well with the MLOps ecosystem. You can use it with model optimizers (ONNX, TVM), model monitoring systems (Seldon Alibi, Arize), model registries (MLFlow, Weights and Biases), machine learning frameworks (XGBoost, Scikit-learn), data app UIs (Gradio, Streamlit), and Web API frameworks (FastAPI, gRPC).

:::


## How does Serve compare to ...

:::{dropdown} TFServing, TorchServe, ONNXRuntime
:animate: fade-in-slide-down

Ray Serve is *framework-agnostic*, so you can use it alongside any other Python framework or library.
We believe data scientists should not be bound to a particular machine learning framework.
They should be empowered to use the best tool available for the job.

Compared to these framework-specific solutions, Ray Serve doesn't perform any model-specific optimizations to make your ML model run faster. However, you can still optimize the models yourself
and run them in Ray Serve. For example, you can run a model compiled by
[PyTorch JIT](https://pytorch.org/docs/stable/jit.html) or [ONNXRuntime](https://onnxruntime.ai/).
:::

:::{dropdown} AWS SageMaker, Azure ML, Google Vertex AI
:animate: fade-in-slide-down

As an open-source project, Ray Serve brings the scalability and reliability of these hosted offerings to your own infrastructure.
You can use the Ray [cluster launcher](cluster-index) to deploy Ray Serve to all major public clouds, K8s, as well as on bare-metal, on-premise machines.

Ray Serve is not a full-fledged ML Platform.
Compared to these other offerings, Ray Serve lacks the functionality for
managing the lifecycle of your models, visualizing their performance, etc. Ray
Serve primarily focuses on model serving and providing the primitives for you to
build your own ML platform on top.

If you are looking for end-to-end ML pipeline framework that can handle everything from data processing to serving, check out [Ray AI Runtime](air).
:::

:::{dropdown} Seldon, KServe, Cortex
:animate: fade-in-slide-down

You can develop Ray Serve on your laptop, deploy it on a dev box, and scale it out
to multiple machines or a Kubernetes cluster, all with minimal or no changes to code. It's a lot
easier to get started with when you don't need to provision and manage a K8s cluster.
When it's time to deploy, you can use our [Kubernetes Operator](kuberay-quickstart)
to transparently deploy your Ray Serve application to K8s.
:::

:::{dropdown} BentoML, Comet.ml, MLflow
:animate: fade-in-slide-down

Many of these tools are focused on serving and scaling models independently.
In contrast, Ray Serve is framework-agnostic and focuses on model composition.
As such, Ray Serve works with any model packaging and registry format.
Ray Serve also provides key features for building production-ready machine learning applications, including best-in-class autoscaling and naturally integrating with business logic.
:::

We truly believe Serve is unique as it gives you end-to-end control
over your ML application while delivering scalability and high performance. To achieve
Serve's feature offerings with other tools, you would need to glue together multiple
frameworks like Tensorflow Serving and SageMaker, or even roll your own
micro-batching component to improve throughput.

## Learn More

Check out {ref}`getting-started` and {ref}`serve-key-concepts`,
or head over to the {doc}`tutorials/index` to get started building your Ray Serve applications.


```{eval-rst}
.. grid:: 1 2 2 2
    :gutter: 1
    :class-container: container pb-3

    .. grid-item-card::
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img
        
        **Getting Started**
        ^^^
        
        Start with our quick start tutorials for :ref:`deploying a single model locally <getting-started>` and how to :ref:`convert an existing model into a Ray Serve deployment <converting-to-ray-serve-application>` .
        
        +++
        .. button-ref:: getting-started
            :color: primary
            :outline:
            :expand:
        
            Get Started with Ray Serve    
    
    .. grid-item-card::
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img
        
        **Key Concepts**
        ^^^
        
        Understand the key concepts behind Ray Serve.
        Learn about :ref:`Deployments <serve-key-concepts-deployment>`, :ref:`how to query them <serve-key-concepts-query-deployment>`, and the :ref:`Deployment Graph <serve-key-concepts-deployment-graph>` API for composing models into a graph structure.
        
        +++
        .. button-ref:: serve-key-concepts
            :color: primary
            :outline:
            :expand:
        
            Learn Key Concepts
    
    .. grid-item-card::
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img
        
        **User Guides**
        ^^^
        Learn best practices for common patterns like :ref:`scaling and resource allocation <serve-scaling-and-resource-allocation>` and :ref:`model composition <serve-model-composition>`.
        Learn how to :ref:`develop Serve applications locally <serve-dev-workflow>` and :ref:`go to production <serve-in-production>`.
        
        +++
        .. button-ref:: serve-user-guides
            :color: primary
            :outline:
            :expand:
        
            Start Using Ray Serve
        
    .. grid-item-card::
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img
        
        **Examples**
        ^^^
        
        Follow the tutorials to learn how to integrate Ray Serve with :ref:`TensorFlow <serve-ml-models-tutorial>`, :ref:`Scikit-Learn <serve-ml-models-tutorial>`, and :ref:`RLlib <serve-rllib-tutorial>`.
        
        +++
        .. button-ref:: serve-examples
            :color: primary
            :outline:
            :expand:
        
            Serve Examples
        
    .. grid-item-card::
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img
        
        **API Reference**
        ^^^
        
        Get more in-depth information about the Ray Serve API.
        
        +++
        .. button-ref:: serve-api
            :color: primary
            :outline:
            :expand:
        
            Read the API Reference
        
    .. grid-item-card::
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img
            
        **Serve Architecture**
        ^^^
        
        Understand how each component in Ray Serve works.
        
        +++
        .. button-ref:: serve-architecture
            :color: primary
            :outline:
            :expand:
        
            Understand Serve Architecture
```

For more, see the following blog posts about Ray Serve:

- [Serving ML Models in Production: Common Patterns](https://www.anyscale.com/blog/serving-ml-models-in-production-common-patterns) by Simon Mo, Edward Oakes, and Michael Galarnyk
- [The Simplest Way to Serve your NLP Model in Production with Pure Python](https://medium.com/distributed-computing-with-ray/the-simplest-way-to-serve-your-nlp-model-in-production-with-pure-python-d42b6a97ad55) by Edward Oakes and Bill Chambers
- [Machine Learning Serving is Broken](https://medium.com/distributed-computing-with-ray/machine-learning-serving-is-broken-f59aff2d607f) by Simon Mo
- [How to Scale Up Your FastAPI Application Using Ray Serve](https://medium.com/distributed-computing-with-ray/how-to-scale-up-your-fastapi-application-using-ray-serve-c9a7b69e786) by Archit Kulkarni

```{eval-rst}
.. include:: /_includes/serve/announcement_bottom.rst
```
