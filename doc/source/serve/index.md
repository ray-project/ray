(rayserve)=

# Ray Serve: Scalable and Programmable Serving

```{toctree}
:hidden:

getting_started
key-concepts
develop-and-deploy
model_composition
multi-app
model-multiplexing
configure-serve-deployment
http-guide
Serving LLMs <llm/serving-llms>
Production Guide <production-guide/index>
monitoring
resource-allocation
autoscaling-guide
advanced-guides/index
architecture
examples
api/index
```

```{image} logo.svg
:align: center
:height: 250px
:width: 400px
```

(rayserve-overview)=

Ray Serve is a scalable model serving library for building online inference APIs.
Serve is framework-agnostic, so you can use a single toolkit to serve everything from deep learning models built with frameworks like PyTorch, TensorFlow, and Keras, to Scikit-Learn models, to arbitrary Python business logic. It has several features and performance optimizations for serving Large Language Models such as response streaming, dynamic request batching, multi-node/multi-GPU serving, etc.

Ray Serve is particularly well suited for [model composition](serve-model-composition) and many model serving, enabling you to build a complex inference service consisting of multiple ML models and business logic all in Python code.

Ray Serve is built on top of Ray, so it easily scales to many machines and offers flexible scheduling support such as fractional GPUs so you can share resources and serve many machine learning models at low cost.

## Quickstart

Install Ray Serve and its dependencies:

```bash
pip install "ray[serve]"
```
Define a simple "hello world" application, run it locally, and query it over HTTP.

```{literalinclude} doc_code/quickstart.py
:language: python
```

## More examples

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
However, machine learning isn't useful in isolation.
It's often important to combine machine learning with business logic and traditional web serving logic such as database queries.

Ray Serve is unique in that it allows you to build and deploy an end-to-end distributed serving application in a single framework.
You can combine multiple ML models, business logic, and expressive HTTP handling using Serve's FastAPI integration (see {ref}`serve-fastapi-http`) to build your entire application as one Python program.

:::

:::{dropdown} Combine multiple models using a programmable API
:animate: fade-in-slide-down

Often solving a problem requires more than just a single machine learning model.
For instance, image processing applications typically require a multi-stage pipeline consisting of steps like preprocessing, segmentation, and filtering to achieve their end goal.
In many cases each model may use a different architecture or framework and require different resources (like CPUs vs GPUs).

Many other solutions support defining a static graph in YAML or some other configuration language.
This can be limiting and hard to work with.
Ray Serve, on the other hand, supports multi-model composition using a programmable API where calls to different models look just like function calls.
The models can use different resources and run across different machines in the cluster, but you can write it like a regular program. See {ref}`serve-model-composition` for more details.

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

:::{dropdown} LLM developer
:animate: fade-in-slide-down

Serve enables you to rapidly prototype, develop, and deploy scalable LLM applications to production. Many large language model (LLM) applications combine prompt preprocessing, vector database lookups, LLM API calls, and response validation. Because Serve supports any arbitrary Python code, you can write all these steps as a single Python module, enabling rapid development and easy testing. You can then quickly deploy your Ray Serve LLM application to production, and each application step can independently autoscale to efficiently accommodate user traffic without wasting resources. In order to improve performance of your LLM applications, Ray Serve has features for batching and can integrate with any model optimization technique. Ray Serve also supports streaming responses, a key feature for chatbot-like applications.

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

Check out {ref}`serve-getting-started` and {ref}`serve-key-concepts`,
or head over to the {doc}`examples` to get started building your Ray Serve applications.


```{eval-rst}
.. grid:: 1 2 2 2
    :gutter: 1
    :class-container: container pb-3

    .. grid-item-card::
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        **Getting Started**
        ^^^

        Start with our quick start tutorials for :ref:`deploying a single model locally <serve-getting-started>` and how to :ref:`convert an existing model into a Ray Serve deployment <converting-to-ray-serve-application>` .

        +++
        .. button-ref:: serve-getting-started
            :color: primary
            :outline:
            :expand:

            Get Started with Ray Serve

    .. grid-item-card::
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        **Key Concepts**
        ^^^

        Understand the key concepts behind Ray Serve.
        Learn about :ref:`Deployments <serve-key-concepts-deployment>`, :ref:`how to query them <serve-key-concepts-ingress-deployment>`, and using :ref:`DeploymentHandles <serve-key-concepts-deployment-handle>` to compose multiple models and business logic together.

        +++
        .. button-ref:: serve-key-concepts
            :color: primary
            :outline:
            :expand:

            Learn Key Concepts

    .. grid-item-card::
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        **Examples**
        ^^^

        Follow the tutorials to learn how to integrate Ray Serve with :ref:`TensorFlow <serve-ml-models-tutorial>`, and :ref:`Scikit-Learn <serve-ml-models-tutorial>`.

        +++
        .. button-ref:: examples
            :color: primary
            :outline:
            :expand:
            :ref-type: doc

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

```

For more, see the following blog posts about Ray Serve:

- [Serving ML Models in Production: Common Patterns](https://www.anyscale.com/blog/serving-ml-models-in-production-common-patterns) by Simon Mo, Edward Oakes, and Michael Galarnyk
- [The Simplest Way to Serve your NLP Model in Production with Pure Python](https://medium.com/distributed-computing-with-ray/the-simplest-way-to-serve-your-nlp-model-in-production-with-pure-python-d42b6a97ad55) by Edward Oakes and Bill Chambers
- [Machine Learning Serving is Broken](https://medium.com/distributed-computing-with-ray/machine-learning-serving-is-broken-f59aff2d607f) by Simon Mo
- [How to Scale Up Your FastAPI Application Using Ray Serve](https://medium.com/distributed-computing-with-ray/how-to-scale-up-your-fastapi-application-using-ray-serve-c9a7b69e786) by Archit Kulkarni
