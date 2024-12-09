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
Production Guide <production-guide/index>
monitoring
resource-allocation
autoscaling-guide
advanced-guides/index
architecture
examples
api/index
```

:::{tip}
[Get in touch with us](https://docs.google.com/forms/d/1l8HT35jXMPtxVUtQPeGoe09VGp5jcvSv0TqPgyz6lGU) if you're using or considering using Ray Serve.
:::

```{image} logo.svg
:align: center
:height: 250px
:width: 400px
```

(rayserve-overview)=

Ray Serve is a scalable, framework-agnostic model serving library for building online inference APIs. Serve integrates with any ML framework including PyTorch, TensorFlow, Keras, Scikit-Learn, and more. It's particularly well suited for model composition and many model serving, and includes performance optimizations for serving LLMs such as response streaming, dynamic request batching, multi-mode/multi-GPU serving, and more.

Ray Serve is built on top of Ray, so it easily scales to many machines and offers flexible scheduling support such as fractional GPUs—so you can share resources and serve many machine learning models at low cost.

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
