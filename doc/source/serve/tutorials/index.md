(serve-examples)=

# Ray Serve Examples

```{toctree}
:caption: Ray Serve Examples
:maxdepth: '-1'
:name: serve-tutorials
:hidden:

serve-ml-models
stable-diffusion
text-classification
object-detection
aws-neuron-core-inference
gradio-integration
batch
streaming
java
```

Below are tutorials for using exploring Ray Serve capabilities and how to integrate different modeling frameworks.

Beginner
--------

```{list-table}
  :widths: 1 5
  :header-rows: 1
  * - Framework
    - Example
  * - PyTorch, Tensorflow, Scikit-Learn, Others
    - [Serving ML Models](serve-ml-models)
  * - PyTorch, Transformers
    - [Serving a Stable Diffusion Model](stable-diffusion)
  * - Transformers
    - [Serving a Distilbert Model](text-classification)
  * - PyTorch
    - [Serving an Object Detection Model](object-detection)
```

Intermediate
------------

```{list-table}
  :widths: 1 5
  :header-rows: 1
  * - Framework
    - Example
  * - PyTorch, Transformers
    - [Serving an inference model on AWS NeuronCores using FastAPI](aws-neuron-core-inference)
  * - Transformers
    - [Scaling your Gradio app with Ray Serve](gradio-integration)
  * - Transformers
    - [Batching with Ray Serve](batch.md)
  * - Transformers
    - [Streaming with Ray Serve](streaming)
```

Advanced
--------

```{list-table}
  :widths: 1 5
  :header-rows: 1
  * - Framework
    - Example
  * - ???
    - [Java and Ray Serve](java)
```