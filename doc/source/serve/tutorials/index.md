(serve-examples)=
# Ray Serve Examples

Below are tutorials for using exploring Ray Serve capabilities and how to integrate different modeling frameworks.

Beginner
--------

```{eval-rst}
.. list-table::
  :widths: 1 5
  :header-rows: 1

  * - Framework
    - Example
  * - PyTorch, Tensorflow, Scikit-Learn, Others
    - :doc: `Serving ML Models <serve-ml-models>`
  * - PyTorch, Transformers
    - :doc: `Serving a Stable Diffusion Model <stable-diffusion>`
  * - Transformers
    - :doc: `Serving a Distilbert Model <text-classification>`
  * - PyTorch
    - :ref: `Serving an Object Detection Model <object-detection>`

```

Intermediate
------------

```{list-table}
  :widths: 1 5
  :header-rows: 1
  * - Framework
    - Example
  * - PyTorch, Transformers
    - :doc:`Serving an inference model on AWS NeuronCores using FastAPI <aws-neuron-core-inference>`
  * - Transformers
    - :doc:`Scaling your Gradio app with Ray Serve <gradio-integration>`
  * - Transformers
    - :doc:`Batching with Ray Serve <batch>`
  * - Transformers
    - :doc:`Streaming with Ray Serve <streaming>`
```

Advanced
--------

```{list-table}
  :widths: 1 5
  :header-rows: 1
  * - Framework
    - Example
  * - ???
    - :doc:`Java and Ray Serve <java>`
```

```{toctree}
:caption: Serve Examples
:maxdepth: '-1'
:name: serve-tutorials

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
