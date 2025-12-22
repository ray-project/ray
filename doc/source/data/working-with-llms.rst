.. _working-with-llms:

Working with LLMs
=================

The :ref:`ray.data.llm <llm-ref>` module integrates with key large language model (LLM) inference engines and deployed models to enable LLM batch inference.

This guide shows you how to use :ref:`ray.data.llm <llm-ref>` to:

* :ref:`Quickstart: vLLM batch inference <vllm_quickstart>`
* :ref:`Perform batch inference with LLMs <batch_inference_llm>`
* :ref:`Configure vLLM for LLM inference <vllm_llm>`
* :ref:`Batch inference with embedding models <embedding_models>`
* :ref:`Batch inference with classification models <classification_models>`
* :ref:`Query deployed models with an OpenAI compatible API endpoint <openai_compatible_api_endpoint>`

.. _vllm_quickstart:

Quickstart: vLLM batch inference
---------------------------------

Get started with vLLM batch inference in just a few steps. This example shows the minimal setup needed to run batch inference on a dataset.

.. note::
    This quickstart requires a GPU as vLLM is GPU-accelerated.

First, install Ray Data with LLM support:

.. code-block:: bash

    pip install -U "ray[data, llm]>=2.49.1"

Here's a complete minimal example that runs batch inference:

.. literalinclude:: doc_code/working-with-llms/minimal_quickstart.py
    :language: python
    :start-after: __minimal_vllm_quickstart_start__
    :end-before: __minimal_vllm_quickstart_end__

This example:

1. Creates a simple dataset with prompts
2. Configures a vLLM processor with minimal settings
3. Builds a processor that handles preprocessing (converting prompts to OpenAI chat format) and postprocessing (extracting generated text)
4. Runs inference on the dataset
5. Iterates through results

The processor expects input rows with a ``prompt`` field and outputs rows with both ``prompt`` and ``response`` fields. You can consume results using ``iter_rows()``, ``take()``, ``show()``, or save to files with ``write_parquet()``.

For more configuration options and advanced features, see the sections below.

.. _batch_inference_llm:

Perform batch inference with LLMs
---------------------------------

At a high level, the :ref:`ray.data.llm <llm-ref>` module provides a :class:`Processor <ray.data.llm.Processor>` object which encapsulates
logic for performing batch inference with LLMs on a Ray Data dataset.

You can use the :func:`build_processor <ray.data.llm.build_processor>` API to construct a processor.
The following example uses the :class:`vLLMEngineProcessorConfig <ray.data.llm.vLLMEngineProcessorConfig>` to construct a processor for the `unsloth/Llama-3.1-8B-Instruct` model.
Upon execution, the Processor object instantiates replicas of the vLLM engine (using :meth:`map_batches <ray.data.Dataset.map_batches>` under the hood).

.. .. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
..     :language: python
..     :start-after: __basic_llm_example_start__
..     :end-before: __basic_llm_example_end__

Here's a simple configuration example:

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __basic_config_example_start__
    :end-before: __basic_config_example_end__

The configuration includes detailed comments explaining:

- **`concurrency`**: Number of vLLM engine replicas (typically 1 per node)
- **`batch_size`**: Number of samples processed per batch (reduce if GPU memory is limited)
- **`max_num_batched_tokens`**: Maximum tokens processed simultaneously (reduce if CUDA OOM occurs)
- **`accelerator_type`**: Specify GPU type for optimal resource allocation

The vLLM processor expects input in OpenAI chat format with a 'messages' column and outputs a 'generated_text' column containing model responses.

Some models may require a Hugging Face token to be specified. You can specify the token in the `runtime_env` argument.

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __hf_token_config_example_start__
    :end-before: __hf_token_config_example_end__

.. _vllm_llm:

Configure vLLM for LLM inference
--------------------------------

Use the :class:`vLLMEngineProcessorConfig <ray.data.llm.vLLMEngineProcessorConfig>` to configure the vLLM engine.

For handling larger models, specify model parallelism:

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __parallel_config_example_start__
    :end-before: __parallel_config_example_end__

The underlying :class:`Processor <ray.data.llm.Processor>` object instantiates replicas of the vLLM engine and automatically
configure parallel workers to handle model parallelism (for tensor parallelism and pipeline parallelism,
if specified).

To optimize model loading, you can configure the `load_format` to `runai_streamer` or `tensorizer`.

.. note::
    In this case, install vLLM with runai dependencies: `pip install -U "vllm[runai]>=0.10.1"`

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __runai_config_example_start__
    :end-before: __runai_config_example_end__

If your model is hosted on AWS S3, you can specify the S3 path in the `model_source` argument, and specify `load_format="runai_streamer"` in the `engine_kwargs` argument.

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __s3_config_example_start__
    :end-before: __s3_config_example_end__

To do multi-LoRA batch inference, you need to set LoRA related parameters in `engine_kwargs`. See :doc:`the vLLM with LoRA example</llm/examples/batch/vllm-with-lora>` for details.

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __lora_config_example_start__
    :end-before: __lora_config_example_end__

.. _vision_language_model:

Batch inference with vision-language-model (VLM)
--------------------------------------------------------

Ray Data LLM also supports running batch inference with vision language
models. This example shows how to prepare a dataset with images and run
batch inference with a vision language model.

This example applies 2 adjustments on top of the previous example:

- set `has_image=True` in `vLLMEngineProcessorConfig`
- prepare image input inside preprocessor

First, install the required dependencies:

.. code-block:: bash

    # Install required dependencies for vision-language models
    pip install datasets>=4.0.0

First, load a vision dataset:

.. literalinclude:: doc_code/working-with-llms/vlm_example.py
    :language: python
    :start-after: def load_vision_dataset():
    :end-before: def create_vlm_config():
    :dedent: 0

Next, configure the VLM processor with the essential settings:

.. literalinclude:: doc_code/working-with-llms/vlm_example.py
    :language: python
    :start-after: __vlm_config_example_start__
    :end-before: __vlm_config_example_end__

Define preprocessing and postprocessing functions to convert dataset rows into
the format expected by the VLM and extract model responses:

.. literalinclude:: doc_code/working-with-llms/vlm_example.py
    :language: python
    :start-after: __vlm_preprocess_example_start__
    :end-before: __vlm_preprocess_example_end__

For a more comprehensive VLM configuration with advanced options:

.. literalinclude:: doc_code/working-with-llms/vlm_example.py
    :language: python
    :start-after: def create_vlm_config():
    :end-before: def run_vlm_example():
    :dedent: 0

Finally, run the VLM inference:

.. literalinclude:: doc_code/working-with-llms/vlm_example.py
    :language: python
    :start-after: def run_vlm_example():
    :end-before: # __vlm_run_example_end__
    :dedent: 0

.. _embedding_models:

Batch inference with embedding models
---------------------------------------

Ray Data LLM supports batch inference with embedding models using vLLM:

.. literalinclude:: doc_code/working-with-llms/embedding_example.py
    :language: python
    :start-after: __embedding_example_start__
    :end-before: __embedding_example_end__

.. testoutput::
    :options: +MOCK

    {'text': 'Hello world', 'embedding': [0.1, -0.2, 0.3, ...]}

Key differences for embedding models:

- Set ``task_type="embed"``
- Set ``apply_chat_template=False`` and ``detokenize=False``
- Use direct ``prompt`` input instead of ``messages``
- Access embeddings through``row["embeddings"]``

For a complete embedding configuration example, see:

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __embedding_config_example_start__
    :end-before: __embedding_config_example_end__

.. _classification_models:

Batch inference with classification models
------------------------------------------

Ray Data LLM supports batch inference with sequence classification models, such as content classifiers and sentiment analyzers:

.. literalinclude:: doc_code/working-with-llms/classification_example.py
    :language: python
    :start-after: __classification_example_start__
    :end-before: __classification_example_end__

.. testoutput::
    :options: +MOCK

    {'text': 'lol that was so funny haha', 'edu_score': -0.05}
    {'text': 'Photosynthesis converts light energy...', 'edu_score': 1.73}
    {'text': "Newton's laws describe...", 'edu_score': 2.52}

Key differences for classification models:

- Set ``task_type="classify"`` (or ``task_type="score"`` for scoring models)
- Set ``apply_chat_template=False`` and ``detokenize=False``
- Use direct ``prompt`` input instead of ``messages``
- Access classification logits through ``row["embeddings"]``

For a complete classification configuration example, see:

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __classification_config_example_start__
    :end-before: __classification_config_example_end__

.. _openai_compatible_api_endpoint:

Batch inference with an OpenAI-compatible endpoint
--------------------------------------------------

You can also make calls to deployed models that have an OpenAI compatible API endpoint.

.. literalinclude:: doc_code/working-with-llms/openai_api_example.py
    :language: python
    :start-after: __openai_example_start__
    :end-before: __openai_example_end__

Batch inference with serve deployments
---------------------------------------

You can configure any :ref:`serve deployment <converting-to-ray-serve-application>` for batch inference. This is particularly useful for multi-turn conversations,
where you can use a shared vLLM engine across conversations. To achieve this, create an :ref:`LLM serve deployment <serving-llms>` and use
the :class:`ServeDeploymentProcessorConfig <ray.data.llm.ServeDeploymentProcessorConfig>` class to configure the processor.

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __shared_vllm_engine_config_example_start__
    :end-before: __shared_vllm_engine_config_example_end__

Cross-node parallelism
---------------------------------------

Ray Data LLM supports cross-node parallelism, including tensor parallelism and pipeline parallelism.
You can configure the parallelism level through the `engine_kwargs` argument in
:class:`vLLMEngineProcessorConfig <ray.data.llm.vLLMEngineProcessorConfig>`. Use `ray` as the
distributed executor backend to enable cross-node parallelism.

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __cross_node_parallelism_config_example_start__
    :end-before: __cross_node_parallelism_config_example_end__


In addition, you can customize the placement group strategy to control how Ray places vLLM engine workers across nodes.
While you can specify the degree of tensor and pipeline parallelism, the specific assignment of model ranks to GPUs is managed by the vLLM engine and you can't directly configure it through the Ray Data LLM API.

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __custom_placement_group_strategy_config_example_start__
    :end-before: __custom_placement_group_strategy_config_example_end__

Besides cross-node parallelism, you can also horizontally scale the LLM stage to multiple nodes.
Configure the number of replicas with the `concurrency` argument in
:class:`vLLMEngineProcessorConfig <ray.data.llm.vLLMEngineProcessorConfig>`.

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __concurrent_config_example_start__
    :end-before: __concurrent_config_example_end__



Usage Data Collection
--------------------------

Data for the following features and attributes is collected to improve Ray Data LLM:

- config name used for building the llm processor
- number of concurrent users for data parallelism
- batch size of requests
- model architecture used for building vLLMEngineProcessor
- task type used for building vLLMEngineProcessor
- engine arguments used for building vLLMEngineProcessor
- tensor parallel size and pipeline parallel size used
- GPU type used and number of GPUs used

If you would like to opt-out from usage data collection, you can follow :ref:`Ray usage stats <ref-usage-stats>`
to turn it off.

.. _faqs:

Frequently Asked Questions (FAQs)
--------------------------------------------------

.. _gpu_memory_management:

GPU Memory Management and CUDA OOM Prevention
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you encounter CUDA out of memory errors, Ray Data LLM provides several configuration options to optimize GPU memory usage:

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __gpu_memory_config_example_start__
    :end-before: __gpu_memory_config_example_end__

**Key strategies for handling GPU memory issues:**

- **Reduce batch size**: Start with smaller batches (8-16) and increase gradually
- **Lower `max_num_batched_tokens`**: Reduce from 4096 to 2048 or 1024
- **Decrease `max_model_len`**: Use shorter context lengths when possible
- **Set `gpu_memory_utilization`**: Use 0.75-0.85 instead of default 0.90
- **Use smaller models**: Consider using smaller model variants for resource-constrained environments

If you run into CUDA out of memory, your batch size is likely too large. Set an explicit small batch size or use a smaller model, or a larger GPU.

.. _model_cache:

How to cache model weight to remote object storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

While deploying Ray Data LLM to large scale clusters, model loading may be rate
limited by HuggingFace. In this case, you can cache the model to remote object
storage (AWS S3 or Google Cloud Storage) for more stable model loading.

Ray Data LLM provides the following utility to help uploading models to remote object storage.

.. code-block:: bash

    # Download model from HuggingFace, and upload to GCS
    python -m ray.llm.utils.upload_model \
        --model-source facebook/opt-350m \
        --bucket-uri gs://my-bucket/path/to/facebook-opt-350m
    # Or upload a local custom model to S3
    python -m ray.llm.utils.upload_model \
        --model-source local/path/to/model \
        --bucket-uri s3://my-bucket/path/to/model_name

And later you can use remote object store URI as `model_source` in the config.

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __s3_config_example_start__
    :end-before: __s3_config_example_end__
