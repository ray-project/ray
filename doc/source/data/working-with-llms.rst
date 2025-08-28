.. _working-with-llms:

Working with LLMs
=================

The :ref:`ray.data.llm <llm-ref>` module integrates with key large language model (LLM) inference engines and deployed models to enable LLM batch inference.

This guide shows you how to use :ref:`ray.data.llm <llm-ref>` to:

* :ref:`Perform batch inference with LLMs <batch_inference_llm>`
* :ref:`Configure vLLM for LLM inference <vllm_llm>`
* :ref:`Query deployed models with an OpenAI compatible API endpoint <openai_compatible_api_endpoint>`

.. _batch_inference_llm:

Perform batch inference with LLMs
---------------------------------

At a high level, the :ref:`ray.data.llm <llm-ref>` module provides a :class:`Processor <ray.data.llm.Processor>` object which encapsulates
logic for performing batch inference with LLMs on a Ray Data dataset.

You can use the :func:`build_llm_processor <ray.data.llm.build_llm_processor>` API to construct a processor.
The following example uses the :class:`vLLMEngineProcessorConfig <ray.data.llm.vLLMEngineProcessorConfig>` to construct a processor for the `unsloth/Llama-3.1-8B-Instruct` model.

To run this example, install vLLM, which is a popular and optimized LLM inference engine.

.. code-block:: bash

    # Later versions *should* work but are not tested yet.
    pip install -U vllm==0.7.2

The :class:`vLLMEngineProcessorConfig <ray.data.llm.vLLMEngineProcessorConfig>` is a configuration object for the vLLM engine.
It contains the model name, the number of GPUs to use, and the number of shards to use, along with other vLLM engine configurations.
Upon execution, the Processor object instantiates replicas of the vLLM engine (using :meth:`map_batches <ray.data.Dataset.map_batches>` under the hood).

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __basic_llm_example_start__
    :end-before: __basic_llm_example_end__

Here's a simple configuration example:

.. code-block:: python

    from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor

    config = vLLMEngineProcessorConfig(
        model_source="unsloth/Llama-3.1-8B-Instruct",
        engine_kwargs={
            "enable_chunked_prefill": True,
            "max_num_batched_tokens": 4096,
            "max_model_len": 16384,
        },
        concurrency=1,
        batch_size=64,
    )
    processor = build_llm_processor(config)

Each processor requires specific input columns. You can find more info by using the following API:

The processor requires specific input columns. Here's how to check what columns are needed:

.. code-block:: python

    from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor
    
    config = vLLMEngineProcessorConfig(model_source="unsloth/Llama-3.1-8B-Instruct")
    processor = build_llm_processor(config)
    processor.log_input_column_names()

    # Output: The first stage of the processor is ChatTemplateStage.
    # Required input columns:
    #         messages: A list of messages in OpenAI chat format.

Some models may require a Hugging Face token to be specified. You can specify the token in the `runtime_env` argument.

.. code-block:: python

    config = vLLMEngineProcessorConfig(
        model_source="unsloth/Llama-3.1-8B-Instruct",
        runtime_env={"env_vars": {"HF_TOKEN": "your_huggingface_token"}},
        concurrency=1,
        batch_size=64,
    )

.. _vllm_llm:

Configure vLLM for LLM inference
--------------------------------

Use the :class:`vLLMEngineProcessorConfig <ray.data.llm.vLLMEngineProcessorConfig>` to configure the vLLM engine.

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: # Basic vLLM configuration
    :end-before: # Create sample dataset
    :dedent: 0

For handling larger models, specify model parallelism.

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: # Model parallelism configuration
    :end-before: # RunAI streamer configuration
    :dedent: 0

The underlying :class:`Processor <ray.data.llm.Processor>` object instantiates replicas of the vLLM engine and automatically
configure parallel workers to handle model parallelism (for tensor parallelism and pipeline parallelism,
if specified).

To optimize model loading, you can configure the `load_format` to `runai_streamer` or `tensorizer`.

.. note::
    In this case, install vLLM with runai dependencies: `pip install -U "vllm[runai]==0.7.2"`

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: # RunAI streamer configuration
    :end-before: # S3 hosted model configuration
    :dedent: 0

If your model is hosted on AWS S3, you can specify the S3 path in the `model_source` argument, and specify `load_format="runai_streamer"` in the `engine_kwargs` argument.

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: # S3 hosted model configuration
    :end-before: # Multi-LoRA configuration
    :dedent: 0

To do multi-LoRA batch inference, you need to set LoRA related parameters in `engine_kwargs`. See :doc:`the vLLM with LoRA example</llm/examples/batch/vllm-with-lora>` for details.

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: # Multi-LoRA configuration
    :end-before: # __basic_llm_example_end__
    :dedent: 0

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

Complete VLM example:

.. literalinclude:: doc_code/working-with-llms/vlm_example.py
    :language: python
    :start-after: __vlm_example_start__
    :end-before: __vlm_example_end__

Here's a simple VLM configuration:

.. code-block:: python

    from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor

    vision_config = vLLMEngineProcessorConfig(
        model_source="Qwen/Qwen2.5-VL-3B-Instruct",
        engine_kwargs={"tensor_parallel_size": 1},
        runtime_env={"env_vars": {"HF_TOKEN": "your-hf-token-here"}},
        batch_size=1,
        accelerator_type="L4",
        has_image=True
    )
    processor = build_llm_processor(vision_config)

For a comprehensive VLM configuration example:

.. literalinclude:: doc_code/working-with-llms/vlm_example.py
    :language: python
    :start-after: def create_vlm_config():
    :end-before: def run_vlm_example():
    :dedent: 0


.. _openai_compatible_api_endpoint:

Batch inference with an OpenAI-compatible endpoint
--------------------------------------------------

You can also make calls to deployed models that have an OpenAI compatible API endpoint.

Complete OpenAI API example:

.. literalinclude:: doc_code/working-with-llms/openai_api_example.py
    :language: python
    :start-after: __openai_example_start__
    :end-before: __openai_example_end__

Here's a simple OpenAI API configuration:

.. code-block:: python

    from ray.data.llm import HttpRequestProcessorConfig, build_llm_processor

    config = HttpRequestProcessorConfig(
        url="https://api.openai.com/v1/chat/completions",
        headers={"Authorization": "Bearer your-api-key-here"},
        qps=1
    )
    processor = build_llm_processor(config)

For a comprehensive configuration and usage demo:

.. literalinclude:: doc_code/working-with-llms/openai_api_example.py
    :language: python
    :start-after: def run_openai_demo():
    :end-before: # __openai_example_end__
    :dedent: 4

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

.. TODO(#55491): Rewrite this section once the restriction is lifted.
.. TODO(#55405): Cross-node TP in progress.
.. _cross_node_parallelism:

How to configure LLM stage to parallelize across multiple nodes?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

At the moment, Ray Data LLM doesn't support cross-node parallelism (either
tensor parallelism or pipeline parallelism).

The processing pipeline is designed to run on a single node. The number of
GPUs is calculated as the product of the tensor parallel size and the pipeline
parallel size, and apply
[`STRICT_PACK` strategy](https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html#pgroup-strategy)
to ensure that each replica of the LLM stage is executed on a single node.

Nevertheless, you can still horizontally scale the LLM stage to multiple nodes
as long as each replica (TP * PP) fits into a single node. The number of
replicas is configured by the `concurrency` argument in
:class:`vLLMEngineProcessorConfig <ray.data.llm.vLLMEngineProcessorConfig>`.

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

.. code-block:: python

    from ray.data.llm import vLLMEngineProcessorConfig
    
    config = vLLMEngineProcessorConfig(
        model_source="gs://my-bucket/path/to/facebook-opt-350m",  # or s3://my-bucket/path/to/model_name
        # ... other configuration parameters
    )

For a more comprehensive S3 configuration example with environment variables:

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: def create_s3_config():
    :end-before: def create_lora_config():
    :dedent: 4
