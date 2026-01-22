.. _working-with-llms:

Working with LLMs
=================

The :ref:`ray.data.llm <llm-ref>` module enables scalable batch inference on Ray Data datasets. It supports two modes: running LLM inference engines directly (vLLM, SGLang) or querying hosted endpoints through :class:`~ray.data.llm.ServeDeploymentProcessorConfig`.

**Getting started:**

* :ref:`Quickstart <vllm_quickstart>` - Run your first batch inference job
* :ref:`Architecture <processor_architecture>` - Understand the processor pipeline
* :ref:`Scaling <horizontal_scaling>` - Scale your LLM stage to multiple replicas

**Common use cases:**

* :ref:`Text generation <text_generation>` - Chat completions with LLMs
* :ref:`Embeddings <embedding_models>` - Generate text embeddings
* :ref:`Classification <classification_models>` - Content classifiers and sentiment analyzers
* :ref:`Vision-language models <vision_language_model>` - Process images with VLMs
* :ref:`OpenAI-compatible endpoints <openai_compatible_api_endpoint>` - Query deployed models
* :ref:`Serve deployments <serve_deployments>` - Share vLLM engines across processors

**Operations:**

* :ref:`Troubleshooting <troubleshooting>` - GPU memory, model loading issues
* :ref:`Advanced configuration <advanced_configuration>` - Parallelism, per-stage tuning, LoRA

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

.. _processor_architecture:

Processor architecture
----------------------

Ray Data LLM uses a **multi-stage processor pipeline** to transform your data through LLM inference. Understanding this architecture helps you optimize performance and debug issues.

.. code-block:: text

    Input Dataset
         |
         v
    - Preprocess (Custom Function)
    - PrepareImage (Optional, for VLM / Omni models)
    - ChatTemplate (Applies chat template to messages)
    - Tokenize (Converts text to token IDs)
    - LLM Engine (vLLM/SGLang inference on GPU)
    - Detokenize (Converts token IDs back to text)
    - Postprocess (Custom Function)
         |
         v
    Output Dataset

**Stage descriptions:**

- **Preprocess**: Your custom function that transforms input rows into the format expected by downstream stages (typically OpenAI chat format with ``messages``).
- **PrepareImage**: Extracts and prepares images from multimodal inputs. Enable with ``prepare_image_stage=True``.
- **ChatTemplate**: Applies the model's chat template to convert messages into a prompt string.
- **Tokenize**: Converts the prompt string into token IDs for the model.
- **LLM Engine**: The accelerated (GPU/TPU) inference stage running vLLM or SGLang.
- **Detokenize**: Converts output token IDs back to readable text.
- **Postprocess**: Your custom function that extracts and formats the final output.

Each stage runs as a separate Ray actor pool, enabling independent scaling and resource allocation. CPU stages (ChatTemplate, Tokenize, Detokenize) use autoscaling actor pools (except for ServeDeployment and HTTP request handling stages), while the GPU stage uses a fixed pool.

.. _horizontal_scaling:

Scaling to multiple GPUs
------------------------

Horizontally scale the LLM stage to multiple GPU replicas using the ``concurrency`` parameter:

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __concurrent_config_example_start__
    :end-before: __concurrent_config_example_end__

Each replica runs an independent inference engine. Set ``concurrency`` to match the number of available GPUs or GPU nodes.

.. _text_generation:

Text generation
---------------

Use :class:`vLLMEngineProcessorConfig <ray.data.llm.vLLMEngineProcessorConfig>` or :class:`SGLangEngineProcessorConfig <ray.data.llm.SGLangEngineProcessorConfig>` for chat completions and text generation tasks.

**Key configuration options:**

- ``model_source``: HuggingFace model ID or path to model weights
- ``concurrency``: Number of vLLM engine replicas (typically 1 per GPU node)
- ``batch_size``: Rows per batch (reduce if hitting memory limits)

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __basic_config_example_start__
    :end-before: __basic_config_example_end__

For gated models requiring authentication, pass your HuggingFace token through ``runtime_env``:

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __hf_token_config_example_start__
    :end-before: __hf_token_config_example_end__

.. _vision_language_model:

Vision-language models
----------------------

To process images with VLMs, enable the image preparation stage:

.. literalinclude:: doc_code/working-with-llms/vlm_image_example.py
    :language: python
    :start-after: __vlm_config_example_start__
    :end-before: __vlm_config_example_end__

In your preprocessor, format images using OpenAI's vision message format. The ``image`` field accepts PIL Images or URLs:

.. code-block:: python

    def preprocess(row):
        return {
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": row["question"]},
                    {"type": "image", "image": row["pil_image"]},
                ]},
            ],
            "sampling_params": {"max_tokens": 100},
        }

.. _embedding_models:

Embeddings
----------

For embedding models, set ``task_type="embed"`` and disable chat templating:

.. literalinclude:: doc_code/working-with-llms/embedding_example.py
    :language: python
    :start-after: __embedding_example_start__
    :end-before: __embedding_example_end__

Key differences from text generation:

- Use ``prompt`` input instead of ``messages``
- Access results through ``row["embeddings"]``

.. _classification_models:

Classification
--------------

Ray Data LLM supports batch inference with sequence classification models, such as content classifiers and sentiment analyzers:

.. literalinclude:: doc_code/working-with-llms/classification_example.py
    :language: python
    :start-after: __classification_example_start__
    :end-before: __classification_example_end__

Key differences for classification models:

- Set ``task_type="classify"`` (or ``task_type="score"`` for scoring models)
- Set ``apply_chat_template=False`` and ``detokenize=False``
- Use direct ``prompt`` input instead of ``messages``
- Access classification logits through ``row["embeddings"]``

.. _openai_compatible_api_endpoint:

OpenAI-compatible endpoints
---------------------------

Query deployed models with an OpenAI-compatible API:

.. literalinclude:: doc_code/working-with-llms/openai_api_example.py
    :language: python
    :start-after: __openai_example_start__
    :end-before: __openai_example_end__

.. _troubleshooting:

Troubleshooting
---------------

GPU memory and CUDA OOM
~~~~~~~~~~~~~~~~~~~~~~~

If you encounter CUDA out of memory errors, try these strategies:

- **Reduce batch size**: Start with 8-16 and increase gradually
- **Lower ``max_num_batched_tokens``**: Reduce from 4096 to 2048 or 1024
- **Decrease ``max_model_len``**: Use shorter context lengths
- **Set ``gpu_memory_utilization``**: Use 0.75-0.85 instead of default 0.90

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __gpu_memory_config_example_start__
    :end-before: __gpu_memory_config_example_end__

Model loading at scale
~~~~~~~~~~~~~~~~~~~~~~

.. _model_cache:

For large clusters, HuggingFace downloads may be rate-limited. Cache models to S3 or GCS:

.. code-block:: bash

    python -m ray.llm.utils.upload_model \
        --model-source facebook/opt-350m \
        --bucket-uri gs://my-bucket/path/to/model

Then reference the remote path in your config:

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __s3_config_example_start__
    :end-before: __s3_config_example_end__

.. _advanced_configuration:

Advanced configuration
----------------------

Model parallelism
~~~~~~~~~~~~~~~~~

For large models that don't fit on a single GPU, use tensor and pipeline parallelism:

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __parallel_config_example_start__
    :end-before: __parallel_config_example_end__

Cross-node parallelism
~~~~~~~~~~~~~~~~~~~~~~

Ray Data LLM supports cross-node parallelism, including tensor parallelism and pipeline parallelism. Configure the parallelism level through ``engine_kwargs``. The ``distributed_executor_backend`` defaults to ``"ray"`` for cross-node support.

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __cross_node_parallelism_config_example_start__
    :end-before: __cross_node_parallelism_config_example_end__

You can customize the placement group strategy to control how Ray places vLLM engine workers across nodes. While you can specify the degree of tensor and pipeline parallelism, the specific assignment of model ranks to GPUs is managed by the vLLM engine.

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __custom_placement_group_strategy_config_example_start__
    :end-before: __custom_placement_group_strategy_config_example_end__

Per-stage configuration
~~~~~~~~~~~~~~~~~~~~~~~

Configure individual pipeline stages for fine-grained resource control:

.. code-block:: python

    config = vLLMEngineProcessorConfig(
        model_source="meta-llama/Llama-3.1-8B-Instruct",
        chat_template_stage={
            "enabled": True,
            "batch_size": 256,
            "concurrency": 4,
        },
        tokenize_stage={
            "enabled": True,
            "batch_size": 512,
            "num_cpus": 0.5,
        },
        detokenize_stage={
            "enabled": True,
            "concurrency": (2, 8),  # Autoscaling pool
        },
    )

See :ref:`stage config classes <stage-configs-ref>` for all available fields.

LoRA adapters
~~~~~~~~~~~~~

For multi-LoRA batch inference:

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __lora_config_example_start__
    :end-before: __lora_config_example_end__

See :doc:`the vLLM with LoRA example</llm/examples/batch/vllm-with-lora>` for details.

Accelerated model loading with RunAI streamer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use `RunAI Model Streamer <https://github.com/run-ai/runai-model-streamer>`_ for faster model loading from cloud storage:

.. note::
    Install vLLM with runai dependencies: ``pip install -U "vllm[runai]>=0.10.1"``

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __runai_config_example_start__
    :end-before: __runai_config_example_end__

.. _serve_deployments:

Serve deployments
~~~~~~~~~~~~~~~~~

For multi-turn conversations or complex agentic workflows, share a vLLM engine across multiple processors using :ref:`Ray Serve <serving-llms>`:

.. literalinclude:: doc_code/working-with-llms/basic_llm_example.py
    :language: python
    :start-after: __shared_vllm_engine_config_example_start__
    :end-before: __shared_vllm_engine_config_example_end__

----

**Usage data collection**: Ray collects anonymous usage data to improve Ray Data LLM. To opt out, see :ref:`Ray usage stats <ref-usage-stats>`.
