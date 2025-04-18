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

.. testcode::

    # Later versions *should* work but are not tested yet.
    pip install -U vllm==0.7.2

The :class:`vLLMEngineProcessorConfig <ray.data.llm.vLLMEngineProcessorConfig>` is a configuration object for the vLLM engine.
It contains the model name, the number of GPUs to use, and the number of shards to use, along with other vLLM engine configurations.
Upon execution, the Processor object instantiates replicas of the vLLM engine (using :meth:`map_batches <ray.data.Dataset.map_batches>` under the hood).

.. testcode::

    import ray
    from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor
    import numpy as np

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
    processor = build_llm_processor(
        config,
        preprocess=lambda row: dict(
            messages=[
                {"role": "system", "content": "You are a bot that responds with haikus."},
                {"role": "user", "content": row["item"]}
            ],
            sampling_params=dict(
                temperature=0.3,
                max_tokens=250,
            )
        ),
        postprocess=lambda row: dict(
            answer=row["generated_text"],
            **row  # This will return all the original columns in the dataset.
        ),
    )

    ds = ray.data.from_items(["Start of the haiku is: Complete this for me..."])

    ds = processor(ds)
    ds.show(limit=1)

.. testoutput::
    :options: +MOCK

    {'answer': 'Snowflakes gently fall\nBlanketing the winter scene\nFrozen peaceful hush'}

Each processor requires specific input columns. You can find more info by using the following API:

.. testcode::

    processor.log_input_column_names()

.. testoutput::
    :options: +MOCK

    The first stage of the processor is ChatTemplateStage.
    Required input columns:
            messages: A list of messages in OpenAI chat format. See https://platform.openai.com/docs/api-reference/chat/create for details.

Some models may require a Hugging Face token to be specified. You can specify the token in the `runtime_env` argument.

.. testcode::

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

.. testcode::

    from ray.data.llm import vLLMEngineProcessorConfig

    config = vLLMEngineProcessorConfig(
        model_source="unsloth/Llama-3.1-8B-Instruct",
        engine_kwargs={"max_model_len": 20000},
        concurrency=1,
        batch_size=64,
    )

For handling larger models, specify model parallelism.

.. testcode::

    config = vLLMEngineProcessorConfig(
        model_source="unsloth/Llama-3.1-8B-Instruct",
        engine_kwargs={
            "max_model_len": 16384,
            "tensor_parallel_size": 2,
            "pipeline_parallel_size": 2,
            "enable_chunked_prefill": True,
            "max_num_batched_tokens": 2048,
        },
        concurrency=1,
        batch_size=64,
    )

The underlying :class:`Processor <ray.data.llm.Processor>` object instantiates replicas of the vLLM engine and automatically
configure parallel workers to handle model parallelism (for tensor parallelism and pipeline parallelism,
if specified).

To optimize model loading, you can configure the `load_format` to `runai_streamer` or `tensorizer`.

.. note::
    In this case, install vLLM with runai dependencies: `pip install -U "vllm[runai]==0.7.2"`

.. testcode::

    config = vLLMEngineProcessorConfig(
        model_source="unsloth/Llama-3.1-8B-Instruct",
        engine_kwargs={"load_format": "runai_streamer"},
        concurrency=1,
        batch_size=64,
    )

If your model is hosted on AWS S3, you can specify the S3 path in the `model_source` argument, and specify `load_format="runai_streamer"` in the `engine_kwargs` argument.

.. testcode::

    config = vLLMEngineProcessorConfig(
        model_source="s3://your-bucket/your-model/",  # Make sure adding the trailing slash!
        engine_kwargs={"load_format": "runai_streamer"},
        runtime_env={"env_vars": {
            "AWS_ACCESS_KEY_ID": "your_access_key_id",
            "AWS_SECRET_ACCESS_KEY": "your_secret_access_key",
            "AWS_REGION": "your_region",
        }},
        concurrency=1,
        batch_size=64,
    )

To do multi-LoRA batch inference, you need to set LoRA related parameters in `engine_kwargs`. See :doc:`the vLLM with LoRA example</llm/examples/batch/vllm-with-lora>` for details.

.. testcode::

    config = vLLMEngineProcessorConfig(
        model_source="unsloth/Llama-3.1-8B-Instruct",
        engine_kwargs={
            enable_lora=True,
            max_lora_rank=32,
            max_loras=1,
        },
        concurrency=1,
        batch_size=64,
    )

.. _openai_compatible_api_endpoint:

Batch inference with an OpenAI-compatible endpoint
--------------------------------------------------

You can also make calls to deployed models that have an OpenAI compatible API endpoint.

.. testcode::

    import ray
    import os
    from ray.data.llm import HttpRequestProcessorConfig, build_llm_processor

    OPENAI_KEY = os.environ["OPENAI_API_KEY"]
    ds = ray.data.from_items(["Hand me a haiku."])


    config = HttpRequestProcessorConfig(
        url="https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_KEY}"},
        qps=1,
    )

    processor = build_llm_processor(
        config,
        preprocess=lambda row: dict(
            payload=dict(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a bot that responds with haikus."},
                    {"role": "user", "content": row["item"]}
                ],
                temperature=0.0,
                max_tokens=150,
            ),
        ),
        postprocess=lambda row: dict(response=row["http_response"]["choices"][0]["message"]["content"]),
    )

    ds = processor(ds)
    print(ds.take_all())

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

.. _production_guide:

Production guide
--------------------------------------------------

.. _model_cache:

Caching model weight to remote object storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

While deploying Ray Data LLM to large scale clusters, model loading may be rate
limited by HuggingFace. In this case, you can cache the model to remote object
storage (AWS S3 or Google Cloud Storage) for more stable model loading.

Ray Data LLM provides the following utility to help uploading models to remote object storage.

.. testcode::

    # Download model from HuggingFace, and upload to GCS
    python -m ray.llm.utils.upload_model \
        --model-source facebook/opt-350m \
        --bucket-uri gs://my-bucket/path/to/facebook-opt-350m
    # Or upload a local custom model to S3
    python -m ray.llm.utils.upload_model \
        --model-source local/path/to/model \
        --bucket-uri s3://my-bucket/path/to/model_name

And later you can use remote object store URI as `model_source` in the config.

.. testcode::

    config = vLLMEngineProcessorConfig(
        model_source="gs://my-bucket/path/to/facebook-opt-350m",  # or s3://my-bucket/path/to/model_name
        ...
    )
