.. _working-with-llms:

Working with LLMs
=================

The `ray.data.llm` module integrates with key large language model (LLM) inference engines and deployed models to enable LLM batch inference.

This guide shows you how to use `ray.data.llm` to:

* :ref:`Perform batch inference with LLMs <batch_inference_llm>`
* :ref:`Configure vLLM for LLM inference <vllm_llm>`
* :ref:`Query deployed models with an OpenAI compatible API endpoint <openai_compatible_api_endpoint>`

.. _batch_inference_llm:

Perform batch inference with LLMs
---------------------------------

At a high level, the `ray.data.llm` module provides a `Processor` object which encapsulates
logic for performing batch inference with LLMs on a Ray Data dataset.

You can use the `build_llm_processor` API to construct a processor. In the following example, we use the `vLLMEngineProcessorConfig` to construct a processor for the `meta-llama/Llama-3.1-8B-Instruct` model.

The vLLMEngineProcessorConfig is a configuration object for the vLLM engine.
It contains the model name, the number of GPUs to use, and the number of shards to use, along with other vLLM engine configurations. Upon execution, the Processor object instantiates replicas of the vLLM engine (using `map_batches` under the hood).

.. testcode::

    import ray
    from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor
    import numpy as np

    config = vLLMEngineProcessorConfig(
        model="unsloth/Llama-3.1-8B-Instruct",
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
            answer=row["generated_text"]
        ),
    )

    ds = ray.data.from_items(["Start of the haiku is: Complete this for me..."])

    ds = processor(ds)
    ds.show(limit=1)

.. testoutput::
    :options: +MOCK

    {'answer': 'Snowflakes gently fall\nBlanketing the winter scene\nFrozen peaceful hush'}

.. _vllm_llm:

Configure vLLM for LLM inference
--------------------------------

Use the `vLLMEngineProcessorConfig` to configure the vLLM engine.

.. testcode::

    from ray.data.llm import vLLMEngineProcessorConfig

    processor_config = vLLMEngineProcessorConfig(
        model="unsloth/Llama-3.1-8B-Instruct",
        engine_kwargs={"max_model_len": 20000},
        concurrency=1,
        batch_size=64,
    )

For handling larger models, specify model parallelism.

.. testcode::

    processor_config = vLLMEngineProcessorConfig(
        model="unsloth/Llama-3.1-8B-Instruct",
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

The underlying `Processor` object instantiates replicas of the vLLM engine and automatically
configure parallel workers to handle model parallelism (for tensor parallelism and pipeline parallelism,
if specified).


.. _openai_compatible_api_endpoint:

OpenAI Compatible API Endpoint
------------------------------

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
