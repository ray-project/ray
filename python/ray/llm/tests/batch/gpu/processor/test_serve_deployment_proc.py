import sys
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray import serve
from ray.llm._internal.batch.processor import ProcessorBuilder
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_llm_deployment
from ray.llm._internal.batch.processor.serve_deployment_proc import (
    ServeDeploymentProcessorConfig,
)
from ray.llm._internal.serve.configs.openai_api_models import (
    CompletionRequest,
    ChatCompletionRequest,
    EmbeddingCompletionRequest,
    EmbeddingChatRequest,
)


@pytest.fixture
def create_serve_deployment(gpu_type, model_opt_125m):
    app_name = "test_serve_deployment_processor_app"

    chat_template = """
{% if messages[0]['role'] == 'system' %}
    {% set offset = 1 %}
{% else %}
    {% set offset = 0 %}
{% endif %}

{{ bos_token }}
{% for message in messages %}
    {% if (message['role'] == 'user') != (loop.index0 % 2 == offset) %}
        {{ raise_exception('Conversation roles must alternate user/assistant/user/assistant/...') }}
    {% endif %}

    {{ '<|im_start|>' + message['role'] + '\n' + message['content'] | trim + '<|im_end|>\n' }}
{% endfor %}

{% if add_generation_prompt %}
    {{ '<|im_start|>assistant\n' }}
{% endif %}
    """

    # Create a vLLM serve deployment
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=model_opt_125m,
            model_source=model_opt_125m,
        ),
        accelerator_type=gpu_type,
        deployment_config=dict(
            name="facebook",
            autoscaling_config=dict(
                min_replicas=1,
                max_replicas=1,
            ),
        ),
        engine_kwargs=dict(
            enable_prefix_caching=True,
            enable_chunked_prefill=True,
            max_num_batched_tokens=4096,
            # Add chat template for OPT model to enable chat API
            chat_template=chat_template,
        ),
    )

    llm_app = build_llm_deployment(llm_config)
    app = serve.run(llm_app, name=app_name)
    yield llm_config, app_name
    serve.shutdown()


def test_serve_deployment_processor(gpu_type, model_opt_125m):
    app_name = "test_serve_deployment_processor_app"
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=model_opt_125m,
            model_source=model_opt_125m,
        ),
        accelerator_type=gpu_type,
        deployment_config=dict(
            name="facebook",
            autoscaling_config=dict(
                min_replicas=1,
                max_replicas=1,
            ),
        ),
        engine_kwargs=dict(
            enable_prefix_caching=True,
            enable_chunked_prefill=True,
            max_num_batched_tokens=4096,
        ),
    )

    config = ServeDeploymentProcessorConfig(
        llm_config=llm_config,
        app_name=app_name,
        method="completions",
        batch_size=16,
        concurrency=1,
    )

    processor = ProcessorBuilder.build(config)
    assert processor.list_stage_names() == [
        "ServeDeploymentStage",
    ]

    stage = processor.get_stage_by_name("ServeDeploymentStage")
    assert stage.fn_constructor_kwargs == {
        "deployment_name": llm_config.deployment_name,
        "app_name": app_name,
        "method": "completions",
    }

    assert stage.map_batches_kwargs == {
        "concurrency": 1,
    }


def test_completion_model(model_opt_125m, create_serve_deployment):
    llm_config, app_name = create_serve_deployment
    config = ServeDeploymentProcessorConfig(
        llm_config=llm_config,
        app_name=app_name,
        method="completions",
        batch_size=16,
        concurrency=1,
    )

    processor = ProcessorBuilder.build(
        config,
        preprocess=lambda row: CompletionRequest(
            model=model_opt_125m, prompt=row["prompt"], stream=False
        ),
        postprocess=lambda row: dict(
            resp=row["choices"][0]["text"],
        ),
    )

    ds = ray.data.range(60)
    ds = ds.map(lambda x: {"prompt": f"Hello {x['id']}"})
    ds = processor(ds)
    ds = ds.materialize()
    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)


def test_multi_turn_completion_model(model_opt_125m, create_serve_deployment):
    llm_config, app_name = create_serve_deployment

    config1 = ServeDeploymentProcessorConfig(
        llm_config=llm_config,
        app_name=app_name,
        method="completions",
        # Use lower batch size to reduce resource usage as there are multiple processors
        batch_size=4,
        concurrency=1,
    )

    processor1 = ProcessorBuilder.build(
        config1,
        preprocess=lambda row: CompletionRequest(
            model=model_opt_125m, prompt=row["prompt"], stream=False
        ),
        postprocess=lambda row: dict(
            prompt=row["choices"][0]["text"],
        ),
    )

    config2 = ServeDeploymentProcessorConfig(
        llm_config=llm_config,
        app_name=app_name,
        method="completions",
        batch_size=4,
        concurrency=1,
    )

    processor2 = ProcessorBuilder.build(
        config2,
        preprocess=lambda row: CompletionRequest(
            model=model_opt_125m, prompt=row["prompt"], stream=False
        ),
        postprocess=lambda row: dict(
            resp=row["choices"][0]["text"],
        ),
    )

    ds = ray.data.range(60)
    ds = ds.map(lambda x: {"prompt": f"Hello {x['id']}"})
    ds = processor1(ds)
    ds = processor2(ds)

    ds = ds.materialize()
    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)


def test_chat_model(model_opt_125m, create_serve_deployment):
    """Test chat functionality with OPT model using vLLM's chat template wrapper."""
    llm_config, app_name = create_serve_deployment
    config = ServeDeploymentProcessorConfig(
        llm_config=llm_config,
        app_name=app_name,
        method="chat",
        batch_size=16,
        concurrency=1,
    )

    processor = ProcessorBuilder.build(
        config,
        preprocess=lambda row: ChatCompletionRequest(
            model=model_opt_125m,
            messages=[
                {"role": "system", "content": "You are a helpful assistant"},
                {"role": "user", "content": f"Hello {row['id']}"},
            ],
            stream=False,
        ),
        postprocess=lambda row: dict(
            resp=row["choices"][0]["message"]["content"],
        ),
    )

    ds = ray.data.range(60)
    ds = ds.map(lambda x: {"id": x["id"]})
    ds = processor(ds)
    ds = ds.materialize()
    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
