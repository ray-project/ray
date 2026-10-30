import pytest

import ray
from ray import serve
from ray.data.llm import ServeDeploymentProcessorConfig, build_processor
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_llm_deployment
from ray.serve.llm.openai_api_models import ChatCompletionRequest, CompletionRequest


@pytest.fixture
def create_model_qwen3_5_0_8b_deployment(gpu_type, serve_cleanup):
    """Create a serve deployment for testing."""
    app_name = "test_serve_deployment_processor_app"
    deployment_name = "test_deployment_name"

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
            model_id="Qwen/Qwen3.5-0.8B",
            model_source="Qwen/Qwen3.5-0.8B",
        ),
        accelerator_type=gpu_type,
        deployment_config=dict(
            name="test_deployment_name",  # This is not necessarily the final deployment name
            autoscaling_config=dict(
                min_replicas=1,
                max_replicas=1,
            ),
        ),
        engine_kwargs=dict(
            enable_prefix_caching=True,
            enable_chunked_prefill=True,
            max_num_batched_tokens=4096,
            max_model_len=2048,
            chat_template=chat_template,
        ),
    )

    llm_app = build_llm_deployment(
        llm_config, override_serve_options=dict(name=deployment_name)
    )
    serve.run(llm_app, name=app_name)
    yield deployment_name, app_name


def test_completion_model(create_model_qwen3_5_0_8b_deployment):
    deployment_name, app_name = create_model_qwen3_5_0_8b_deployment
    config = ServeDeploymentProcessorConfig(
        deployment_name=deployment_name,
        app_name=app_name,
        dtype_mapping={
            "CompletionRequest": CompletionRequest,
        },
        batch_size=16,
        concurrency=1,
    )

    processor = build_processor(
        config,
        preprocess=lambda row: dict(
            method="completions",
            dtype="CompletionRequest",
            request_kwargs=dict(
                model="Qwen/Qwen3.5-0.8B",
                prompt=row["prompt"],
                stream=False,
            ),
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


def test_multi_turn_completion_model(create_model_qwen3_5_0_8b_deployment):
    deployment_name, app_name = create_model_qwen3_5_0_8b_deployment

    config1 = ServeDeploymentProcessorConfig(
        deployment_name=deployment_name,
        app_name=app_name,
        dtype_mapping={
            "CompletionRequest": CompletionRequest,
        },
        # Use lower batch size to reduce resource usage as there are multiple processors
        batch_size=4,
        concurrency=1,
    )

    processor1 = build_processor(
        config1,
        preprocess=lambda row: dict(
            dtype="CompletionRequest",
            method="completions",
            request_kwargs=dict(
                model="Qwen/Qwen3.5-0.8B",
                prompt=row["prompt"],
                stream=False,
            ),
        ),
        postprocess=lambda row: dict(
            prompt=row["choices"][0]["text"],
        ),
    )

    config2 = ServeDeploymentProcessorConfig(
        deployment_name=deployment_name,
        app_name=app_name,
        dtype_mapping={
            "CompletionRequest": CompletionRequest,
        },
        batch_size=4,
        concurrency=1,
    )

    processor2 = build_processor(
        config2,
        preprocess=lambda row: dict(
            dtype="CompletionRequest",
            method="completions",
            request_kwargs=dict(
                model="Qwen/Qwen3.5-0.8B",
                prompt=row["prompt"],
                stream=False,
            ),
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


def test_chat_model(create_model_qwen3_5_0_8b_deployment):
    deployment_name, app_name = create_model_qwen3_5_0_8b_deployment
    config = ServeDeploymentProcessorConfig(
        deployment_name=deployment_name,
        app_name=app_name,
        dtype_mapping={
            "ChatCompletionRequest": ChatCompletionRequest,
        },
        batch_size=16,
        concurrency=1,
    )

    processor = build_processor(
        config,
        preprocess=lambda row: dict(
            dtype="ChatCompletionRequest",
            method="chat",
            request_kwargs=dict(
                model="Qwen/Qwen3.5-0.8B",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant"},
                    {"role": "user", "content": f"Hello {row['id']}"},
                ],
                stream=False,
            ),
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
