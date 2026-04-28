import gc
import sys

import pytest

import ray
from ray.data.llm import SGLangEngineProcessorConfig, build_processor


@pytest.fixture(autouse=True)
def _release_engine_gpus():
    """Force the SGLang engines of a finished test to release their GPUs.

    Ray Data keeps its MapWorker actors warm after a dataset completes, so the
    SGLangEngineWrapper (and therefore its SchedulerActors and placement group)
    stays alive until the worker is collected. With the parametrized cases
    reserving 2 and 4 GPUs on a 4-GPU cluster, the next case would otherwise
    block forever waiting on bundles the previous one still holds.

    SGLangEngineStageUDF.__del__ runs the real teardown; this just makes sure it
    happens between tests. The wrapper removes its own placement group, so
    nothing needs to be reaped by hand here.
    """
    yield
    gc.collect()


def test_chat_template():
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

    processor_config = SGLangEngineProcessorConfig(
        model_source="unsloth/Llama-3.2-1B-Instruct",
        engine_kwargs=dict(
            context_length=2048,
            disable_cuda_graph=True,
            dtype="half",
        ),
        batch_size=16,
        concurrency=1,
        chat_template_stage={"enabled": True, "chat_template": chat_template},
        tokenize_stage=True,
        detokenize_stage=True,
    )

    processor = build_processor(
        processor_config,
        preprocess=lambda row: dict(
            messages=[
                {"role": "system", "content": "You are a calculator"},
                {"role": "user", "content": f"{row['id']} ** 3 = ?"},
            ],
            sampling_params=dict(
                temperature=0.3,
                max_new_tokens=50,  # SGLang uses max_new_tokens instead of max_tokens
            ),
        ),
        postprocess=lambda row: {
            "resp": row["generated_text"],
        },
    )

    ds = ray.data.range(60)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})
    ds = processor(ds)
    ds = ds.materialize()
    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)


@pytest.mark.parametrize(
    "tp_size,dp_size,concurrency",
    [
        (2, 1, 2),
        (2, 2, 1),
    ],
)
def test_sglang_llama_parallel(tp_size, dp_size, concurrency):
    """Test SGLang with Llama model using different parallelism configurations."""
    runtime_env = {}

    processor_config = SGLangEngineProcessorConfig(
        model_source="unsloth/Llama-3.2-1B-Instruct",
        engine_kwargs=dict(
            context_length=2048,
            tp_size=tp_size,
            dp_size=dp_size,
            dtype="half",
        ),
        runtime_env=runtime_env,
        tokenize_stage=True,
        detokenize_stage=True,
        batch_size=16,
        concurrency=concurrency,
    )

    processor = build_processor(
        processor_config,
        preprocess=lambda row: dict(
            messages=[
                {"role": "system", "content": "You are a calculator"},
                {"role": "user", "content": f"{row['id']} ** 3 = ?"},
            ],
            sampling_params=dict(
                temperature=0.3,
                max_new_tokens=50,
            ),
        ),
        postprocess=lambda row: {
            "resp": row["generated_text"],
        },
    )

    ds = ray.data.range(120)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})
    ds = processor(ds)
    ds = ds.materialize()

    # Verify results
    outs = ds.take_all()
    assert len(outs) == 120
    assert all("resp" in out for out in outs)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
