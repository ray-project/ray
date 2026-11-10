import sys

import numpy as np
import pytest

import ray
from ray.data.llm import SGLangEngineProcessorConfig, build_processor


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
        model_source="Qwen/Qwen3.5-0.8B",
        engine_kwargs=dict(
            context_length=2048,
            disable_cuda_graph=True,
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
@pytest.mark.requires_4GPU
def test_sglang_qwen_parallel(tp_size, dp_size, concurrency):
    """Test SGLang with Qwen model using different parallelism configurations."""
    runtime_env = {}

    processor_config = SGLangEngineProcessorConfig(
        model_source="Qwen/Qwen3.5-0.8B",
        engine_kwargs=dict(
            context_length=2048,
            tp_size=tp_size,
            dp_size=dp_size,
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


SCORE_MODEL = "Qwen/Qwen3-0.6B"
SCORE_QUERY = "Is this city the capital of France? Answer Yes or No. City: "
SCORE_ROWS = [
    {"id": 0, "query": SCORE_QUERY, "items": ["Paris", "London", "Berlin"]},
    {"id": 1, "query": "Is 7 a prime number? Answer: ", "items": ["Yes", "No"]},
]
# Scores are probabilities; batching and MIS change the kernels, not the math.
SCORE_ATOL = 1e-2


@pytest.fixture(scope="module")
def score_tokenizer():
    from transformers import AutoTokenizer

    return AutoTokenizer.from_pretrained(SCORE_MODEL)


@pytest.fixture(scope="module")
def label_token_ids(score_tokenizer):
    return [
        score_tokenizer.encode(word, add_special_tokens=False)[0]
        for word in ("Yes", "No")
    ]


def _run_score(rows, task_kwargs, enable_mis=False, **config_kwargs):
    """Score `rows` with the Ray Data SGLang processor, ordered by `id`."""
    engine_kwargs = dict(context_length=2048)
    if enable_mis:
        engine_kwargs.update(enable_mis=True, attention_backend="flashinfer")
    config = SGLangEngineProcessorConfig(
        model_source=SCORE_MODEL,
        task_type="score",
        engine_kwargs=engine_kwargs,
        task_kwargs=task_kwargs,
        batch_size=4,
        concurrency=1,
        **config_kwargs,
    )
    outputs = build_processor(config)(ray.data.from_items(rows)).take_all()
    return [row["scores"] for row in sorted(outputs, key=lambda row: row["id"])]


@ray.remote(num_gpus=1)
def _score_with_engine(rows, label_token_ids):
    """Score `rows` text with `sglang.Engine` directly, as a reference."""
    import sglang

    engine = sglang.Engine(model_path=SCORE_MODEL, context_length=2048)
    try:
        return [
            engine.score(
                query=row["query"],
                items=row["items"],
                label_token_ids=label_token_ids,
                apply_softmax=True,
            ).scores
            for row in rows
        ]
    finally:
        engine.shutdown()


def _assert_scores_close(actual, expected):
    assert len(actual) == len(expected)
    for actual_row, expected_row in zip(actual, expected):
        np.testing.assert_allclose(
            np.asarray(actual_row), np.asarray(expected_row), atol=SCORE_ATOL
        )


def test_sglang_score_matches_engine(label_token_ids):
    task_kwargs = dict(label_token_ids=label_token_ids, apply_softmax=True)
    scores = _run_score(SCORE_ROWS, task_kwargs)
    expected = ray.get(_score_with_engine.remote(SCORE_ROWS, label_token_ids))

    _assert_scores_close(scores, expected)
    # Sanity check that the model, not noise, produced the scores.
    assert np.argmax(np.asarray(scores[0])[:, 0]) == 0


def test_sglang_score_mis_matches_single_item(label_token_ids):
    task_kwargs = dict(label_token_ids=label_token_ids, apply_softmax=True)
    single_item = _run_score(SCORE_ROWS, task_kwargs, enable_mis=False)
    multi_item = _run_score(SCORE_ROWS, task_kwargs, enable_mis=True)

    _assert_scores_close(multi_item, single_item)


@pytest.mark.parametrize("enable_mis", [False, True])
def test_sglang_score_embed_overrides(score_tokenizer, label_token_ids, enable_mis):
    """Overriding a marker with a token's own embedding scores like the token."""
    from transformers import AutoModelForCausalLM

    model = AutoModelForCausalLM.from_pretrained(SCORE_MODEL, torch_dtype="auto")
    embeddings = model.get_input_embeddings().weight.detach().float().numpy()
    del model

    marker_id = score_tokenizer.convert_tokens_to_ids("<|vision_pad|>")

    def encode(text):
        return score_tokenizer.encode(text, add_special_tokens=False)

    query_ids = encode("Is this city the capital of France? City:")
    city_ids = [encode(" Paris")[0], encode(" London")[0]]
    answer_ids = encode(" Answer:")

    rows = [
        # The reference row scores the city tokens themselves.
        {
            "id": 0,
            "query_token_ids": query_ids,
            "item_token_ids": [[city_id] + answer_ids for city_id in city_ids],
            "query_embed_overrides": None,
            "item_embed_overrides": [None, None],
        },
        # The override row replaces a query token and the first item's city
        # token with markers, overridden by the original tokens' embeddings.
        {
            "id": 1,
            "query_token_ids": query_ids[:-1] + [marker_id],
            "item_token_ids": [
                [marker_id] + answer_ids,
                [city_ids[1]] + answer_ids,
            ],
            "query_embed_overrides": embeddings[[query_ids[-1]]],
            "item_embed_overrides": [embeddings[[city_ids[0]]], None],
        },
    ]
    task_kwargs = dict(
        label_token_ids=label_token_ids,
        apply_softmax=True,
        embed_override_token_id=marker_id,
    )
    reference, overridden = _run_score(
        rows, task_kwargs, enable_mis=enable_mis, tokenize_stage=False
    )

    _assert_scores_close([overridden], [reference])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
