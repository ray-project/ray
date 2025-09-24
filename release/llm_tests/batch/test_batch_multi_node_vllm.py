import pytest

import ray
from ray.data.llm import build_llm_processor, vLLMEngineProcessorConfig


@pytest.fixture(autouse=True)
def cleanup_ray_resources():
    """Automatically cleanup Ray resources between tests to prevent conflicts."""
    yield
    ray.shutdown()


@pytest.mark.parametrize(
    "tp_size,pp_size",
    [
        (2, 4),
        (4, 2),
    ],
)
def test_vllm_multi_node(tp_size, pp_size):
    config = vLLMEngineProcessorConfig(
        model_source="facebook/opt-1.3b",
        engine_kwargs=dict(
            enable_prefix_caching=True,
            enable_chunked_prefill=True,
            max_num_batched_tokens=4096,
            pipeline_parallel_size=pp_size,
            tensor_parallel_size=tp_size,
            distributed_executor_backend="ray",
        ),
        tokenize=False,
        detokenize=False,
        concurrency=1,
        batch_size=64,
        apply_chat_template=False,
    )

    processor = build_llm_processor(
        config,
        preprocess=lambda row: dict(
            prompt=f"You are a calculator. {row['id']} ** 3 = ?",
            sampling_params=dict(
                temperature=0.3,
                max_tokens=20,
                detokenize=True,
            ),
        ),
        postprocess=lambda row: dict(
            resp=row["generated_text"],
        ),
    )

    ds = ray.data.range(60)
    ds = processor(ds)
    ds = ds.materialize()

    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)
