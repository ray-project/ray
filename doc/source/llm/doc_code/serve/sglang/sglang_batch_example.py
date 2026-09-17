# __sglang_batch_start__
import ray
from ray.data.llm import SGLangEngineProcessorConfig, build_processor

config = SGLangEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs=dict(
        dtype="half",
        mem_fraction_static=0.8,
    ),
    batch_size=32,
    concurrency=1,
)

processor = build_processor(
    config,
    preprocess=lambda row: dict(
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": row["prompt"]},
        ],
        sampling_params=dict(
            temperature=0.7,
            max_new_tokens=256,
        ),
    ),
    postprocess=lambda row: dict(
        prompt=row["prompt"],
        response=row["generated_text"],
    ),
)

ds = ray.data.from_items(
    [
        {"prompt": "What is the capital of France?"},
        {"prompt": "Explain photosynthesis in one sentence."},
        {"prompt": "Write a haiku about programming."},
    ]
)
ds = processor(ds)

for row in ds.take_all():
    print(f"Prompt: {row['prompt']}")
    print(f"Response: {row['response']}\n")
# __sglang_batch_end__
