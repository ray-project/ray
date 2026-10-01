"""
This file serves as a documentation example for aggregated tokenization.

"""

import ray
from ray.data.llm import (
    vLLMEngineProcessorConfig,
    build_processor,
    TokenizerStageConfig,
    DetokenizeStageConfig,
)

# __aggregated_tokenization_start__
config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs={"max_model_len": 4096},
    concurrency=1,
    batch_size=64,
    tokenize_stage=TokenizerStageConfig(enabled=False),
    detokenize_stage=DetokenizeStageConfig(enabled=False),
)

processor = build_processor(
    config,
    preprocess=lambda row: dict(
        messages=[
            {"role": "user", "content": row["item"]},
        ],
        sampling_params=dict(
            temperature=0.3,
            max_tokens=250,
            # Let the vLLM engine handle detokenization
            detokenize=True,
        ),
    ),
    postprocess=lambda row: dict(resp=row["generated_text"]),
)
# __aggregated_tokenization_end__

ds = ray.data.from_items(["Hello world!"])

if __name__ == "__main__":
    try:
        import torch

        if torch.cuda.is_available():
            ds = processor(ds)
            ds.show(limit=1)
        else:
            print("Skipping aggregated run (no GPU available)")
    except Exception as e:
        print(f"Skipping aggregated run due to environment error: {e}")
