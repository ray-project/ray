"""
Quickstart: vLLM + Ray Data batch inference.

1. Installation
2. Dataset creation
3. Processor configuration
4. Running inference
5. Getting results
"""

# __minimal_vllm_quickstart_start__
import ray
from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor

# Initialize Ray
ray.init()

# simple dataset
ds = ray.data.from_items([
    {"prompt": "What is machine learning?"},
    {"prompt": "Explain neural networks in one sentence."},
])

# Minimal vLLM configuration
config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    concurrency=1,  # 1 vLLM engine replica
    batch_size=32,  # 32 samples per batch
)

# Build processor
# preprocess: converts input row to format expected by vLLM (OpenAI chat format)
# postprocess: extracts generated text from vLLM output
processor = build_llm_processor(
    config,
    preprocess=lambda row: {
        "messages": [{"role": "user", "content": row["prompt"]}],
        "sampling_params": {"temperature": 0.7, "max_tokens": 100},
    },
    postprocess=lambda row: {
        "prompt": row["prompt"],
        "response": row["generated_text"],
    },
)

# inference
ds = processor(ds)

# iterate through the results
for result in ds.iter_rows():
    print(f"Q: {result['prompt']}")
    print(f"A: {result['response']}\n")

# Alternative ways to get results:
# results = ds.take(10)  # Get first 10 results
# ds.show(limit=5)       # Print first 5 results
# ds.write_parquet("output.parquet")  # Save to file
# __minimal_vllm_quickstart_end__

