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
from ray.data.llm import vLLMEngineProcessorConfig, build_processor


def run_quickstart():
    # Initialize Ray
    ray.init()

    # Simple dataset
    ds = ray.data.from_items([
        {"prompt": "What is machine learning?"},
        {"prompt": "Explain neural networks in one sentence."},
    ])

    # Minimal vLLM configuration
    config = vLLMEngineProcessorConfig(
        model_source="unsloth/Llama-3.2-1B-Instruct",
        concurrency=1,  # 1 vLLM engine replica
        batch_size=8,  # Keep the quickstart lightweight for local runs
        engine_kwargs={
            "enforce_eager": True,
            "max_model_len": 2048,
        },
    )

    # Build processor
    # preprocess: converts input rows to the format expected by vLLM
    # postprocess: extracts generated text from the vLLM output
    processor = build_processor(
        config,
        preprocess=lambda row: {
            "messages": [{"role": "user", "content": row["prompt"]}],
            "sampling_params": {"temperature": 0.7, "max_tokens": 32},
        },
        postprocess=lambda row: {
            "prompt": row["prompt"],
            "response": row["generated_text"],
        },
    )

    # Inference
    ds = processor(ds)

    # Iterate through the results
    for result in ds.iter_rows():
        print(f"Q: {result['prompt']}")
        print(f"A: {result['response']}\n")

    # Alternative ways to get results:
    # results = ds.take(10)  # Get first 10 results
    # ds.show(limit=5)       # Print first 5 results
    # ds.write_parquet("output.parquet")  # Save to file


if __name__ == "__main__":
    try:
        import torch

        if torch.cuda.is_available():
            run_quickstart()
        else:
            print("Skipping quickstart run (no GPU available)")
    except Exception as e:
        print(f"Skipping quickstart run due to environment error: {e}")
# __minimal_vllm_quickstart_end__
