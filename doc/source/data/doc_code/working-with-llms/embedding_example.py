"""
Documentation example and test for embedding model batch inference.

"""

import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "ray[llm]"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "numpy==1.26.4"])


def run_embedding_example():
    # __embedding_example_start__
    import ray
    from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor

    embedding_config = vLLMEngineProcessorConfig(
        model_source="sentence-transformers/all-MiniLM-L6-v2",
        task_type="embed",
        engine_kwargs=dict(
            enable_prefix_caching=False,
            enable_chunked_prefill=False,
            max_model_len=256,
            enforce_eager=True,
        ),
        batch_size=32,
        concurrency=1,
        apply_chat_template=False,
        detokenize=False,
    )

    embedding_processor = build_llm_processor(
        embedding_config,
        preprocess=lambda row: dict(prompt=row["text"]),
        postprocess=lambda row: {
            "text": row["prompt"],
            "embedding": row["embeddings"],
        },
    )

    texts = [
        "Hello world",
        "This is a test sentence",
        "Embedding models convert text to vectors",
    ]
    ds = ray.data.from_items([{"text": text} for text in texts])

    embedded_ds = embedding_processor(ds)
    embedded_ds.show(limit=1)
    # __embedding_example_end__


if __name__ == "__main__":
    try:
        import torch

        if torch.cuda.is_available():
            run_embedding_example()
        else:
            print("Skipping embedding example (no GPU available)")
    except Exception as e:
        print(f"Skipping embedding example: {e}")
