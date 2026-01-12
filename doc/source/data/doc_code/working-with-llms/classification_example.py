"""
Documentation example and test for classification model batch inference.

This example demonstrates how to use Ray Data LLM with sequence classification
models like educational content classifiers.
"""

import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "ray[llm]"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "numpy==1.26.4"])


def run_classification_example():
    # __classification_example_start__
    import ray
    from ray.data.llm import vLLMEngineProcessorConfig, build_processor

    # Configure vLLM for a sequence classification model
    classification_config = vLLMEngineProcessorConfig(
        model_source="nvidia/nemocurator-fineweb-nemotron-4-edu-classifier",
        task_type="classify",  # Use 'classify' for sequence classification models
        engine_kwargs=dict(
            max_model_len=512,
            enforce_eager=True,
        ),
        batch_size=8,
        concurrency=1,
        apply_chat_template=False,
        detokenize=False,
    )

    classification_processor = build_processor(
        classification_config,
        preprocess=lambda row: dict(prompt=row["text"]),
        postprocess=lambda row: {
            "text": row["prompt"],
            # Classification models return logits in the 'embeddings' field
            "edu_score": float(row["embeddings"][0])
            if row.get("embeddings") is not None and len(row["embeddings"]) > 0
            else None,
        },
    )

    # Sample texts with varying educational quality
    texts = [
        "lol that was so funny haha",
        "Photosynthesis converts light energy into chemical energy.",
        "Newton's laws describe the relationship between forces and motion.",
    ]
    ds = ray.data.from_items([{"text": text} for text in texts])

    classified_ds = classification_processor(ds)
    classified_ds.show(limit=3)
    # __classification_example_end__


if __name__ == "__main__":
    try:
        import torch

        if torch.cuda.is_available():
            run_classification_example()
        else:
            print("Skipping classification example (no GPU available)")
    except Exception as e:
        print(f"Skipping classification example: {e}")

