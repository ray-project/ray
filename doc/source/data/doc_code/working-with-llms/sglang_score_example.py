"""
Score items against a query with SGLang and Ray Data LLM.

Requires SGLang with FlashInfer for multi-item scoring.
"""

# __sglang_score_example_start__
import ray
from transformers import AutoTokenizer
from ray.data.llm import SGLangEngineProcessorConfig, build_processor

model = "Qwen/Qwen3-0.6B"
tokenizer = AutoTokenizer.from_pretrained(model)
# The score of each item is the probability of these tokens following it.
label_token_ids = [
    tokenizer.encode(word, add_special_tokens=False)[0] for word in ("Yes", "No")
]

config = SGLangEngineProcessorConfig(
    model_source=model,
    task_type="score",
    # Multi-item scoring scores all items of a row in one forward pass.
    engine_kwargs=dict(enable_mis=True, attention_backend="flashinfer"),
    task_kwargs=dict(label_token_ids=label_token_ids, apply_softmax=True),
    batch_size=16,
    concurrency=1,
)
processor = build_processor(config)

ds = ray.data.from_items(
    [
        {
            "query": "Is this city the capital of France? ",
            "items": ["Paris", "London", "Berlin"],
        }
    ]
)

if __name__ == "__main__":
    import importlib.util

    import torch

    if importlib.util.find_spec("sglang") is None:
        print("Skipping SGLang score run (SGLang isn't installed)")
    elif not torch.cuda.is_available():
        print("Skipping SGLang score run (no GPU available)")
    else:
        for row in processor(ds).take_all():
            # One [p_yes, p_no] list per item, in item order.
            print(row["items"], row["scores"])
# __sglang_score_example_end__
