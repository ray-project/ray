"""
Documentation example and test for custom tokenizer batch inference.

Demonstrates how to use vLLM's tokenizer infrastructure for models whose
tokenizers are not natively supported by HuggingFace (e.g. Mistral Tekken,
DeepSeek-V3.2, Grok-2 tiktoken).

This example uses a standard model to demonstrate the pattern. For models
that truly require vLLM's custom tokenizer (e.g. deepseek-ai/DeepSeek-V3-0324),
replace the model ID and adjust tokenizer_mode accordingly.
"""

# __custom_chat_template_start__
from typing import Any, Dict, List
from vllm.tokenizers import get_tokenizer


class VLLMChatTemplate:
    """Apply a chat template using vLLM's tokenizer."""

    def __init__(self, model_id: str, tokenizer_mode: str = "auto"):
        self.tokenizer = get_tokenizer(
            model_id,
            tokenizer_mode=tokenizer_mode,
            trust_remote_code=True,
        )

    async def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        prompts: List[str] = []
        all_messages: List[List[Dict[str, Any]]] = []

        for messages in batch["messages"]:
            if hasattr(messages, "tolist"):
                messages = messages.tolist()
            all_messages.append(messages)

            add_generation_prompt = messages[-1]["role"] == "user"
            prompt = self.tokenizer.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=add_generation_prompt,
                continue_final_message=not add_generation_prompt,
            )
            prompts.append(prompt)

        return {
            "prompt": prompts,
            "messages": all_messages,
            "sampling_params": batch["sampling_params"],
        }


# __custom_chat_template_end__


# __custom_tokenize_start__
class VLLMTokenize:
    """Tokenize text prompts using vLLM's tokenizer."""

    def __init__(self, model_id: str, tokenizer_mode: str = "auto"):
        self.tokenizer = get_tokenizer(
            model_id,
            tokenizer_mode=tokenizer_mode,
            trust_remote_code=True,
        )

    async def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        all_tokenized: List[List[int]] = [
            self.tokenizer.encode(prompt) for prompt in batch["prompt"]
        ]

        return {
            "tokenized_prompt": all_tokenized,
            "messages": batch["messages"],
            "sampling_params": batch["sampling_params"],
        }


# __custom_tokenize_end__


# __custom_detokenize_start__
class VLLMDetokenize:
    """Detokenize generated token IDs using vLLM's tokenizer."""

    def __init__(self, model_id: str, tokenizer_mode: str = "auto"):
        self.tokenizer = get_tokenizer(
            model_id,
            tokenizer_mode=tokenizer_mode,
            trust_remote_code=True,
        )

    async def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        decoded: List[str] = []
        for tokens in batch["generated_tokens"]:
            if hasattr(tokens, "tolist"):
                tokens = tokens.tolist()
            decoded.append(self.tokenizer.decode(tokens, skip_special_tokens=True))

        return {
            **batch,
            "generated_text_custom": decoded,
        }


# __custom_detokenize_end__


def run_custom_tokenizer_example():
    import ray
    from ray.data.llm import vLLMEngineProcessorConfig, build_processor

    # Input dataset with sampling_params per row.
    ds = ray.data.from_items(
        [
            {
                "messages": [
                    {"role": "user", "content": "What is the capital of France?"}
                ],
                "sampling_params": {"max_tokens": 256, "temperature": 0.7},
            },
            {
                "messages": [
                    {"role": "user", "content": "Write a haiku about computing."}
                ],
                "sampling_params": {"max_tokens": 256, "temperature": 0.7},
            },
        ]
    )

    # __custom_tokenizer_pipeline_start__
    MODEL_ID = "unsloth/Llama-3.1-8B-Instruct"

    config = vLLMEngineProcessorConfig(
        model_source=MODEL_ID,
        engine_kwargs=dict(
            max_model_len=4096,
            trust_remote_code=True,
            tokenizer_mode="auto",
        ),
        batch_size=4,
        concurrency=1,
        # Disable built-in stages -- we handle them via map_batches.
        chat_template_stage=False,
        tokenize_stage=False,
        detokenize_stage=False,
    )

    processor = build_processor(
        config,
        postprocess=lambda row: {
            "generated_text": row.get("generated_text", ""),
            "generated_tokens": row.get("generated_tokens", []),
            "num_input_tokens": row.get("num_input_tokens", 0),
            "num_generated_tokens": row.get("num_generated_tokens", 0),
        },
    )

    ds = ds.map_batches(
        VLLMChatTemplate,
        fn_constructor_kwargs={"model_id": MODEL_ID},
        concurrency=1,
        batch_size=4,
    )

    ds = ds.map_batches(
        VLLMTokenize,
        fn_constructor_kwargs={"model_id": MODEL_ID},
        concurrency=1,
        batch_size=4,
    )

    ds = processor(ds)

    ds = ds.map_batches(
        VLLMDetokenize,
        fn_constructor_kwargs={"model_id": MODEL_ID},
        concurrency=1,
        batch_size=4,
    )

    # __custom_tokenizer_pipeline_end__
    ds.show(limit=2)


if __name__ == "__main__":
    try:
        import torch

        if torch.cuda.is_available():
            run_custom_tokenizer_example()
        else:
            print("Skipping custom tokenizer example (no GPU available)")
    except Exception as e:
        print(f"Skipping custom tokenizer example: {e}")
