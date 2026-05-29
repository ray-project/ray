"""
This script merges the weights of a LoRA checkpoint with the base model weights
to create a single model that can be used for model evaluation.
"""

import torch
import argparse
import time
import peft
from pathlib import Path

from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    StoppingCriteriaList,
)

from utils import download_model, get_mirror_link, get_checkpoint_and_refs_dir

# In addition to merging the lora weights, you can also formulate a prompt for the
# model here to quickly test it after merging
TEST_EVAL = False
TEST_PROMPT = (
    "<START_Q>Natalia sold clips to 48 of her friends in April, and then "
    "she sold half as many clips in May. How many clips did Natalia sell "
    "altogether in April and May?<END_Q><START_A>"
)
STOP_TOKEN = "<END_A>"


def parse_args():
    parser = argparse.ArgumentParser(description="Simple example of training script.")

    parser.add_argument(
        "--output-path",
        type=str,
        help="Path to output directory. Defaults to the original checkpoint directory.",
        required=True,
    )

    parser.add_argument("--model-name", required=True, type=str, help="7b, 13b or 70b.")

    parser.add_argument(
        "--checkpoint",
        type=str,
        required=True,
        help="Path to checkpoint containing the LoRA weights.",
    )

    args = parser.parse_args()

    return args


def test_eval(model, tokenizer):
    """Query the model with a single prompt to sanity check it."""

    print("Starting model evaluation...")

    model.eval()
    model.to("cuda")

    print("Prompting model with prompt : ", TEST_PROMPT)
    input_ids = tokenizer(TEST_PROMPT, return_tensors="pt")["input_ids"].to("cuda")

    stop_token_embedding = tokenizer(
        STOP_TOKEN, return_tensors="pt", add_special_tokens=False
    )["input_ids"].to("cuda")

    def custom_stopping_criteria(embeddings, *args, **kwargs) -> bool:
        return stop_token_embedding in embeddings

    stopping_criteria = StoppingCriteriaList([custom_stopping_criteria])

    with torch.no_grad():
        generation_output = model.generate(
            input_ids=input_ids,
            output_scores=True,
            max_new_tokens=500,
            stopping_criteria=stopping_criteria,
        )

    decoded = tokenizer.batch_decode(generation_output)
    print("Outputs: ", decoded)


def main():
    args = parse_args()

    # Sanity checks
    if not Path(args.checkpoint).exists():
        raise ValueError(f"Checkpoint {args.checkpoint} does not exist.")

    if not args.output_path:
        args.output_path = Path(args.checkpoint) / "merged_model"
        print(f"Output path not specified. Using {args.output_path}")

    Path(args.output_path).mkdir(parents=True, exist_ok=True)

    # Load orignal model
    s = time.time()
    model_id = f"meta-llama/Llama-2-{args.model_name}-hf"
    s3_bucket = get_mirror_link(model_id)
    ckpt_path, _ = get_checkpoint_and_refs_dir(model_id=model_id, bucket_uri=s3_bucket)

    print(f"Downloading original model {model_id} from {s3_bucket} to {ckpt_path} ...")
    print("Loading tokenizer...")

    tokenizer = AutoTokenizer.from_pretrained(args.checkpoint, legacy=True)
    tokenizer.save_pretrained(Path(args.output_path))

    print(f"Saved tokenizer to {args.output_path}")

    download_model(
        model_id=model_id,
        bucket_uri=s3_bucket,
        s3_sync_args=["--no-sign-request"],
    )

    print(f"Downloading to {ckpt_path} finished after {time.time() - s} seconds.")
    print(f"Loading original model from {ckpt_path} ...")

    s2 = time.time()

    model = AutoModelForCausalLM.from_pretrained(
        ckpt_path,
        trust_remote_code=True,
        torch_dtype=torch.bfloat16,
        use_cache=False,
    )
    model.resize_token_embeddings(len(tokenizer))

    print(f"Done downloading and loading model after {time.time() - s2} seconds.")
    print("Loading and merging peft weights...")
    s3 = time.time()

    # Load LoRA weights
    model: peft.PeftModel = peft.PeftModel.from_pretrained(
        model=model,
        model_id=args.checkpoint,
    )

    # Merge weights and save
    model = model.merge_and_unload()
    output_path = Path(args.output_path)
    model.save_pretrained(output_path, safe_serialization=True)
    model.config.save_pretrained(output_path)

    print(f"Saved merged model to {args.output_path} after {time.time() - s3} seconds.")
    print(f"This script took {time.time() - s} seconds to execute.")

    if TEST_EVAL:
        test_eval(model, tokenizer)


if __name__ == "__main__":
    main()
