"""
This script merges the weights of a LoRA checkpoint with the base model weights
to create a single model that can be used for inference.
"""

import torch
import argparse
import time
import peft
from pathlib import Path

from transformers import (
    AutoModelForCausalLM,
)

from finetune_hf_llm import get_pretrained_path


def parse_args():

    parser = argparse.ArgumentParser(description="Simple example of training script.")
    parser.add_argument(
        "--mx",
        type=str,
        default="bf16",
        choices=["no", "fp16", "bf16", "fp8"],
        help="Whether to use mixed precision. Choose"
        "between fp16 and bf16 (bfloat16). Bf16 requires PyTorch >= 1.10."
        "and an Nvidia Ampere GPU.",
    )

    parser.add_argument("--output-path", type=str, default=None, help="Path to output directory. Defaults to the orginal checkpoint directory.")

    parser.add_argument(
        "--model-name", default="meta-llama/Llama-2-7b-chat-hf", type=str
    )

    parser.add_argument(
        "--checkpoint", type=str, help="Path to checkpoint containing the LoRA weights."
    )

    args = parser.parse_args()

    return args


def main():
    args = parse_args()

    # Sanity checks
    if not Path(args.checkpoint).exists():
        raise ValueError(f"Checkpoint {args.checkpoint} does not exist.")
    
    if not args.output_path:
        args.output_path = Path(args.checkpoint) + "/merged_model"
        print(f"Output path not specified. Using {args.output_path}")
    
    Path(args.output_path).mkdir(parents=True, exist_ok=True)


    # Load orignal model
    pretrained_path = get_pretrained_path(args.model_name)
    print(f"Loading model from {pretrained_path} ...")
    s = time.time()
    model = AutoModelForCausalLM.from_pretrained(
        pretrained_path,
        trust_remote_code=True,
        torch_dtype=torch.bfloat16,
        # `use_cache=True` is incompatible with gradient checkpointing.
        use_cache=False,
    )
    print(f"Done loading model in {time.time() - s} seconds.")
    
    # Load LoRA weights
    model = peft.PeftModel.from_pretrained(
        model, 
        args.checkpoint, 
        torch_dtype=torch.bfloat16,
    )

    # Merge weights and save
    model = model.merge_and_unload()
    model.save_pretrained(Path(args.output_path))

    print(f"Saved merged model to {args.output_path}")


if __name__ == "__main__":
    main()