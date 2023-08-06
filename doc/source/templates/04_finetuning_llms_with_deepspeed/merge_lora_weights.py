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
from transformers import AutoModelForCausalLM

from finetune_hf_llm import get_pretrained_path

from utils import download_model, get_mirror_link, get_checkpoint_and_refs_dir




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
        "--model-name", required=True, type=str
    )

    parser.add_argument(
        "--checkpoint", type=str, required=True, help="Path to checkpoint containing the LoRA weights."
    )

    args = parser.parse_args()

    return args


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
    print(f"Downloading original model {model_id} ...")
    s3_bucket = get_mirror_link(model_id)
    ckpt_path, _ = get_checkpoint_and_refs_dir(model_id=model_id, bucket_uri=s3_bucket)

    download_model(
        model_id=model_id,
        bucket_uri=s3_bucket,
        s3_sync_args=["--no-sign-request"],
    )

    print(f"Loading original model from {ckpt_path} ...")

    model = AutoModelForCausalLM.from_pretrained(
        ckpt_path,
        trust_remote_code=True,
        torch_dtype=torch.bfloat16,
        # `use_cache=True` is incompatible with gradient checkpointing.
        use_cache=False,
    )
    print(f"Done downloading and loading model in {time.time() - s} seconds.")

    print(f"Loading and merging peft weights...")
    
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