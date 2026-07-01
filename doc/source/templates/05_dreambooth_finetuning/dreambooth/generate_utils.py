from diffusers import DiffusionPipeline
import torch


def get_pipeline(model_dir, lora_weights_dir=None):
    pipeline = DiffusionPipeline.from_pretrained(model_dir, torch_dtype=torch.float16)
    if lora_weights_dir:
        # `load_lora_weights` reads the diffusers-format adapter checkpoint
        # written by `save_lora_weights` in train.py and fuses it into the
        # pipeline's unet and text encoder.
        print(f"Loading LoRA weights from {lora_weights_dir}")
        pipeline.load_lora_weights(lora_weights_dir)
    return pipeline
