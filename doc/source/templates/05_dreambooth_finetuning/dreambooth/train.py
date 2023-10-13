from typing import Dict

import itertools
from diffusers import (
    AutoencoderKL,
    DDPMScheduler,
    DiffusionPipeline,
    UNet2DConditionModel,
)

# LoRA related imports begin ##
from diffusers.loaders import (
    LoraLoaderMixin,
    text_encoder_lora_state_dict,
)
from diffusers.models.attention_processor import (
    AttnAddedKVProcessor,
    AttnAddedKVProcessor2_0,
    LoRAAttnAddedKVProcessor,
    LoRAAttnProcessor,
    LoRAAttnProcessor2_0,
    SlicedAttnAddedKVProcessor,
)

# LoRA related imports end ##
from diffusers.utils.import_utils import is_xformers_available
from ray.train import ScalingConfig
from ray import train
from ray.train.torch import TorchTrainer
import torch
import torch.nn.functional as F
from torch.nn.utils import clip_grad_norm_
from transformers import CLIPTextModel

from dataset import collate, get_train_dataset
from flags import train_arguments

LORA_RANK = 4


def prior_preserving_loss(model_pred, target, weight):
    # Chunk the noise and model_pred into two parts and compute
    # the loss on each part separately.
    model_pred, model_pred_prior = torch.chunk(model_pred, 2, dim=0)
    target, target_prior = torch.chunk(target, 2, dim=0)

    # Compute instance loss
    loss = F.mse_loss(model_pred.float(), target.float(), reduction="mean")

    # Compute prior loss
    prior_loss = F.mse_loss(
        model_pred_prior.float(), target_prior.float(), reduction="mean"
    )

    # Add the prior loss to the instance loss.
    return loss + weight * prior_loss


def get_target(scheduler, noise, latents, timesteps):
    """Get the target for loss depending on the prediction type."""
    pred_type = scheduler.config.prediction_type
    if pred_type == "epsilon":
        return noise
    if pred_type == "v_prediction":
        return scheduler.get_velocity(latents, noise, timesteps)
    raise ValueError(f"Unknown prediction type {pred_type}")


def add_lora_layers(unet, text_encoder):
    """Add LoRA layers for unet and text encoder.

    `unet` and `text_encoder` will be modified in place.

    Returns:
        The LoRA parameters for unet and text encoder correspondingly.
    """
    unet_lora_attn_procs = {}
    unet_lora_parameters = []
    for name, attn_processor in unet.attn_processors.items():
        cross_attention_dim = (
            None
            if name.endswith("attn1.processor")
            else unet.config.cross_attention_dim
        )
        if name.startswith("mid_block"):
            hidden_size = unet.config.block_out_channels[-1]
        elif name.startswith("up_blocks"):
            block_id = int(name[len("up_blocks.")])
            hidden_size = list(reversed(unet.config.block_out_channels))[block_id]
        elif name.startswith("down_blocks"):
            block_id = int(name[len("down_blocks.")])
            hidden_size = unet.config.block_out_channels[block_id]

        if isinstance(
            attn_processor,
            (AttnAddedKVProcessor, SlicedAttnAddedKVProcessor, AttnAddedKVProcessor2_0),
        ):
            lora_attn_processor_class = LoRAAttnAddedKVProcessor
        else:
            lora_attn_processor_class = (
                LoRAAttnProcessor2_0
                if hasattr(F, "scaled_dot_product_attention")
                else LoRAAttnProcessor
            )

        module = lora_attn_processor_class(
            hidden_size=hidden_size,
            cross_attention_dim=cross_attention_dim,
            rank=LORA_RANK,
        )
        unet_lora_attn_procs[name] = module
        unet_lora_parameters.extend(module.parameters())

    unet.set_attn_processor(unet_lora_attn_procs)

    text_lora_parameters = LoraLoaderMixin._modify_text_encoder(
        text_encoder, dtype=torch.float32, rank=LORA_RANK
    )

    return unet_lora_parameters, text_lora_parameters


def load_models(config):
    """Load pre-trained Stable Diffusion models."""
    # Load all models in bfloat16 to save GRAM.
    # For models that are only used for inferencing,
    # full precision is also not required.
    dtype = torch.bfloat16

    text_encoder = CLIPTextModel.from_pretrained(
        args.model_dir,
        subfolder="text_encoder",
        torch_dtype=dtype,
    )

    noise_scheduler = DDPMScheduler.from_pretrained(
        config["model_dir"],
        subfolder="scheduler",
        torch_dtype=dtype,
    )

    # VAE is only used for inference, keeping weights in full precision is not required.
    vae = AutoencoderKL.from_pretrained(
        config["model_dir"],
        subfolder="vae",
        torch_dtype=dtype,
    )
    # We are not training VAE part of the model.
    vae.requires_grad_(False)

    # Convert unet to bf16 to save GRAM.
    unet = UNet2DConditionModel.from_pretrained(
        config["model_dir"],
        subfolder="unet",
        torch_dtype=dtype,
    )

    if is_xformers_available():
        unet.enable_xformers_memory_efficient_attention()

    if not config["use_lora"]:
        unet_trainable_parameters = unet.parameters()
        text_trainable_parameters = text_encoder.parameters()
    else:
        text_encoder.requires_grad_(False)
        unet.requires_grad_(False)
        unet_trainable_parameters, text_trainable_parameters = add_lora_layers(
            unet, text_encoder
        )

    text_encoder.train()
    unet.train()

    torch.cuda.empty_cache()

    return (
        text_encoder,
        noise_scheduler,
        vae,
        unet,
        unet_trainable_parameters,
        text_trainable_parameters,
    )


def train_fn(config):

    # Load pre-trained models.
    (
        text_encoder,
        noise_scheduler,
        vae,
        unet,
        unet_trainable_parameters,
        text_trainable_parameters,
    ) = load_models(config)

    text_encoder = train.torch.prepare_model(text_encoder)
    unet = train.torch.prepare_model(unet)
    # manually move to device as `prepare_model` can't be used on
    # non-training models.
    vae = vae.to(train.torch.get_device())

    # Use the regular AdamW optimizer to work with bfloat16 weights.
    optimizer = torch.optim.AdamW(
        itertools.chain(unet_trainable_parameters, text_trainable_parameters),
        lr=config["lr"],
    )

    train_dataset = train.get_dataset_shard("train")

    # Train!
    num_train_epochs = config["num_epochs"]

    print(f"Running {num_train_epochs} epochs.")

    global_step = 0
    for _ in range(num_train_epochs):
        if global_step >= config["max_train_steps"]:
            print(f"Stopping training after reaching {global_step} steps...")
            break

        for _, batch in enumerate(
            train_dataset.iter_torch_batches(
                batch_size=config["train_batch_size"],
                device=train.torch.get_device(),
            )
        ):
            batch = collate(batch, torch.bfloat16)

            optimizer.zero_grad()

            # Convert images to latent space
            latents = vae.encode(batch["images"]).latent_dist.sample() * 0.18215

            # Sample noise that we'll add to the latents
            noise = torch.randn_like(latents)
            bsz = latents.shape[0]
            # Sample a random timestep for each image
            timesteps = torch.randint(
                0,
                noise_scheduler.config.num_train_timesteps,
                (bsz,),
                device=latents.device,
            )
            timesteps = timesteps.long()

            # Add noise to the latents according to the noise magnitude at each timestep
            # (this is the forward diffusion process)
            noisy_latents = noise_scheduler.add_noise(latents, noise, timesteps)

            # Get the text embedding for conditioning
            encoder_hidden_states = text_encoder(batch["prompt_ids"])[0]

            # Predict the noise residual.
            model_pred = unet(
                noisy_latents.to(train.torch.get_device()),
                timesteps.to(train.torch.get_device()),
                encoder_hidden_states.to(train.torch.get_device()),
            ).sample
            target = get_target(noise_scheduler, noise, latents, timesteps)

            loss = prior_preserving_loss(
                model_pred, target, config["prior_loss_weight"]
            )
            loss.backward()

            # Gradient clipping before optimizer stepping.
            clip_grad_norm_(
                itertools.chain(unet_trainable_parameters, text_trainable_parameters),
                config["max_grad_norm"],
            )

            optimizer.step()  # Step all optimizers.

            global_step += 1
            results = {
                "step": global_step,
                "loss": loss.detach().item(),
            }
            train.report(results)

            if global_step >= config["max_train_steps"]:
                break
    # END: Training loop

    # Create pipeline using the trained modules and save it.
    if train.get_context().get_world_rank() == 0:
        if not config["use_lora"]:
            pipeline = DiffusionPipeline.from_pretrained(
                config["model_dir"],
                text_encoder=text_encoder.module,
                unet=unet.module,
            )
            pipeline.save_pretrained(config["output_dir"])
        else:
            save_lora_weights(unet.module, text_encoder.module, config["output_dir"])


def unet_attn_processors_state_dict(unet) -> Dict[str, torch.tensor]:
    """
    Returns:
        a state dict containing just the attention processor parameters.
    """
    attn_processors = unet.attn_processors

    attn_processors_state_dict = {}

    for attn_processor_key, attn_processor in attn_processors.items():
        for parameter_key, parameter in attn_processor.state_dict().items():
            param_name = f"{attn_processor_key}.{parameter_key}"
            attn_processors_state_dict[param_name] = parameter
    return attn_processors_state_dict


def save_lora_weights(unet, text_encoder, output_dir):
    unet_lora_layers_to_save = None
    text_encoder_lora_layers_to_save = None

    unet_lora_layers_to_save = unet_attn_processors_state_dict(unet)
    text_encoder_lora_layers_to_save = text_encoder_lora_state_dict(text_encoder)

    LoraLoaderMixin.save_lora_weights(
        output_dir,
        unet_lora_layers=unet_lora_layers_to_save,
        text_encoder_lora_layers=text_encoder_lora_layers_to_save,
    )


if __name__ == "__main__":
    args = train_arguments().parse_args()

    # Build training dataset.
    train_dataset = get_train_dataset(args)

    print(f"Loaded training dataset (size: {train_dataset.count()})")

    # Train with Ray Train TorchTrainer.
    trainer = TorchTrainer(
        train_fn,
        train_loop_config=vars(args),
        scaling_config=ScalingConfig(
            use_gpu=True,
            num_workers=args.num_workers,
        ),
        datasets={
            "train": train_dataset,
        },
    )
    result = trainer.fit()

    print(result)
