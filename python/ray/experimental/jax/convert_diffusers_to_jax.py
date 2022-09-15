import argparse
import re

import jax.numpy as jnp
from diffusers import AutoencoderKL as Autoencoder
from diffusers import UNet2DConditionModel
from flax.traverse_util import flatten_dict, unflatten_dict

from . import AutoencoderKL, UNet2D, UNet2DConfig, VAEConfig

regex = r"\w+[.]\d+"


def rename_key(key):
    key = key.replace("downsamplers.0", "downsample")
    key = key.replace("upsamplers.0", "upsample")
    key = key.replace("net.0.proj", "dense1")
    key = key.replace("net.2", "dense2")
    key = key.replace("to_out.0", "to_out")
    key = key.replace("attn1", "self_attn")
    key = key.replace("attn2", "cross_attn")

    pats = re.findall(regex, key)
    for pat in pats:
        key = key.replace(pat, "_".join(pat.split(".")))
    return key


# Adapted from https://github.com/huggingface/transformers/blob/ff5cdc086be1e0c3e2bbad8e3469b34cffb55a85/src/transformers/modeling_flax_pytorch_utils.py#L61
def convert_pytorch_state_dict_to_flax(pt_state_dict, flax_model):
    # convert pytorch tensor to numpy
    pt_state_dict = {k: v.numpy() for k, v in pt_state_dict.items()}

    random_flax_state_dict = flatten_dict(flax_model.params_shape_tree)
    flax_state_dict = {}

    remove_base_model_prefix = (flax_model.base_model_prefix not in flax_model.params_shape_tree) and (
        flax_model.base_model_prefix in set([k.split(".")[0] for k in pt_state_dict.keys()])
    )
    add_base_model_prefix = (flax_model.base_model_prefix in flax_model.params_shape_tree) and (
        flax_model.base_model_prefix not in set([k.split(".")[0] for k in pt_state_dict.keys()])
    )

    # Need to change some parameters name to match Flax names so that we don't have to fork any layer
    for pt_key, pt_tensor in pt_state_dict.items():
        pt_tuple_key = tuple(pt_key.split("."))

        has_base_model_prefix = pt_tuple_key[0] == flax_model.base_model_prefix
        require_base_model_prefix = (flax_model.base_model_prefix,) + pt_tuple_key in random_flax_state_dict

        if remove_base_model_prefix and has_base_model_prefix:
            pt_tuple_key = pt_tuple_key[1:]
        elif add_base_model_prefix and require_base_model_prefix:
            pt_tuple_key = (flax_model.base_model_prefix,) + pt_tuple_key

        # Correctly rename weight parameters
        if (
            "norm" in pt_key
            and (pt_tuple_key[-1] == "bias")
            and (pt_tuple_key[:-1] + ("bias",) not in random_flax_state_dict)
            and (pt_tuple_key[:-1] + ("scale",) in random_flax_state_dict)
        ):
            pt_tuple_key = pt_tuple_key[:-1] + ("scale",)
        elif pt_tuple_key[-1] in ["weight", "gamma"] and pt_tuple_key[:-1] + ("scale",) in random_flax_state_dict:
            pt_tuple_key = pt_tuple_key[:-1] + ("scale",)
        if pt_tuple_key[-1] == "weight" and pt_tuple_key[:-1] + ("embedding",) in random_flax_state_dict:
            pt_tuple_key = pt_tuple_key[:-1] + ("embedding",)
        elif pt_tuple_key[-1] == "weight" and pt_tensor.ndim == 4 and pt_tuple_key not in random_flax_state_dict:
            # conv layer
            pt_tuple_key = pt_tuple_key[:-1] + ("kernel",)
            pt_tensor = pt_tensor.transpose(2, 3, 1, 0)
        elif pt_tuple_key[-1] == "weight" and pt_tuple_key not in random_flax_state_dict:
            # linear layer
            pt_tuple_key = pt_tuple_key[:-1] + ("kernel",)
            pt_tensor = pt_tensor.T
        elif pt_tuple_key[-1] == "gamma":
            pt_tuple_key = pt_tuple_key[:-1] + ("weight",)
        elif pt_tuple_key[-1] == "beta":
            pt_tuple_key = pt_tuple_key[:-1] + ("bias",)

        if pt_tuple_key in random_flax_state_dict:
            if pt_tensor.shape != random_flax_state_dict[pt_tuple_key].shape:
                raise ValueError(
                    f"PyTorch checkpoint seems to be incorrect. Weight {pt_key} was expected to be of shape "
                    f"{random_flax_state_dict[pt_tuple_key].shape}, but is {pt_tensor.shape}."
                )

        # also add unexpected weight so that warning is thrown
        flax_state_dict[pt_tuple_key] = jnp.asarray(pt_tensor)

    return unflatten_dict(flax_state_dict)


def convert_params(pt_model, fx_model):
    state_dict = pt_model.state_dict()
    keys = list(state_dict.keys())
    for key in keys:
        renamed_key = rename_key(key)
        state_dict[renamed_key] = state_dict.pop(key)

    fx_params = convert_pytorch_state_dict_to_flax(state_dict, fx_model)
    return fx_params


def convert_diffusers_to_jax(pt_model_path, save_path):
    unet_pt = UNet2DConditionModel.from_pretrained(pt_model_path, subfolder="unet", use_auth_token=True)

    # create UNet flax config and model
    config = UNet2DConfig(
        sample_size=unet_pt.config.sample_size,
        in_channels=unet_pt.config.in_channels,
        out_channels=unet_pt.config.out_channels,
        down_block_types=unet_pt.config.down_block_types,
        up_block_types=unet_pt.config.up_block_types,
        block_out_channels=unet_pt.config.block_out_channels,
        layers_per_block=unet_pt.config.layers_per_block,
        attention_head_dim=unet_pt.config.attention_head_dim,
        cross_attention_dim=unet_pt.config.cross_attention_dim,
    )
    unet_fx = UNet2D(config, _do_init=False)

    # convert unet pt params to jax
    params = convert_params(unet_pt, unet_fx)
    # save unet
    unet_fx.save_pretrained(f"{save_path}/unet", params=params)

    vae_pt = Autoencoder.from_pretrained(pt_model_path, subfolder="vae", use_auth_token=True)

    # create AutoEncoder flax config and model
    config = VAEConfig(
        sample_size=vae_pt.config.sample_size,
        in_channels=vae_pt.config.in_channels,
        out_channels=vae_pt.config.out_channels,
        down_block_types=vae_pt.config.down_block_types,
        up_block_types=vae_pt.config.up_block_types,
        block_out_channels=vae_pt.config.block_out_channels,
        layers_per_block=vae_pt.config.layers_per_block,
        latent_channels=vae_pt.config.latent_channels,
    )
    vae_fx = AutoencoderKL(config, _do_init=False)

    # convert vae pt params to jax
    params = convert_params(vae_pt, vae_fx)
    # save vae
    vae_fx.save_pretrained(f"{save_path}/vae", params=params)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pt_model_path", type=str, required=True)
    parser.add_argument("--save_path", type=str, required=True)
    args = parser.parse_args()

    convert_diffusers_to_jax(args.pt_model_path, args.save_path)
