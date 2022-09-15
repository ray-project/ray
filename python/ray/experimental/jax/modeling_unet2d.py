import math
from typing import Tuple

import flax.linen as nn
import jax
import jax.numpy as jnp
from flax.core.frozen_dict import FrozenDict
from transformers.modeling_flax_utils import FlaxPreTrainedModel

from .configuration_unet2d import UNet2DConfig


def get_sinusoidal_embeddings(timesteps, embedding_dim):
    half_dim = embedding_dim // 2
    emb = -math.log(10000) * jnp.arange(half_dim, dtype=jnp.float32)
    emb = emb / half_dim
    emb = jnp.exp(emb)

    emb = timesteps[:, None] * emb[None, :]
    emb = jnp.concatenate([jnp.cos(emb), jnp.sin(emb)], -1)
    return emb


##############################################################################################
# Transformer Blocks
##############################################################################################


class Attention(nn.Module):
    query_dim: int
    heads: int = 8
    dim_head: int = 64
    dropout: float = 0.0
    dtype: jnp.dtype = jnp.float32

    def setup(self):
        inner_dim = self.dim_head * self.heads
        self.scale = self.dim_head**-0.5

        self.to_q = nn.Dense(inner_dim, use_bias=False, dtype=self.dtype)
        self.to_k = nn.Dense(inner_dim, use_bias=False, dtype=self.dtype)
        self.to_v = nn.Dense(inner_dim, use_bias=False, dtype=self.dtype)

        self.to_out = nn.Dense(self.query_dim, dtype=self.dtype)

    def reshape_heads_to_batch_dim(self, tensor):
        batch_size, seq_len, dim = tensor.shape
        head_size = self.heads
        tensor = tensor.reshape(batch_size, seq_len, head_size, dim // head_size)
        tensor = jnp.transpose(tensor, (0, 2, 1, 3))
        tensor = tensor.reshape(batch_size * head_size, seq_len, dim // head_size)
        return tensor

    def reshape_batch_dim_to_heads(self, tensor):
        batch_size, seq_len, dim = tensor.shape
        head_size = self.heads
        tensor = tensor.reshape(batch_size // head_size, head_size, seq_len, dim)
        tensor = jnp.transpose(tensor, (0, 2, 1, 3))
        tensor = tensor.reshape(batch_size // head_size, seq_len, dim * head_size)
        return tensor

    def __call__(self, hidden_states, context=None, deterministic=True):
        context = hidden_states if context is None else context

        q = self.to_q(hidden_states)
        k = self.to_k(context)
        v = self.to_v(context)

        q = self.reshape_heads_to_batch_dim(q)
        k = self.reshape_heads_to_batch_dim(k)
        v = self.reshape_heads_to_batch_dim(v)

        # compute attentions
        attn_weights = jnp.einsum("b i d, b j d->b i j", q, k)
        attn_weights = attn_weights * self.scale
        attn_weights = nn.softmax(attn_weights, axis=2)

        ## attend to values
        hidden_states = jnp.einsum("b i j, b j d -> b i d", attn_weights, v)
        hidden_states = self.reshape_batch_dim_to_heads(hidden_states)
        hidden_states = self.to_out(hidden_states)
        return hidden_states


class GluFeedForward(nn.Module):
    dim: int
    dropout: float = 0.0
    dtype: jnp.dtype = jnp.float32

    def setup(self):
        inner_dim = self.dim * 4
        self.dense1 = nn.Dense(inner_dim * 2, dtype=self.dtype)
        self.dense2 = nn.Dense(self.dim, dtype=self.dtype)

    def __call__(self, hidden_states, deterministic=True):
        hidden_states = self.dense1(hidden_states)
        hidden_linear, hidden_gelu = jnp.split(hidden_states, 2, axis=2)
        hidden_states = hidden_linear * nn.gelu(hidden_gelu)
        hidden_states = self.dense2(hidden_states)
        return hidden_states


class TransformerBlock(nn.Module):
    dim: int
    n_heads: int
    d_head: int
    dropout: float = 0.0
    dtype: jnp.dtype = jnp.float32

    def setup(self):
        # self attention
        self.self_attn = Attention(self.dim, self.n_heads, self.d_head, self.dropout, dtype=self.dtype)
        # cross attention
        self.cross_attn = Attention(self.dim, self.n_heads, self.d_head, self.dropout, dtype=self.dtype)
        self.ff = GluFeedForward(dim=self.dim, dropout=self.dropout, dtype=self.dtype)
        self.norm1 = nn.LayerNorm(epsilon=1e-5, dtype=self.dtype)
        self.norm2 = nn.LayerNorm(epsilon=1e-5, dtype=self.dtype)
        self.norm3 = nn.LayerNorm(epsilon=1e-5, dtype=self.dtype)

    def __call__(self, hidden_states, context, deterministic=True):
        # self attention
        residual = hidden_states
        hidden_states = self.self_attn(self.norm1(hidden_states))
        hidden_states = hidden_states + residual

        # cross attention
        residual = hidden_states
        hidden_states = self.cross_attn(self.norm2(hidden_states), context)
        hidden_states = hidden_states + residual

        # feed forward
        residual = hidden_states
        hidden_states = self.ff(self.norm3(hidden_states))
        hidden_states = hidden_states + residual

        return hidden_states


class SpatialTransformer(nn.Module):
    in_channels: int
    n_heads: int
    d_head: int
    depth: int = 1
    dropout: float = 0.0
    dtype: jnp.dtype = jnp.float32

    def setup(self):
        self.norm = nn.GroupNorm(num_groups=32, epsilon=1e-5)

        inner_dim = self.n_heads * self.d_head
        self.proj_in = nn.Conv(
            inner_dim,
            kernel_size=(1, 1),
            strides=(1, 1),
            padding="VALID",
            dtype=self.dtype,
        )

        self.transformer_blocks = [
            TransformerBlock(inner_dim, self.n_heads, self.d_head, dropout=self.dropout, dtype=self.dtype)
            for _ in range(self.depth)
        ]

        self.proj_out = nn.Conv(
            inner_dim,
            kernel_size=(1, 1),
            strides=(1, 1),
            padding="VALID",
            dtype=self.dtype,
        )

    def __call__(self, hidden_states, context, deterministic=True):
        batch, height, width, channels = hidden_states.shape
        # import ipdb; ipdb.set_trace()
        residual = hidden_states
        hidden_states = self.norm(hidden_states)
        hidden_states = self.proj_in(hidden_states)

        # hidden_states = jnp.transpose(hidden_states, (0, 2, 3, 1))
        hidden_states = hidden_states.reshape(batch, height * width, channels)

        for transformer_block in self.transformer_blocks:
            hidden_states = transformer_block(hidden_states, context)

        hidden_states = hidden_states.reshape(batch, height, width, channels)
        # hidden_states = jnp.transpose(hidden_states, (0, 3, 1, 2))

        hidden_states = self.proj_out(hidden_states)
        hidden_states = hidden_states + residual

        return hidden_states


##############################################################################################
# UNet Blocks
##############################################################################################


class Timesteps(nn.Module):
    dim: int = 32

    @nn.compact
    def __call__(self, timesteps):
        return get_sinusoidal_embeddings(timesteps, self.dim)


class TimestepEmbedding(nn.Module):
    time_embed_dim: int = 32
    dtype: jnp.dtype = jnp.float32

    @nn.compact
    def __call__(self, temb):
        temb = nn.Dense(self.time_embed_dim, dtype=self.dtype, name="linear_1")(temb)
        temb = nn.silu(temb)
        temb = nn.Dense(self.time_embed_dim, dtype=self.dtype, name="linear_2")(temb)
        return temb


class Upsample(nn.Module):
    out_channels: int
    dtype: jnp.dtype = jnp.float32

    def setup(self):
        self.conv = nn.Conv(
            self.out_channels,
            kernel_size=(3, 3),
            strides=(1, 1),
            padding=((1, 1), (1, 1)),
            dtype=self.dtype,
        )

    def __call__(self, hidden_states):
        batch, height, width, channels = hidden_states.shape
        hidden_states = jax.image.resize(
            hidden_states,
            shape=(batch, height * 2, width * 2, channels),
            method="nearest",
        )
        hidden_states = self.conv(hidden_states)
        return hidden_states


class Downsample(nn.Module):
    out_channels: int
    dtype: jnp.dtype = jnp.float32

    def setup(self):
        self.conv = nn.Conv(
            self.out_channels,
            kernel_size=(3, 3),
            strides=(2, 2),
            padding=((1, 1), (1, 1)),  # padding="VALID",
            dtype=self.dtype,
        )

    def __call__(self, hidden_states):
        # pad = ((0, 0), (0, 1), (0, 1), (0, 0))  # pad height and width dim
        # hidden_states = jnp.pad(hidden_states, pad_width=pad)
        hidden_states = self.conv(hidden_states)
        return hidden_states


class ResnetBlock(nn.Module):
    in_channels: int
    out_channels: int = None
    dropout_prob: float = 0.0
    use_nin_shortcut: bool = None
    dtype: jnp.dtype = jnp.float32

    def setup(self):
        out_channels = self.in_channels if self.out_channels is None else self.out_channels

        self.norm1 = nn.GroupNorm(num_groups=32, epsilon=1e-5)
        self.conv1 = nn.Conv(
            out_channels,
            kernel_size=(3, 3),
            strides=(1, 1),
            padding=((1, 1), (1, 1)),
            dtype=self.dtype,
        )

        self.time_emb_proj = nn.Dense(out_channels, dtype=self.dtype)

        self.norm2 = nn.GroupNorm(num_groups=32, epsilon=1e-5)
        self.dropout = nn.Dropout(self.dropout_prob)
        self.conv2 = nn.Conv(
            out_channels,
            kernel_size=(3, 3),
            strides=(1, 1),
            padding=((1, 1), (1, 1)),
            dtype=self.dtype,
        )

        use_nin_shortcut = self.in_channels != out_channels if self.use_nin_shortcut is None else self.use_nin_shortcut

        self.conv_shortcut = None
        if use_nin_shortcut:
            self.conv_shortcut = nn.Conv(
                out_channels,
                kernel_size=(1, 1),
                strides=(1, 1),
                padding="VALID",
                dtype=self.dtype,
            )

    def __call__(self, hidden_states, temb, deterministic=True):
        residual = hidden_states
        hidden_states = self.norm1(hidden_states)
        hidden_states = nn.swish(hidden_states)
        hidden_states = self.conv1(hidden_states)

        temb = self.time_emb_proj(nn.swish(temb))
        temb = jnp.expand_dims(jnp.expand_dims(temb, 1), 1)
        hidden_states = hidden_states + temb

        hidden_states = self.norm2(hidden_states)
        hidden_states = nn.swish(hidden_states)
        hidden_states = self.dropout(hidden_states, deterministic)
        hidden_states = self.conv2(hidden_states)

        if self.conv_shortcut is not None:
            residual = self.conv_shortcut(residual)

        return hidden_states + residual


class CrossAttnDownBlock2D(nn.Module):
    in_channels: int
    out_channels: int
    dropout: float = 0.0
    num_layers: int = 1
    attn_num_head_channels: int = 1
    add_downsample: bool = True
    dtype: jnp.dtype = jnp.float32

    def setup(self):
        resnets = []
        attentions = []

        for i in range(self.num_layers):
            in_channels = self.in_channels if i == 0 else self.out_channels

            res_block = ResnetBlock(
                in_channels=in_channels,
                out_channels=self.out_channels,
                dropout_prob=self.dropout,
                dtype=self.dtype,
            )
            resnets.append(res_block)

            attn_block = SpatialTransformer(
                in_channels=self.out_channels,
                n_heads=self.attn_num_head_channels,
                d_head=self.out_channels // self.attn_num_head_channels,
                depth=1,
                dtype=self.dtype,
            )
            attentions.append(attn_block)

        self.resnets = resnets
        self.attentions = attentions

        if self.add_downsample:
            self.downsample = Downsample(self.out_channels, dtype=self.dtype)

    def __call__(self, hidden_states, temb, encoder_hidden_states, deterministic=True):
        output_states = ()

        for resnet, attn in zip(self.resnets, self.attentions):
            hidden_states = resnet(hidden_states, temb)
            hidden_states = attn(hidden_states, encoder_hidden_states)
            output_states += (hidden_states,)

        if self.add_downsample:
            hidden_states = self.downsample(hidden_states)
            output_states += (hidden_states,)

        return hidden_states, output_states


class DownBlock2D(nn.Module):
    in_channels: int
    out_channels: int
    dropout: float = 0.0
    num_layers: int = 1
    add_downsample: bool = True
    dtype: jnp.dtype = jnp.float32

    def setup(self):
        resnets = []

        for i in range(self.num_layers):
            in_channels = self.in_channels if i == 0 else self.out_channels

            res_block = ResnetBlock(
                in_channels=in_channels,
                out_channels=self.out_channels,
                dropout_prob=self.dropout,
                dtype=self.dtype,
            )
            resnets.append(res_block)
        self.resnets = resnets

        if self.add_downsample:
            self.downsample = Downsample(self.out_channels, dtype=self.dtype)

    def __call__(self, hidden_states, temb, deterministic=True):
        output_states = ()

        for resnet in self.resnets:
            hidden_states = resnet(hidden_states, temb)
            output_states += (hidden_states,)

        if self.add_downsample:
            hidden_states = self.downsample(hidden_states)
            output_states += (hidden_states,)

        return hidden_states, output_states


class CrossAttnUpBlock2D(nn.Module):
    in_channels: int
    out_channels: int
    prev_output_channel: int
    dropout: float = 0.0
    num_layers: int = 1
    attn_num_head_channels: int = 1
    add_upsample: bool = True
    dtype: jnp.dtype = jnp.float32

    def setup(self):
        resnets = []
        attentions = []

        for i in range(self.num_layers):
            res_skip_channels = self.in_channels if (i == self.num_layers - 1) else self.out_channels
            resnet_in_channels = self.prev_output_channel if i == 0 else self.out_channels

            res_block = ResnetBlock(
                in_channels=resnet_in_channels + res_skip_channels,
                out_channels=self.out_channels,
                dropout_prob=self.dropout,
                dtype=self.dtype,
            )
            resnets.append(res_block)

            attn_block = SpatialTransformer(
                in_channels=self.out_channels,
                n_heads=self.attn_num_head_channels,
                d_head=self.out_channels // self.attn_num_head_channels,
                depth=1,
                dtype=self.dtype,
            )
            attentions.append(attn_block)

        self.resnets = resnets
        self.attentions = attentions

        if self.add_upsample:
            self.upsample = Upsample(self.out_channels, dtype=self.dtype)

    def __call__(self, hidden_states, res_hidden_states_tuple, temb, encoder_hidden_states, deterministic=True):

        for resnet, attn in zip(self.resnets, self.attentions):
            # pop res hidden states
            res_hidden_states = res_hidden_states_tuple[-1]
            res_hidden_states_tuple = res_hidden_states_tuple[:-1]
            hidden_states = jnp.concatenate((hidden_states, res_hidden_states), axis=-1)

            hidden_states = resnet(hidden_states, temb)
            hidden_states = attn(hidden_states, encoder_hidden_states)

        if self.add_upsample:
            hidden_states = self.upsample(hidden_states)

        return hidden_states


class UpBlock2D(nn.Module):
    in_channels: int
    out_channels: int
    prev_output_channel: int
    dropout: float = 0.0
    num_layers: int = 1
    add_upsample: bool = True
    dtype: jnp.dtype = jnp.float32

    def setup(self):
        resnets = []

        for i in range(self.num_layers):
            res_skip_channels = self.in_channels if (i == self.num_layers - 1) else self.out_channels
            resnet_in_channels = self.prev_output_channel if i == 0 else self.out_channels

            res_block = ResnetBlock(
                in_channels=resnet_in_channels + res_skip_channels,
                out_channels=self.out_channels,
                dropout_prob=self.dropout,
                dtype=self.dtype,
            )
            resnets.append(res_block)

        self.resnets = resnets

        if self.add_upsample:
            self.upsample = Upsample(self.out_channels, dtype=self.dtype)

    def __call__(self, hidden_states, res_hidden_states_tuple, temb, deterministic=True):
        for resnet in self.resnets:
            # pop res hidden states
            res_hidden_states = res_hidden_states_tuple[-1]
            res_hidden_states_tuple = res_hidden_states_tuple[:-1]
            hidden_states = jnp.concatenate((hidden_states, res_hidden_states), axis=-1)

            hidden_states = resnet(hidden_states, temb)

        if self.add_upsample:
            hidden_states = self.upsample(hidden_states)

        return hidden_states


class UNetMidBlock2DCrossAttn(nn.Module):
    in_channels: int
    dropout: float = 0.0
    num_layers: int = 1
    attn_num_head_channels: int = 1
    dtype: jnp.dtype = jnp.float32

    def setup(self):
        # there is always at least one resnet
        resnets = [
            ResnetBlock(
                in_channels=self.in_channels,
                out_channels=self.in_channels,
                dropout_prob=self.dropout,
                dtype=self.dtype,
            )
        ]

        attentions = []

        for _ in range(self.num_layers):
            attn_block = SpatialTransformer(
                in_channels=self.in_channels,
                n_heads=self.attn_num_head_channels,
                d_head=self.in_channels // self.attn_num_head_channels,
                depth=1,
                dtype=self.dtype,
            )
            attentions.append(attn_block)

            res_block = ResnetBlock(
                in_channels=self.in_channels,
                out_channels=self.in_channels,
                dropout_prob=self.dropout,
                dtype=self.dtype,
            )
            resnets.append(res_block)

        self.resnets = resnets
        self.attentions = attentions

    def __call__(self, hidden_states, temb, encoder_hidden_states, deterministic=True):
        hidden_states = self.resnets[0](hidden_states, temb)
        for attn, resnet in zip(self.attentions, self.resnets[1:]):
            hidden_states = attn(hidden_states, encoder_hidden_states)
            hidden_states = resnet(hidden_states, temb)

        return hidden_states


class UNet2DModule(nn.Module):
    config: UNet2DConfig
    dtype: jnp.dtype = jnp.float32

    def setup(self):
        config = self.config

        self.sample_size = config.sample_size
        block_out_channels = config.block_out_channels
        time_embed_dim = block_out_channels[0] * 4

        # input
        self.conv_in = nn.Conv(
            block_out_channels[0],
            kernel_size=(3, 3),
            strides=(1, 1),
            padding=((1, 1), (1, 1)),
            dtype=self.dtype,
        )

        # time
        self.time_proj = Timesteps(block_out_channels[0])
        self.time_embedding = TimestepEmbedding(time_embed_dim, dtype=self.dtype)

        # down
        down_blocks = []
        output_channel = block_out_channels[0]
        for i, down_block_type in enumerate(config.down_block_types):
            input_channel = output_channel
            output_channel = block_out_channels[i]
            is_final_block = i == len(block_out_channels) - 1

            if down_block_type == "CrossAttnDownBlock2D":
                down_block = CrossAttnDownBlock2D(
                    in_channels=input_channel,
                    out_channels=output_channel,
                    dropout=config.dropout,
                    num_layers=config.layers_per_block,
                    attn_num_head_channels=config.attention_head_dim,
                    add_downsample=not is_final_block,
                    dtype=self.dtype,
                )
            else:
                down_block = DownBlock2D(
                    in_channels=input_channel,
                    out_channels=output_channel,
                    dropout=config.dropout,
                    num_layers=config.layers_per_block,
                    add_downsample=not is_final_block,
                    dtype=self.dtype,
                )

            down_blocks.append(down_block)
        self.down_blocks = down_blocks

        # mid
        self.mid_block = UNetMidBlock2DCrossAttn(
            in_channels=block_out_channels[-1],
            dropout=config.dropout,
            attn_num_head_channels=config.attention_head_dim,
            dtype=self.dtype,
        )

        # up
        up_blocks = []
        reversed_block_out_channels = list(reversed(block_out_channels))
        output_channel = reversed_block_out_channels[0]
        for i, up_block_type in enumerate(config.up_block_types):
            prev_output_channel = output_channel
            output_channel = reversed_block_out_channels[i]
            input_channel = reversed_block_out_channels[min(i + 1, len(block_out_channels) - 1)]

            is_final_block = i == len(block_out_channels) - 1

            if up_block_type == "CrossAttnUpBlock2D":
                up_block = CrossAttnUpBlock2D(
                    in_channels=input_channel,
                    out_channels=output_channel,
                    prev_output_channel=prev_output_channel,
                    num_layers=config.layers_per_block + 1,
                    attn_num_head_channels=config.attention_head_dim,
                    add_upsample=not is_final_block,
                    dropout=config.dropout,
                    dtype=self.dtype,
                )
            else:
                up_block = UpBlock2D(
                    in_channels=input_channel,
                    out_channels=output_channel,
                    prev_output_channel=prev_output_channel,
                    num_layers=config.layers_per_block + 1,
                    add_upsample=not is_final_block,
                    dropout=config.dropout,
                    dtype=self.dtype,
                )

            up_blocks.append(up_block)
            prev_output_channel = output_channel
        self.up_blocks = up_blocks

        # out
        self.conv_norm_out = nn.GroupNorm(num_groups=32, epsilon=1e-5)
        self.conv_out = nn.Conv(
            config.out_channels,
            kernel_size=(3, 3),
            strides=(1, 1),
            padding=((1, 1), (1, 1)),
            dtype=self.dtype,
        )

    def __call__(self, sample, timesteps, encoder_hidden_states, deterministic=True):

        # 1. time
        # broadcast to batch dimension
        # timesteps = jnp.broadcast_to(timesteps, (sample.shape[0],) + timesteps.shape)
        t_emb = self.time_proj(timesteps)
        t_emb = self.time_embedding(t_emb)

        # 2. pre-process
        sample = self.conv_in(sample)

        # 3. down
        down_block_res_samples = (sample,)
        for down_block in self.down_blocks:
            if isinstance(down_block, CrossAttnDownBlock2D):
                sample, res_samples = down_block(sample, t_emb, encoder_hidden_states)
            else:
                sample, res_samples = down_block(sample, t_emb)
            down_block_res_samples += res_samples

        # 4. mid
        sample = self.mid_block(sample, t_emb, encoder_hidden_states)

        # 5. up
        for up_block in self.up_blocks:
            res_samples = down_block_res_samples[-(self.config.layers_per_block + 1) :]
            down_block_res_samples = down_block_res_samples[: -(self.config.layers_per_block + 1)]
            if isinstance(up_block, CrossAttnUpBlock2D):
                sample = up_block(
                    sample,
                    temb=t_emb,
                    encoder_hidden_states=encoder_hidden_states,
                    res_hidden_states_tuple=res_samples,
                )
            else:
                sample = up_block(sample, temb=t_emb, res_hidden_states_tuple=res_samples)

        # 6. post-process
        sample = self.conv_norm_out(sample)
        sample = nn.silu(sample)
        sample = self.conv_out(sample)

        return sample


class UNet2DPretrainedModel(FlaxPreTrainedModel):
    config_class = UNet2DConfig
    base_model_prefix = "model"
    module_class: nn.Module = None

    def __init__(
        self,
        config: UNet2DConfig,
        input_shape: Tuple = (1, 32, 32, 4),
        seed: int = 0,
        dtype: jnp.dtype = jnp.float32,
        _do_init: bool = True,
        **kwargs,
    ):
        module = self.module_class(config=config, dtype=dtype, **kwargs)
        super().__init__(config, module, input_shape=input_shape, seed=seed, dtype=dtype, _do_init=_do_init)

    def init_weights(self, rng: jax.random.PRNGKey, input_shape: Tuple) -> FrozenDict:
        # init input tensors
        sample_shape = (1, self.config.sample_size, self.config.sample_size, self.config.in_channels)
        sample = jnp.zeros(sample_shape, dtype=jnp.float32)
        timestpes = jnp.ones((1,), dtype=jnp.int32)
        encoder_hidden_states = jnp.zeros((1, 1, self.config.cross_attention_dim), dtype=jnp.float32)

        params_rng, dropout_rng = jax.random.split(rng)
        rngs = {"params": params_rng, "dropout": dropout_rng}

        return self.module.init(rngs, sample, timestpes, encoder_hidden_states)["params"]

    def __call__(
        self,
        sample,
        timesteps,
        encoder_hidden_states,
        params: dict = None,
        dropout_rng: jax.random.PRNGKey = None,
        train: bool = False,
    ):
        # Handle any PRNG if needed
        rngs = {"dropout": dropout_rng} if dropout_rng is not None else {}

        return self.module.apply(
            {"params": params or self.params},
            jnp.array(sample),
            jnp.array(timesteps, dtype=jnp.int32),
            encoder_hidden_states,
            not train,
            rngs=rngs,
        )


class UNet2D(UNet2DPretrainedModel):
    module_class = UNet2DModule
