# LICENSE: MIT
"""
Adapted from https://github.com/karpathy/minGPT

Full definition of a GPT Language Model, all of it in this single file.
References:
1) the official GPT-2 TensorFlow implementation released by OpenAI:
https://github.com/openai/gpt-2/blob/master/src/model.py
2) huggingface/transformers PyTorch implementation:
https://github.com/huggingface/transformers/blob/main/src/transformers
        /models/gpt2/modeling_gpt2.py
"""

import math
from dataclasses import dataclass
from typing import Tuple

import torch
import torch.nn as nn
from torch.nn import functional as F

from ray._common.deprecation import Deprecated
from ray.rllib.utils.annotations import DeveloperAPI


@DeveloperAPI
@dataclass
class GPTConfig:
    # block size must be provided
    block_size: int

    # transformer config
    n_layer: int = 12
    n_head: int = 12
    n_embed: int = 768

    # dropout config
    embed_pdrop: float = 0.1
    resid_pdrop: float = 0.1
    attn_pdrop: float = 0.1


@Deprecated(error=False)
class NewGELU(nn.Module):
    """
    Implementation of the GELU activation function currently in Google BERT
    repo (identical to OpenAI GPT).
    Reference: Gaussian Error Linear Units (GELU) paper:
    https://arxiv.org/abs/1606.08415
    """

    def forward(self, x):
        return (
            0.5
            * x
            * (
                1.0
                + torch.tanh(
                    math.sqrt(2.0 / math.pi) * (x + 0.044715 * torch.pow(x, 3.0))
                )
            )
        )


@Deprecated(error=False)
class CausalSelfAttention(nn.Module):
    """
    Vanilla multi-head masked self-attention layer with a projection at the end.
    It is possible to use torch.nn.MultiheadAttention here but I am including an
    explicit implementation here to show that there is nothing too scary here.
    """

    def __init__(self, config: GPTConfig):
        super().__init__()
        assert config.n_embed % config.n_head == 0
        # key, query, value projections for all heads, but in a batch
        self.c_attn = nn.Linear(config.n_embed, 3 * config.n_embed)
        # output projection
        self.c_proj = nn.Linear(config.n_embed, config.n_embed)
        # regularization
        self.attn_dropout = nn.Dropout(config.attn_pdrop)
        self.resid_dropout = nn.Dropout(config.resid_pdrop)
        # causal mask to ensure that attention is only applied to the left
        # in the input sequence
        self.register_buffer(
            "bias",
            torch.tril(torch.ones(config.block_size, config.block_size)).view(
                1, 1, config.block_size, config.block_size
            ),
        )
        self.n_head = config.n_head
        self.n_embed = config.n_embed

    def forward(self, x, attention_masks=None):
        # batch size, sequence length, embedding dimensionality (n_embed)
        B, T, C = x.size()

        # calculate query, key, values for all heads in batch and move head
        # forward to be the batch dim
        q, k, v = self.c_attn(x).split(self.n_embed, dim=2)
        # (B, nh, T, hs)
        k = k.view(B, T, self.n_head, C // self.n_head).transpose(1, 2)
        # (B, nh, T, hs)
        q = q.view(B, T, self.n_head, C // self.n_head).transpose(1, 2)
        # (B, nh, T, hs)
        v = v.view(B, T, self.n_head, C // self.n_head).transpose(1, 2)

        # causal self-attention; Self-attend:
        # (B, nh, T, hs) x (B, nh, hs, T) -> (B, nh, T, T)
        att = (q @ k.transpose(-2, -1)) * (1.0 / math.sqrt(k.size(-1)))
        att = att.masked_fill(self.bias[:, :, :T, :T] == 0, float("-inf"))
        if attention_masks is not None:
            att = att + attention_masks
        att = F.softmax(att, dim=-1)
        att = self.attn_dropout(att)
        y = att @ v  # (B, nh, T, T) x (B, nh, T, hs) -> (B, nh, T, hs)
        # re-assemble all head outputs side by side
        y = y.transpose(1, 2).contiguous().view(B, T, C)

        # output projection
        y = self.resid_dropout(self.c_proj(y))
        return y, att


@Deprecated(error=False)
class Block(nn.Module):
    """an unassuming Transformer block"""

    def __init__(self, config: GPTConfig):
        super().__init__()
        self.ln_1 = nn.LayerNorm(config.n_embed)
        self.attn = CausalSelfAttention(config)
        self.ln_2 = nn.LayerNorm(config.n_embed)
        self.mlp = nn.ModuleDict(
            dict(
                c_fc=nn.Linear(config.n_embed, 4 * config.n_embed),
                c_proj=nn.Linear(4 * config.n_embed, config.n_embed),
                act=NewGELU(),
                dropout=nn.Dropout(config.resid_pdrop),
            )
        )

    def forward(self, x, attention_masks=None):
        # Multi-head attention sub-layer.
        x_att, att = self.attn(self.ln_1(x), attention_masks=attention_masks)
        # Residual of multi-head attention sub-layer.
        x = x + x_att

        # Position-wise FFN sub-layer: fc + activation + fc + dropout
        x_ffn = self.mlp.dropout(self.mlp.c_proj(self.mlp.act(self.mlp.c_fc(x))))
        # Residual of position-wise FFN sub-layer.
        x = x + x_ffn
        return x, att


@Deprecated(error=False)
def configure_gpt_optimizer(
    model: nn.Module,
    learning_rate: float,
    weight_decay: float,
    betas: Tuple[float, float] = (0.9, 0.95),
    **kwargs,
) -> torch.optim.Optimizer:
    """
    This long function is unfortunately doing something very simple and is
    being very defensive: We are separating out all parameters of the model
    into two buckets: those that will experience weight decay for regularization
    and those that won't (biases, and layernorm/embedding weights). We are then
    returning the PyTorch optimizer object.
    """

    # separate out all parameters to those that will and won't experience
    # regularizing weight decay
    decay = set()
    no_decay = set()
    whitelist_w_modules = (torch.nn.Linear,)
    blacklist_w_modules = (torch.nn.LayerNorm, torch.nn.Embedding)
    for mn, m in model.named_modules():
        for pn, p in m.named_parameters():
            fpn = "%s.%s" % (mn, pn) if mn else pn  # full param name
            # random note: because named_modules and named_parameters are
            # recursive we will see the same tensors p many many times. but
            # doing it this way allows us to know which parent module any
            # tensor p belongs to...
            if pn.endswith("bias"):
                # all biases will not be decayed
                no_decay.add(fpn)
            elif pn.endswith("weight") and isinstance(m, whitelist_w_modules):
                # weights of whitelist modules will be weight decayed
                decay.add(fpn)
            elif pn.endswith("weight") and isinstance(m, blacklist_w_modules):
                # weights of blacklist modules will NOT be weight decayed
                no_decay.add(fpn)

    # validate that we considered every parameter
    param_dict = dict(model.named_parameters())
    inter_params = decay & no_decay
    union_params = decay | no_decay
    assert (
        len(inter_params) == 0
    ), f"parameters {str(inter_params)} made it into both decay/no_decay sets!"
    assert len(param_dict.keys() - union_params) == 0, (
        f"parameters {str(param_dict.keys() - union_params)} were not "
        f"separated into either decay/no_decay set!"
    )

    # create the pytorch optimizer object
    optim_groups = [
        {
            "params": [param_dict[pn] for pn in sorted(decay)],
            "weight_decay": weight_decay,
        },
        {
            "params": [param_dict[pn] for pn in sorted(no_decay)],
            "weight_decay": 0.0,
        },
    ]
    optimizer = torch.optim.AdamW(optim_groups, lr=learning_rate, betas=betas, **kwargs)
    return optimizer


@Deprecated(error=False)
class GPT(nn.Module):
    """GPT Transformer Model"""

    def __init__(self, config: GPTConfig):
        super().__init__()
        assert config.block_size is not None
        self.block_size = config.block_size

        self.transformer = nn.ModuleDict(
            dict(
                drop=nn.Dropout(config.embed_pdrop),
                h=nn.ModuleList([Block(config) for _ in range(config.n_layer)]),
                ln_f=nn.LayerNorm(config.n_embed),
            )
        )

        # init all weights, and apply a special scaled init to the residual
        # projections, per GPT-2 paper
        self.apply(self._init_weights)
        for pn, p in self.named_parameters():
            if pn.endswith("c_proj.weight"):
                torch.nn.init.normal_(
                    p, mean=0.0, std=0.02 / math.sqrt(2 * config.n_layer)
                )

    def _init_weights(self, module):
        if isinstance(module, nn.Linear):
            torch.nn.init.normal_(module.weight, mean=0.0, std=0.02)
            if module.bias is not None:
                torch.nn.init.zeros_(module.bias)
        elif isinstance(module, nn.Embedding):
            torch.nn.init.normal_(module.weight, mean=0.0, std=0.02)
        elif isinstance(module, nn.LayerNorm):
            torch.nn.init.zeros_(module.bias)
            torch.nn.init.ones_(module.weight)

    def forward(self, input_embeds, attention_masks=None, return_attentions=False):
        """
        input_embeds: [batch_size x seq_len x n_embed]
        attention_masks: [batch_size x seq_len], 0 don't attend, 1 attend
        """
        B, T, C = input_embeds.size()
        assert T <= self.block_size, (
            f"Cannot forward sequence of length {T}, "
            f"block size is only {self.block_size}"
        )

        if attention_masks is not None:
            _B, _T = attention_masks.size()
            assert _B == B and _T == T
            # We create a 3D attention mask from a 2D tensor mask.
            # Sizes are [batch_size, 1, 1, to_seq_len]
            # So we can broadcast to
            # [batch_size, num_heads, from_seq_length, to_seq_length]
            # this attention mask is more simple than the triangular
            # masking of causal attention used in OpenAI GPT, we just need
            # to prepare the broadcast dimension here.
            attention_masks = attention_masks[:, None, None, :]

            # Since attention_mask is 1.0 for positions we want to attend
            # and 0.0 for masked positions, this operation will create a
            # tensor which is 0.0 for positions we want to attend and -inf
            # for masked positions. Since we are adding it to the raw scores
            # before the softmax, this is effectively the same as removing
            # these entirely.
            attention_masks = attention_masks.to(dtype=input_embeds.dtype)
            attention_masks = (1.0 - attention_masks) * -1e9

        # forward the GPT model itself
        x = self.transformer.drop(input_embeds)

        atts = []
        for block in self.transformer.h:
            x, att = block(x, attention_masks=attention_masks)
            atts.append(att)
        x = self.transformer.ln_f(x)

        if return_attentions:
            return x, atts
        else:
            return x
