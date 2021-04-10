from typing import Callable, Tuple

import flax
from flax import optim
import flax.linen as nn

import jax.numpy as jnp
from jax.random import PRNGKey

from functools import partial 

from transformers.models.bert.modeling_flax_bert import FlaxBertPreTrainedModel, FlaxBertModule, BERT_INPUTS_DOCSTRING
from transformers.models.bert.modeling_flax_bert import FlaxBertOnlyMLMHead, FlaxBertOnlyNSPHead
from transformers.models.bert.configuration_bert import BertConfig
from transformers.file_utils import add_start_docstrings, add_start_docstrings_to_model_forward

class ToyModel(nn.Module):
    num_classes: int

    def setup(self):
        self.conv1 = nn.Conv(features=32, kernel_size=(3, 3))
        self.relu = nn.relu
        self.avg_pool = partial(nn.avg_pool, window_shape=(2, 2), strides=(2, 2))
        self.conv2 = nn.Conv(features=64, kernel_size=(3, 3))
        self.fc1 = nn.Dense(features=256)
        self.fc2 = nn.Dense(features=self.num_classes)
        self.log_softmax = nn.log_softmax

    @nn.compact
    def __call__(self, x):
        x = self.conv1(x)
        x = self.relu(x)
        x = self.avg_pool(x)
        x = self.conv2(x)
        x = self.relu(x)
        x = self.avg_pool(x)
        x = x.reshape((x.shape[0], -1))  # flatten
        x = self.fc1(x)
        x = self.relu(x)
        x = self.fc2(x)
        x = self.log_softmax(x)
        return x


class Bert(FlaxBertPreTrainedModel):
    def __init__(
        self, config: BertConfig, input_shape: Tuple = (1, 1), seed: int = 0, dtype: jnp.dtype = jnp.float32, **kwargs
    ):
        module = BertModule(config=config, dtype=dtype, **kwargs)

        super().__init__(config, module, input_shape=input_shape, seed=seed, dtype=dtype)

    @add_start_docstrings_to_model_forward(BERT_INPUTS_DOCSTRING.format("batch_size, sequence_length"))
    def __call__(
        self,
        input_ids,
        attention_mask=None,
        token_type_ids=None,
        position_ids=None,
        params: dict = None,
        dropout_rng: PRNGKey = None,
        train: bool = False,
    ):
        input_ids, attention_mask, token_type_ids, position_ids = self._check_inputs(
            input_ids, attention_mask, token_type_ids, position_ids
        )

        # Handle any PRNG if needed
        rngs = {}
        if dropout_rng is not None:
            rngs["dropout"] = dropout_rng

        return self.module.apply(
            {"params": params or self.params},
            jnp.array(input_ids, dtype="i4"),
            jnp.array(attention_mask, dtype="i4"),
            jnp.array(token_type_ids, dtype="i4"),
            jnp.array(position_ids, dtype="i4"),
            not train,
            rngs=rngs,
        )

    def apply(self, *args, **kwargs):
        self.__call__(*args, **kwargs)

class BertModule(nn.Module):
    config: BertConfig
    dtype: jnp.dtype = jnp.float32  
    add_pooling_layer: bool = True

    def setup(self):
        self.bert = FlaxBertModule(config=self.config, dtype=self.dtype)
        self.MLMClassifier = FlaxBertOnlyMLMHead(config=self.config, dtype=self.dtype)
        self.NSPClassifier = FlaxBertOnlyNSPHead(dtype=self.dtype)
        
    @nn.compact
    def __call__(
        self, input_ids, attention_mask=None, token_type_ids=None, position_ids=None, deterministic: bool = True
    ):
        # Model
        hidden_states, pooled_output  = self.bert(input_ids, attention_mask, token_type_ids, position_ids, deterministic=deterministic)

        # Compute the prediction scores
        logits = self.MLMClassifier(hidden_states)
        seq_relationship_scores = self.NSPClassifier(pooled_output)

        return (logits, seq_relationship_scores)
