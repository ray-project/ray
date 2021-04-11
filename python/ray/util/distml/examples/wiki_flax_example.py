import os
import argparse
import functools
from tqdm import trange

from filelock import FileLock

import ray
from ray.util.distml.jax_operator import JAXTrainingOperator
from ray.util.distml.allreduce_strategy import AllReduceStrategy
from ray.util.distml.ps_strategy import ParameterServerStrategy

from ray.util.sgd.utils import BATCH_SIZE, override

import numpy as np
import numpy.random as npr
import jax
from jax import jit, grad, random
from jax.tree_util import tree_flatten, tree_unflatten
from jax.experimental import optimizers
from jax.lib import xla_client
import jax.numpy as jnp
import jax.nn as jnn

from flax import optim
from flax.core import unfreeze, freeze

from flax_util.datasets import make_wiki_train_loader
from flax_util.models import Bert

from jax_util.datasets import _one_hot
from transformers.models.bert.configuration_bert import BertConfig

        
def train():
    batch_size = 8
    lr = 0.001
    n_ctx = 128  

    key = random.PRNGKey(0)
    drop_key = random.PRNGKey(33)

    config = BertConfig()

    model = Bert(config, (batch_size, n_ctx)) # output (64, 128, 768) (64, 768)
    params = model.init(key, (batch_size, n_ctx))

    def criterion1(logits, targets, weights):
        entroty = jax.vmap(lambda x,y:x[y])(logits,targets)
        return -jnp.mean(weights*entroty)
    criterion2 = lambda logits, targets: -jnp.mean(logits * targets) #  jnp.sum(jnp.dot(logits, targets))
    
    def loss_fn(params, batch):
        input_ids = batch[0]["input_word_ids"]
        attention_mask = batch[0]["input_mask"]
        token_type_ids =  batch[0]["input_type_ids"]
        masked_positions = batch[0]["masked_lm_positions"]
        masked_lm_ids = batch[0]["masked_lm_ids"]
        masked_lm_weights = batch[0]["masked_lm_weights"]
        next_sentence_label = batch[0]["next_sentence_labels"]
        position_ids = None

        logits = model(input_ids=input_ids,
                       attention_mask=attention_mask,
                       token_type_ids=token_type_ids,
                       position_ids=position_ids,
                       params=params,
                       train=True,
                       dropout_rng=drop_key)

        mask_logits = jax.vmap(lambda x,y:x[y])(logits[0], masked_positions)
        mask_logits = jnn.log_softmax(mask_logits)
        sentence_logits = jnn.log_softmax(logits[1])

        loss1 = jnp.mean(jax.vmap(criterion1)(mask_logits, masked_lm_ids, masked_lm_weights))
        loss2 = criterion2(sentence_logits, _one_hot(next_sentence_label,2))
        loss = (loss1 + loss2)/2
        return loss

    optimizer_def = optim.GradientDescent(learning_rate=lr) # Choose the method
    optimizer = optimizer_def.create(params) # Create the wrapping optimizer with initial parameters
    del params
    loss_grad_fn = jax.value_and_grad(loss_fn)

    train_loader = make_wiki_train_loader(batch_size=batch_size)
    
    for i in range(10):
        for idx, batch in enumerate(train_loader):
            loss_val, grad = loss_grad_fn(optimizer.target, batch)
            optimizer = optimizer.apply_gradient(grad) # Return the updated optimizer with parameters.
            if idx % 10 == 0:
                print('Loss step {}: '.format(idx), loss_val.item())

if __name__ == "__main__":
    os.environ["CUDA_VISIBLE_DEVICES"] = "3"
    train()