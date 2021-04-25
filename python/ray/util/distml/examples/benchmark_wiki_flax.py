import os
import argparse
import functools
from tqdm import trange

from filelock import FileLock

import numpy as np
import numpy.random as npr
import jax
from jax import jit, grad, value_and_grad, random
from jax.tree_util import tree_flatten, tree_unflatten
from jax.experimental import optimizers
from jax.lib import xla_client
import jax.numpy as jnp
import jax.nn as jnn

from flax import optim
from flax.core import unfreeze, freeze

from flax_util.datasets import make_wiki_train_loader, tf2numpy
from flax_util.models import Bert


from ray.util.distml.util import ThroughoutCollection, func_timer

from jax_util.datasets import _one_hot, _one_hot_jit
from transformers.models.bert.configuration_bert import BertConfig
# pip install git+https://github.com/Ezra-H/transformers.git

import tensorflow as tf
import time

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address",
        required=False,
        type=str,
        help="the address to use for connecting to the Ray cluster")
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=4,
        help="Sets number of workers for training.")
    parser.add_argument(
        "--num-epochs", type=int, default=20, help="Number of epochs to train.")
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enables GPU training")
    parser.add_argument(
        "--fp16",
        action="store_true",
        default=False,
        help="Enables FP16 training with apex. Requires `use-gpu`.")
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.")
    parser.add_argument(
        "--tune", action="store_true", default=False, help="Tune training")
    parser.add_argument(
        "--trainer", type=str, default="ar", help="Trainer type, Optional: ar, ps")

    os.environ["CUDA_VISIBLE_DEVICES"] = "1"
    tf.config.experimental.set_visible_devices([], 'GPU')
    args, _ = parser.parse_known_args()

    batch_size = 8
    lr = 0.01
    n_ctx = 128  

    collector = ThroughoutCollection(batch_size=batch_size, num_workers=1,
                                     job_name=f"flax_wiki_benchmark_bert_base")

    key = random.PRNGKey(0)

    config = BertConfig()
    # config = BertConfig.from_json_file("./flax_util/bert-large-uncased-config.json")

    model = Bert(config, (batch_size, n_ctx)) # output (64, 128, 768) (64, 768)
    params = model.init(key, (batch_size, n_ctx))

    def criterion1(logits, targets, weights):
        entroty = jax.vmap(lambda x,y:x[y])(logits,targets)
        return -jnp.mean(weights*entroty)

    def criterion2(logits, targets):
        return -jnp.mean(logits * targets)
        
    # criterion2 = lambda logits, targets: -jnp.mean(logits * targets) #  jnp.sum(jnp.dot(logits, targets))

    # 10,000 steps warmup. 
    # TODO(HUI): need to implementation a learning rate scheduler
    # lr_scheduler should be a function to return lr for current step
    class SimpleWarmUpScheduler:
        def __init__(self, learning_rate, warmup_steps=10000):
            self.lr = learning_rate
            self.warmup_steps = warmup_steps
            self.steps = 0

        def step(self):
            self.steps += 1
            if self.steps<self.warmup_steps:
                return self.lr * self.steps / self.warmup_steps
            else:
                return self.lr

    scheduler = SimpleWarmUpScheduler(learning_rate=lr, warmup_steps=2000)

    optimizer_def = optim.Adam(learning_rate=lr, weight_decay=0.01) # Choose the method
    optimizer = optimizer_def.create(params) # Create the wrapping optimizer with initial parameters

    train_loader = make_wiki_train_loader(batch_size=batch_size)
    
    def loss_func(params, batch):
        batch = tf2numpy(batch)
        input_ids = batch[0]["input_word_ids"]
        attention_mask = batch[0]["input_mask"]
        token_type_ids =  batch[0]["input_type_ids"]
        masked_positions = batch[0]["masked_lm_positions"]
        masked_lm_ids = batch[0]["masked_lm_ids"]
        masked_lm_weights = batch[0]["masked_lm_weights"]
        next_sentence_label = batch[0]["next_sentence_labels"]
        position_ids = None

        drop_key = random.PRNGKey(33)
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
        loss2 = criterion2(sentence_logits, _one_hot_jit(next_sentence_label,2))
        # loss2 = criterion2(sentence_logits, _one_hot(next_sentence_label,2))
        loss = (loss1 + loss2)/2
        return loss

    for i in range(args.num_epochs):
        for idx, batch in enumerate(train_loader):
            batch = tf2numpy(batch)
            with collector.record("train_batch"):
                # start_time = time.time()
                loss_val, grad = value_and_grad(loss_func)(optimizer.target, batch)
                optimizer = optimizer.apply_gradient(grad) # Return the updated optimizer with parameters.
                # print("time spending", time.time() - start_time)
            if idx % 10 == 0:
                print('Loss step {}: '.format(idx), loss_val.item())

    print("success!")
