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
from jax_util.sst import make_sst5_dataloader, make_sst2_dataloader

from flax import optim
# from transformers.models.auto.modeling_flax_auto import FlaxAutoModel
from transformers.models.bert.modeling_flax_bert import FlaxBertModel
from transformers.models.bert.modeling_flax_bert import FlaxBertForSequenceClassification
from transformers.models.bert.configuration_bert import BertConfig
# from transformers.models.roberta.modeling_flax_roberta import FlaxRobertaModel



def initialization_hook():
    # Need this for avoiding a connection restart issue on AWS.
    os.environ["NCCL_SOCKET_IFNAME"] = "^docker0,lo"
    os.environ["NCCL_LL_THRESHOLD"] = "0"

    # set the below if needed
    # print("NCCL DEBUG SET")
    # os.environ["NCCL_DEBUG"] = "INFO"

class Dataloader:
    def __init__(self, data, target, batch_size=128, shuffle=False):
        '''
        data: shape(width, height, channel, num)
        target: shape(num, num_classes)
        '''
        self.data = data
        self.target = target
        self.batch_size = batch_size
        num_data = self.target.shape[0]
        num_complete_batches, leftover = divmod(num_data, batch_size)
        self.num_batches = num_complete_batches + bool(leftover)
        self.shuffle = shuffle

    def synth_batches(self):
        num_imgs = self.target.shape[0]
        rng = npr.RandomState(npr.randint(10))
        perm = rng.permutation(num_imgs) if self.shuffle else np.arange(num_imgs)
        for i in range(self.num_batches):
            batch_idx = perm[i * self.batch_size:(i + 1) * self.batch_size]
            img_batch = self.data[:, :, :, batch_idx]
            label_batch = self.target[batch_idx]
            yield img_batch, label_batch

    def __iter__(self):
        return self.synth_batches()

    def __len__(self):
        return self.num_batches
        
def train():
    num_classes = 5
    batch_size = 32
    lr = 0.001
    n_ctx = 256  # length
    # n_head = 4
    # n_layer = 8
    # n_embd = 256

    key = random.PRNGKey(0)
    # x = random.uniform(key1, (batch_size, n_ctx))
    # "bert-base-cased"
    # "bert-large-uncased"
    model_name = "bert-base-cased"
    # config = BertConfig.from_json_file("./jax_util/config.json")
    config = BertConfig(num_labels=num_classes)
    # print(config)
    # tokenizer = AutoTokenizer.from_pretrained(model_name)
    # model = FlaxBertForSequenceClassification(config, (batch_size, n_ctx))
    model = FlaxBertForSequenceClassification.from_pretrained(model_name)
    # model = FlaxBertModel(config, (batch_size, n_ctx)) # output (64, 256, 768) (64, 768)
    params = model.init(key, (batch_size, n_ctx))
    # 64,5 @ 5,64
    criterion = lambda logits, targets: -jnp.sum(jnn.log_softmax(logits) * targets) #  jnp.sum(jnp.dot(logits, targets))

    def loss_fn(params, batch):
        inputs, targets = batch 
        logits = model(inputs, params=params)
        loss = criterion(logits[0], jnp.asarray(targets)) # compute the loss
        aux = logits # compute auxiliary outputs (eg. training metrics)
        return loss, aux

    optimizer_def = optim.Adam(learning_rate=lr) # Choose the method
    optimizer_def = optim.Gradien(learning_rate=lr) # Choose the method
    optimizer = optimizer_def.create(params) # Create the wrapping optimizer with initial parameters
    loss_grad_fn = jax.value_and_grad(loss_fn, has_aux=True)
    
    with FileLock(".ray.lock"):
        train_loader, val_loader, test_loader = make_sst2_dataloader(batch_size)
    
    for idx, batch in enumerate(train_loader):
        (loss_val, logits), grad = loss_grad_fn(optimizer.target, batch)
        
        grad_flat, tree = tree_flatten(grad)
        print(len(grad_flat))
        # print(grad.keys())
        optimizer = optimizer.apply_gradient(grad) # Return the updated optimizer with parameters.
        if idx % 10 == 0:
            print('Loss step {}: '.format(idx), loss_val)

if __name__ == "__main__":
    train()