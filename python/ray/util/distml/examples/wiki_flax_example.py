import os
import argparse
import functools
from tqdm import trange

from filelock import FileLock

import ray
from ray.util.distml.flax_operator import FLAXTrainingOperator
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

from flax_util.datasets import make_wiki_train_loader, tf2numpy
from flax_util.models import Bert

from jax_util.datasets import _one_hot
from transformers.models.bert.configuration_bert import BertConfig
# pip install git+https://github.com/Ezra-H/transformers.git

import tensorflow as tf

class WikiTrainingOperator(FLAXTrainingOperator):
    @override(FLAXTrainingOperator)
    def setup(self, *args, **kwargs):
        batch_size = kwargs["batch_size"]
        lr = kwargs["lr"]
        n_ctx = 128  

        key = random.PRNGKey(0)

        config = BertConfig()
        # config = BertConfig.from_json_file("./flax_util/bert-large-uncased-config.json")

        model = Bert(config, (batch_size, n_ctx)) # output (64, 128, 768) (64, 768)
        params = model.init(key, (batch_size, n_ctx))

        def criterion1(logits, targets, weights):
            entroty = jax.vmap(lambda x,y:x[y])(logits,targets)
            return -jnp.mean(weights*entroty)
        criterion2 = lambda logits, targets: -jnp.mean(logits * targets) #  jnp.sum(jnp.dot(logits, targets))

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
        
        self.register(model=model, optimizer=optimizer, criterion=[criterion1, criterion2], lr_schedulers=scheduler)
    
        self.register_data(train_loader=train_loader, validation_loader=None)

    @override(FLAXTrainingOperator)
    def loss_func(self, params, batch):
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
        logits = self.model(input_ids=input_ids,
                            attention_mask=attention_mask,
                            token_type_ids=token_type_ids,
                            position_ids=position_ids,
                            params=params,
                            train=True,
                            dropout_rng=drop_key)

        mask_logits = jax.vmap(lambda x,y:x[y])(logits[0], masked_positions)
        mask_logits = jnn.log_softmax(mask_logits)
        sentence_logits = jnn.log_softmax(logits[1])

        loss1 = jnp.mean(jax.vmap(self.criterion[0])(mask_logits, masked_lm_ids, masked_lm_weights))
        loss2 = self.criterion[1](sentence_logits, _one_hot(next_sentence_label,2))
        loss = (loss1 + loss2)/2
        return loss


def make_ar_trainer(args):
    trainer = AllReduceStrategy(
        training_operator_cls=WikiTrainingOperator,
        world_size=args.num_workers,
        operator_config={
            "lr": 0.01,
            "test_mode": args.smoke_test,  # subset the data
            # this will be split across workers.
            "batch_size": 8,
        },
        use_tqdm= True,
        max_iteration= 3000,
        record_config={
            "batch_size": 8,
            "num_workers": args.num_workers,
            "job_name": f"wiki_bert_ar_{args.num_workers}workers",
            "save_freq": 50,
        },
        )
    return trainer

def make_ps_trainer(args):
    trainer = ParameterServerStrategy(
        training_operator_cls=WikiTrainingOperator,
        world_size=args.num_workers,
        num_workers=args.num_workers - args.num_ps,
        num_ps=args.num_ps,
        operator_config={
            "lr": 0.01,
           "test_mode": args.smoke_test,  # subset the data
            # this will be split across workers.
            "batch_size": 8,
        },
        use_tqdm=True,
        max_iteration=3000,
        record_config={
            "batch_size": 8,
            "num_workers": args.num_workers-args.num_ps,
            "job_name": f"wiki_bert_{args.num_ps}ps_{args.num_workers-args.num_ps}workers",
            "save_freq": 50,
        },
        )

    return trainer

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
        default=2,
        help="Sets number of workers for training.")
    parser.add_argument(
        "--num-ps", type=int, default=1, help="Sets number of parameter server for training.")
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

    # os.environ["CUDA_VISIBLE_DEVICES"] = "1,6"
    tf.config.experimental.set_visible_devices([], 'GPU')

    args, _ = parser.parse_known_args()
    num_cpus = 4 if args.smoke_test else None

    if args.address:
        ray.init(args.address)
    else:
        ray.init(num_gpus=args.num_workers, num_cpus=args.num_workers, log_to_driver=True, resources={"server":args.num_ps})

    if args.trainer == "ar":
        trainer = make_ar_trainer(args)
    elif args.trainer == "ps":
        trainer = make_ps_trainer(args)
    else:
        raise RuntimeError("Unrecognized trainer type. Except 'ar' or 'ps'"
                           "Got {}".format(args.trainer))

    info = {"num_steps": 1}
    for i in range(args.num_epochs):
        info["epoch_idx"] = i
        info["num_epochs"] = args.num_epochs
        # Increase `max_retries` to turn on fault tolerance.
        trainer.train(max_retries=1, info=info)
        # val_stats = trainer.validate()
        # info.update(val_acc=val_stats["val_accuracy"]) 

    trainer.shutdown()
    print("success!")
