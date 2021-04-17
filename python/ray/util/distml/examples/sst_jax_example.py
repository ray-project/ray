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
from jax.tree_util import tree_flatten
from jax.experimental import optimizers
from jax.lib import xla_client
import jax.numpy as jnp
from jax_util.sst import make_sst5_dataloader
from jax_util.model_transformer import transformer, create_root_context

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
        
class SstTrainingOperator(JAXTrainingOperator):
    @override(JAXTrainingOperator)
    def setup(self, *args, **kwargs):
        rng_key = random.PRNGKey(0)

        batch_size = kwargs["batch_size"]
        lr = kwargs["lr"]

        n_ctx = 256  # length
        n_head = 4
        n_layer = 8
        n_embd = 256
        input_shape = (batch_size, n_ctx)

        predict_fun = functools.partial(transformer, n_vocab=30522,
            n_head=n_head, n_layer=n_layer, n_ctx=n_ctx, n_embd=n_embd)

        def loss(cx, batch):
            input, target = batch
            logprobs_btq = predict_fun(cx, input[:, :-1])
            return -jnp.sum(logprobs_btq * target)

        with FileLock(".ray.lock"):
            train_loader, val_loader, test_loader = make_sst5_dataloader(batch_size)
            
        root_cx = create_root_context()

        def init_fun():
            batch = next(iter(train_loader))
            loss(root_cx, batch) # Just create variables
            root_cx.allow_new = False
            # print_variables(root_cx)
            init_params = root_cx.variables_list()
            return init_params

        opt_init, opt_update, get_params = optimizers.adam(lr)

        init_params = init_fun()

        opt_state = opt_init(init_params)
    
        self.root_cx = root_cx
        
        self.register(model=[opt_state, init_fun, predict_fun], optimizer=[opt_init, opt_update, get_params], criterion=lambda logits, targets:-jnp.sum(logits * targets))
    
        self.register_data(train_loader=train_loader, validation_loader=test_loader)

        # self.register_input_signatures(input_shape=input_shape)

    @override(JAXTrainingOperator)
    def loss_func(self, params, batch):
        cx = self.root_cx.replace_with_list(params)
        inputs, targets = batch
        logits = self.predict_fun(cx, inputs)
        return self.criterion(logits, targets)

    @override(JAXTrainingOperator)
    def validate_step(self, params, batch, batch_info):
        if not hasattr(self, "opt_state"):
            raise RuntimeError("model unset. Please register model in setup.")
        if not hasattr(self, "criterion"):
            raise RuntimeError("criterion unset. Please register criterion in setup.")
        criterion = self.criterion
        predict_fun = self.predict_fun
        # unpack features into list to support multiple inputs model
        *inputs, targets = batch

        cx = self.root_cx.replace_with_list(params)
        with self.timers.record("eval_fwd"):
            outputs = predict_fun(cx, *inputs)
            loss = criterion(outputs, targets)
            prediction_class = jnp.argmax(outputs, axis=1)
            targets_class = jnp.argmax(targets, axis=1)

        acc = jnp.mean(prediction_class == targets_class)
        samples_num = targets.shape[0]

        return {
            "val_loss": loss,
            "val_accuracy": acc,
            "samples_num": samples_num
        }


def make_ar_trainer(args):
    trainer = AllReduceStrategy(
        training_operator_cls=SstTrainingOperator,
        world_size=args.num_workers,
        operator_config={
            "lr": 0.01,
           "test_mode": args.smoke_test,  # subset the data
            # this will be split across workers.
            "batch_size": 64,
            "num_classes": 10,
        },
        use_tqdm=True,
        record_config={
            "batch_size": 64,
            "num_workers": args.num_workers//2,
            "job_name": f"sst_transformer_{args.num_workers}workers",
            "save_freq": 50,
        },
        )
    return trainer


def make_ps_trainer(args):
    trainer = ParameterServerStrategy(
        training_operator_cls=SstTrainingOperator,
        world_size=args.num_workers,
        num_workers=args.num_workers//2,
        num_ps=args.num_workers//2,
        operator_config={
            "lr": 0.01,
           "test_mode": args.smoke_test,  # subset the data
            # this will be split across workers.
            "batch_size": 64,
            "num_classes": 10,
        },
        use_tqdm=True,
        record_config={
            "batch_size": 64,
            "num_workers": args.num_workers//2,
            "job_name": f"sst_transformer_{args.num_workers//2}ps_{args.num_workers//2}workers",
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
        "--num-epochs", type=int, default=5, help="Number of epochs to train.")
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

    # os.environ["CUDA_VISIBLE_DEVICES"] = "0,2"

    args, _ = parser.parse_known_args()
    num_cpus = 12
    num_gpus = args.num_workers
    ray.init(num_gpus=num_gpus, num_cpus=num_cpus, log_to_driver=True)

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
        val_stats = trainer.validate()
        info.update(val_acc=val_stats["val_accuracy"]) 

    trainer.shutdown()
    print("success!")
