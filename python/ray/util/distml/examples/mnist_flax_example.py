import os
import argparse

from filelock import FileLock

from tqdm import trange

import ray
from ray.util.distml.flax_operator import FLAXTrainingOperator
from ray.util.distml.allreduce_strategy import AllReduceStrategy
from ray.util.distml.ps_strategy import ParameterServerStrategy
from ray.util.sgd.utils import BATCH_SIZE, override

import numpy as np
import numpy.random as npr

import jax
from jax import jit, grad, random
from jax.tree_util import tree_flatten
import jax.numpy as jnp

import flax 
import flax.optim as optim

from flax_util.models import ToyModel 
from jax_util.datasets import mnist

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
            img_batch = self.data[batch_idx]
            label_batch = self.target[batch_idx]
            yield img_batch, label_batch

    def __iter__(self):
        return self.synth_batches()

    def __len__(self):
        return self.num_batches
        

class MnistTrainingOperator(FLAXTrainingOperator):
    @override(FLAXTrainingOperator)
    def setup(self, *args, **kwargs):
        key1, key2 = random.split(random.PRNGKey(0))
        batch_size = kwargs["batch_size"]
        input_shape = (batch_size, 28, 28, 1)
        lr = kwargs["lr"]

        model = ToyModel(num_classes=10)
        x = random.normal(key1, input_shape)
        params = model.init(key2, x)

        optimizer_def = optim.Adam(learning_rate=lr) # Choose the method
        optimizer = optimizer_def.create(params) # Create the wrapping optimizer with initial parameters
        
        with FileLock(".ray.lock"):
            train_images, train_labels, test_images, test_labels = mnist()
            
        train_images = train_images.reshape(train_images.shape[0], 1, 28, 28).transpose(0, 2, 3, 1)
        test_images = test_images.reshape(test_images.shape[0], 1, 28, 28).transpose(0, 2, 3, 1)

        train_loader = Dataloader(train_images, train_labels, batch_size=batch_size, shuffle=True)
        test_loader = Dataloader(test_images, test_labels, batch_size=batch_size)
        
        self.register(model=model, optimizer=optimizer, criterion=lambda logits, targets:-jnp.sum(logits * targets))
    
        self.register_data(train_loader=train_loader, validation_loader=test_loader)


def make_ar_trainer(args):
    trainer = AllReduceStrategy(
        training_operator_cls=MnistTrainingOperator,
        world_size=3,
        operator_config={
            "lr": 0.01,
           "test_mode": args.smoke_test,  # subset the data
            # this will be split across workers.
            "batch_size": 128,
            "num_classes": 10,
        },
        use_tqdm=True,
        record_config={
            "batch_size": 128,
            "num_workers": 2,
            "job_name": "flax_mnist_ps_2workers",
            "save_freq": 50,
        },
        )
    return trainer


def make_ps_trainer(args):
    trainer = ParameterServerStrategy(
        training_operator_cls=MnistTrainingOperator,
        world_size=4,
        num_workers=2,
        num_ps=2,
        operator_config={
            "lr": 0.01,
           "test_mode": args.smoke_test,  # subset the data
            # this will be split across workers.
            "batch_size": 128,
            "num_classes": 10,
        },
        use_tqdm=True,
        record_config={
            "batch_size": 128,
            "num_workers": 2,
            "job_name": "flax_mnist_allreduce_2workers",
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
        default=4,
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

    # os.environ["CUDA_VISIBLE_DEVICES"] = "0,2,6,7"

    args, _ = parser.parse_known_args()
    num_cpus = 4 if args.smoke_test else None
    ray.init(num_gpus=args.num_workers, num_cpus=num_cpus, log_to_driver=True)

    if args.trainer == "ar":
        trainer = make_ar_trainer(args)
    elif args.trainer == "ps":
        trainer = make_ps_trainer(args)
    else:
        raise RuntimeError("Unrecognized trainer type. Except 'ar' or 'ps'"
                           "Got {}".format(args.trainer))

    # trainer.load_parameters("jax_checkpoint.db")

    info = {"num_steps": 1}
    for i in range(args.num_epochs):
        info["epoch_idx"] = i
        info["num_epochs"] = args.num_epochs
        # Increase `max_retries` to turn on fault tolerance.
        trainer.train(max_retries=1, info=info)
        val_stats = trainer.validate()
        print("validate", val_stats)
        info.update(val_acc=val_stats["val_accuracy"]) 
        # pbar.set_postfix(dict(acc=val_stats["val_accuracy"]))
        # trainer.save_parameters("jax_checkpoint.db")

    trainer.shutdown()
    print("success!")
