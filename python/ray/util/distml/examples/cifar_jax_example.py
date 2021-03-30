import os
import argparse

from filelock import FileLock

from tqdm import trange

import ray
from ray.util.distml.jax_operator import JAXTrainingOperator
from ray.util.distml.allreduce_strategy import AllReduceStrategy

from ray.util.sgd.torch.resnet import ResNet18
from ray.util.sgd.utils import BATCH_SIZE, override

import numpy.random as npr
import jax
from jax import jit, grad, random
from jax.tree_util import tree_flatten
from jax.experimental import optimizers
from jax.lib import xla_client
import jax.numpy as jnp
from jax_util.resnet import ResNet18, ResNet50, ResNet101
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
            img_batch = self.data[:, :, :, batch_idx]
            label_batch = self.target[batch_idx]
            yield img_batch, label_batch

    def __iter__(self):
        return self.synth_batches()
        
        
class CifarTrainingOperator(JAXTrainingOperator):
    @override(JAXTrainingOperator)
    def setup(self, *args, **kwargs):
        rng_key = random.PRNGKey(0)
        input_shape = (28, 28, 1, kwargs["batch_size"])
        lr=0.01
        init_fun, predict_fun = ResNet18(kwargs["num_classes"])
        _, init_params = init_fun(rng_key, input_shape)
            
        opt_init, opt_update, get_params = optimizers.adam(lr)
        opt_state = opt_init(init_params)
        
        with FileLock(".ray.lock"):
            train_images, train_labels, test_images, test_labels = mnist()
            
        train_images = train_images.reshape(train_images.shape[0], 1, 28, 28)[:1000].transpose(2, 3, 1, 0)
        test_images = test_images.reshape(test_images.shape[0], 1, 28, 28)[:1000].transpose(2, 3, 1, 0)

        train_labels = train_labels[:1000]
        test_labels = test_labels[:1000]

        train_loader = Dataloader(train_images, train_labels, batch_size=128, shuffle=True)
        test_loader = Dataloader(test_images, test_labels, batch_size=128)
            
        self.register(model=[opt_state, get_params, predict_fun], optimizer=opt_update, criterion=lambda logits, targets:-jnp.sum(logits * targets))
    
        self.register_data(train_loader=train_loader, validation_loader=test_loader)


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

    args, _ = parser.parse_known_args()
    num_cpus = 4 if args.smoke_test else None
    ray.init(num_gpus=2, num_cpus=num_cpus, log_to_driver=True)

    trainer1 = AllReduceStrategy(
        training_operator_cls=CifarTrainingOperator,
        world_size=args.num_workers,
        operator_config={
            "lr": 0.1,
           "test_mode": args.smoke_test,  # subset the data
            # this will be split across workers.
            "batch_size": 64 * args.num_workers,
            "num_classes": 10,
        },
        )
    pbar = trange(args.num_epochs, unit="epoch")
    for i in pbar:
        info = {"num_steps": 1} if args.smoke_test else {}
        info["epoch_idx"] = i
        info["num_epochs"] = args.num_epochs
        # Increase `max_retries` to turn on fault tolerance.
        trainer1.train(max_retries=1, info=info)
        val_stats = trainer1.validate()
        pbar.set_postfix(dict(acc=val_stats["val_accuracy"]))

    print(trainer1.validate())
    trainer1.shutdown()
    print("success!")
