import os
import argparse

from filelock import FileLock

from tqdm import trange

import ray
from ray.util.distml.jax_operator import JAXTrainingOperator
from ray.util.distml.allreduce_strategy import AllReduceStrategy

from ray.util.sgd.utils import BATCH_SIZE, override

import numpy as np
import numpy.random as npr
import jax
from jax import jit, grad, random
from jax.tree_util import tree_flatten
from jax.experimental import optimizers
from jax.lib import xla_client
import jax.numpy as jnp
from jax_util.resnet import ResNet18, ResNet50, ResNet101, ToyModel 
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

    def __len__(self):
        return self.num_batches
        
class MnistTrainingOperator(JAXTrainingOperator):
    @override(JAXTrainingOperator)
    def setup(self, *args, **kwargs):
        batch_size = kwargs["batch_size"]
        rng_key = random.PRNGKey(0)
        input_shape = (28, 28, 1, batch_size)
        lr = kwargs["lr"]
        init_fun, predict_fun = ResNet18(kwargs["num_classes"])
        # init_fun, predict_fun = ToyModel(kwargs["num_classes"])

        _, init_params = init_fun(rng_key, input_shape)
            
        opt_init, opt_update, get_params = optimizers.adam(lr)
        opt_state = opt_init(init_params)
        
        with FileLock(".ray.lock"):
            train_images, train_labels, test_images, test_labels = mnist()
            
        train_images = train_images.reshape(train_images.shape[0], 1, 28, 28).transpose(2, 3, 1, 0)
        test_images = test_images.reshape(test_images.shape[0], 1, 28, 28).transpose(2, 3, 1, 0)

        train_labels = train_labels
        test_labels = test_labels

        train_loader = Dataloader(train_images, train_labels, batch_size=batch_size, shuffle=True)
        test_loader = Dataloader(test_images, test_labels, batch_size=batch_size)
        
        self.register(model=[opt_state, init_fun, predict_fun], optimizer=[opt_init, opt_update, get_params], criterion=lambda logits, targets:-jnp.sum(logits * targets))
    
        self.register_data(train_loader=train_loader, validation_loader=test_loader)

        self.register_input_signatures(input_shape=input_shape)

        # def accuracy_batch(outputs, targets):
        #     predicted_class = jnp.argmax(outputs, axis=-1)
        #     target_class = jnp.argmax(targets, axis=-1)
        #     return np.mean(list(predicted_class == target_class))
        # self.register_metrics({"accuracy": accuracy_batch})


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

    os.environ["CUDA_VISIBLE_DEVICES"] = "0,7"
    os.environ["XLA_FLAGS"] = "--xla_gpu_cuda_data_dir=/data/shanyx/cuda-10.1"

    args, _ = parser.parse_known_args()
    num_cpus = 4 if args.smoke_test else None
    ray.init(num_gpus=2, num_cpus=num_cpus, log_to_driver=True)

    trainer1 = AllReduceStrategy(
        training_operator_cls=MnistTrainingOperator,
        world_size=args.num_workers,
        operator_config={
            "lr": 0.01,
            "test_mode": args.smoke_test,  # subset the data
            # this will be split across workers.
            "batch_size": 128,
            "num_classes": 10,
            "use_tqdm": True,
        },
        )

    # trainer1.save_parameters("jax_checkpoint")
    # trainer1.load_parameters("jax_checkpoint")

    info = {"num_steps": 1}
    for i in range(args.num_epochs):
        info["epoch_idx"] = i
        info["num_epochs"] = args.num_epochs
        # Increase `max_retries` to turn on fault tolerance.
        trainer1.train(max_retries=1, info=info)
        val_stats = trainer1.validate()
        print("validate", val_stats)
        info.update(val_acc=val_stats["val_accuracy"]) 
        # pbar.set_postfix(dict(acc=val_stats["val_accuracy"]))

    trainer1.shutdown()
    print("success!")
