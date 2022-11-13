# flake8: noqa

# fmt: off
# __gpu_start__

import ray
import os

@ray.remote(num_gpus=1)
def use_gpu():
    print("ray.get_gpu_ids(): {}".format(ray.get_gpu_ids()))
    print("CUDA_VISIBLE_DEVICES: {}".format(os.environ["CUDA_VISIBLE_DEVICES"]))

# __gpu_end__
# fmt: on

# fmt: off
# __tf_start__

import tensorflow as tf

@ray.remote(num_gpus=1)
def use_gpu():
    # Create a TensorFlow session. TensorFlow will restrict itself to use the
    # GPUs specified by the CUDA_VISIBLE_DEVICES environment variable.
    tf.Session()

# _tf_end__
# fmt: on

# fmt: off
# __leak_gpu_start__

import tensorflow as tf

@ray.remote(num_gpus=1, max_calls=1)
def leak_gpus():
    # This task will allocate memory on the GPU and then never release it, so
    # we include the max_calls argument to kill the worker and release the
    # resources.
    sess = tf.Session()

# The four tasks created here can execute concurrently.
ray.get([f.remote() for _ in range(4)])
# __leak_gpu_end__
# fmt: on
