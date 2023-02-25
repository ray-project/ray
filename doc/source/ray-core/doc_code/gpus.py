# flake8: noqa

# __get_gpu_ids_start__
import os
import ray

ray.init(num_gpus=2)


@ray.remote(num_gpus=1)
class GPUActor:
    def ping(self):
        print("ray.get_gpu_ids(): {}".format(ray.get_gpu_ids()))
        print("CUDA_VISIBLE_DEVICES: {}".format(os.environ["CUDA_VISIBLE_DEVICES"]))


@ray.remote(num_gpus=1)
def use_gpu():
    print("ray.get_gpu_ids(): {}".format(ray.get_gpu_ids()))
    print("CUDA_VISIBLE_DEVICES: {}".format(os.environ["CUDA_VISIBLE_DEVICES"]))


gpu_actor = GPUActor.remote()
ray.get(gpu_actor.ping.remote())
# The actor uses the first GPU so the task will use the second one.
ray.get(use_gpu.remote())
# (GPUActor pid=52420) ray.get_gpu_ids(): [0]
# (GPUActor pid=52420) CUDA_VISIBLE_DEVICES: 0
# (use_gpu pid=51830) ray.get_gpu_ids(): [1]
# (use_gpu pid=51830) CUDA_VISIBLE_DEVICES: 1
# __get_gpu_ids_end__


# __gpus_tf_start__
@ray.remote(num_gpus=1)
def use_gpu():
    import tensorflow as tf

    # Create a TensorFlow session. TensorFlow will restrict itself to use the
    # GPUs specified by the CUDA_VISIBLE_DEVICES environment variable.
    tf.Session()


# __gpus_tf_end__

ray.shutdown()

# __fractional_gpus_start__

ray.init(num_cpus=4, num_gpus=1)


@ray.remote(num_gpus=0.25)
def f():
    import time

    time.sleep(1)


# The four tasks created here can execute concurrently
# and share the same GPU.
ray.get([f.remote() for _ in range(4)])
# __fractional_gpus_end__

ray.shutdown()

# __fractional_gpus_packing_start__
ray.init(num_gpus=3)


@ray.remote(num_gpus=0.5)
class FractionalGPUActor:
    def ping(self):
        print("ray.get_gpu_ids(): {}".format(ray.get_gpu_ids()))


fractional_gpu_actors = [FractionalGPUActor.remote() for _ in range(3)]
# Ray will try to pack GPUs if possible.
[ray.get(fractional_gpu_actors[i].ping.remote()) for i in range(3)]
# (FractionalGPUActor pid=57417) ray.get_gpu_ids(): [0]
# (FractionalGPUActor pid=57416) ray.get_gpu_ids(): [0]
# (FractionalGPUActor pid=57418) ray.get_gpu_ids(): [1]
# __fractional_gpus_packing_end__


# __leak_gpus_start__
# By default, ray will not reuse workers for GPU tasks to prevent
# GPU resource leakage.
@ray.remote(num_gpus=1)
def leak_gpus():
    import tensorflow as tf

    # This task will allocate memory on the GPU and then never release it.
    tf.Session()


# __leak_gpus_end__

ray.shutdown()
import ray.util.accelerators
import ray._private.ray_constants as ray_constants

v100_resource_name = f"{ray_constants.RESOURCE_CONSTRAINT_PREFIX}{ray.util.accelerators.NVIDIA_TESLA_V100}"
ray.init(num_gpus=4, resources={v100_resource_name: 1})


# __accelerator_type_start__
from ray.util.accelerators import NVIDIA_TESLA_V100


@ray.remote(num_gpus=1, accelerator_type=NVIDIA_TESLA_V100)
def train(data):
    return "This function was run on a node with a Tesla V100 GPU"


ray.get(train.remote(1))
# __accelerator_type_end__
