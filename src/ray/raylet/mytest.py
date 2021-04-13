import ray
ray.init()

import tensorflow as tf

@ray.remote
def f():
    return tf.__version__
print("tf version:", ray.get(f.remote()))
