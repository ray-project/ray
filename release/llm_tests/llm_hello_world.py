import platform

import ray
import vllm

if __name__ == "__main__":
    print("python version: ", platform.python_version())
    ray.init()
    print("ray version: ", ray.__version__)
    print("vllm version: ", vllm.__version__)
