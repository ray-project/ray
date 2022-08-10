#! /bin/bash
git clone https://github.com/ray-project/ray.git
cd "$HOME/ray/python/ray/train/examples" && git pull 
sudo env LD_LIBRARY_PATH=/usr/local/lib python3 "$HOME/ray/python/ray/train/examples/jax_tpu/jax_mnist_example_tpu.py"
