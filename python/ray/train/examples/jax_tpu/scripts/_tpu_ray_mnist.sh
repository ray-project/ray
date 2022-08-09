#! /bin/bash
git clone https://github.com/JiahaoYao/ray.git
cd ray && git checkout jax_trainer_new_0727
sudo ln -s $HOME/ray/python/ray/train/jax  /usr/local/lib/python3.8/dist-packages/ray/train/jax
sudo ln -s $HOME/ray/python/ray/train/examples/jax_mnist_example.py /usr/local/lib/python3.8/dist-packages/ray/train/examples/jax_mnist_example.py
python3 -c "import ray; from ray.train.jax import JaxTrainer"

# ray status
cd $HOME/ray/python/ray/train/examples && git pull 
sudo env LD_LIBRARY_PATH=/usr/local/lib python3 $HOME/ray/python/ray/train/examples/jax_tpu/jax_mnist_example_tpu.py