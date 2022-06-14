
## MNIST classification

Trains a simple convolutional network on the MNIST dataset.

Quick run
```
python3 jax_mnist_example.py 
```

### Requirements
* TensorFlow dataset `mnist` will be downloaded and prepared automatically, if necessary

### How to run

- multi-gpu one-node training 
For example, training with `g4dn.12xlarge`
`python3 jax_mnist_example.py --num-nodes 1 --use_gpu -ngpu 4`


- multi-gpu multi-node training 
For example, training with a ray cluster with 4 `g4dn.12xlarge` nodes. 
`python3 jax_mnist_example.py --num-nodes 4 --use_gpu -ngpu 4`

- tpu-pod training 

For example, distributed training with a `v2-32` Pod slice
 
```
sudo env LD_LIBRARY_PATH=/usr/local/lib python3 jax_mnist_example.py --num-nodes 4 --use_tpu
```

**Note**: adding the environment variable is due to [this issue](https://stackoverflow.com/questions/67257008/oserror-libmkl-intel-lp64-so-1-cannot-open-shared-object-file-no-such-file-or).

### Acknoledgements
Adopted from https://github.com/google/flax/tree/main/examples/mnist with the following modifications to better demonstrate the training speeedup on this small network.
- Use a large batch size
- Use ray dataset for data loading instead of jax numpy array
- Remove random permutation
- change the CNN to MLP