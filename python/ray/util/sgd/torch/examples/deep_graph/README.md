# Distributed Graph Attention Network training with DGL and RaySGD
This is an example of integrating DGL(https://github.com/dmlc/dgl) and RaySGD for distributed graph neural network training.

Original scripts are taken from: https://github.com/dmlc/dgl/blob/master/examples/pytorch/ogb/ogbn-products/gat/main.py.

In order to overwrite the train_epoch and validate methods in the custom class, we adjusted the execution logic of the original code to make it easier to extend to training on multiple GPUs.

## Packages required
- ray: 1.1.0  
- DGL: 0.6.0 
- torch: 1.8.1+cu102

## Run

```
python gat_dgl.py --lr 0.001 --num-epochs 20 --use-gpu True
```
To leverage multiple GPUs (beyond a single node), be sure to add an `address` parameter:
```
python gat_dgl.py --args="--address='auto' --lr 0.001 ..."
```

## Note
Ray will set OMP_NUM_THREADS=1 by default when running, but we found that this will cause data sampling and loading to the GPU in the train_epoch/validate method to become very slow. Therefore, it is necessary to manually set this parameter to the number of cpu cores of the machine, so that the training time of each epoch will be reduced a lot.
For the setting description of this parameter, please refer https://docs.ray.io/en/master/configure.html.
