# Semantic segmentation reference -> RaySGD

Original scripts are taken from: https://github.com/pytorch/vision/tree/master/references/segmentation.

On a single node, you can leverage Distributed Data Parallelism (DDP) by simply using the `-n` parameter. This will automatically parallelize your training across `n` GPUs. As listed from the original repository, below are standard hyperparameters.

```bash
pip install tqdm pycocotools
```


## fcn_resnet101
```
python train_segmentation.py  -n 4 --lr 0.02 --dataset coco -b 4 --model fcn_resnet101 --aux-loss
```

## deeplabv3_resnet101
```
python train_segmentation.py train_segmentation.py --lr 0.02 --dataset coco -b 4 --model deeplabv3_resnet101 --aux-loss
```

## Scaling up

This example can be executed on AWS by running
```
ray submit cluster.yaml train_segmentation.py -- start --args="--lr 0.02 ..."
```

To leverage multiple GPUs (beyond a single node), be sure to add an `address` parameter:

```
ray submit cluster.yaml train_segmentation.py -- start --args="--address='auto' --lr 0.02 ..."
```

