from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


def test_pytorch_mnist():
    from torchvision import datasets
    from ray.tune.examples.mnist_pytorch_trainable import TrainMNIST, parser
    datasets.MNIST("~/data", train=True, download=True)
    args = parser.parse_args('')
    trainable = TrainMNIST(config={"args": args})
    trainable.train()
    path = trainable.save()
    trainable.restore(path)
