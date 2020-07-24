# Original Code here:
# https://github.com/pytorch/examples/blob/master/mnist/main.py
import logging
import torch
import torch.optim as optim
from torch.nn.parallel import DistributedDataParallel

import ray
from ray import tune
from ray.tune.examples.mnist_pytorch import (train, test, get_data_loaders,
                                             ConvNet)
from ray.util.sgd.torch.func_trainable import (DistributedTrainableCreator,
                                               distributed_checkpoint)

logger = logging.getLogger(__name__)


def train_mnist(config, checkpoint=False):
    use_cuda = config.get("use_gpu") and torch.cuda.is_available()
    device = torch.device("cuda" if use_cuda else "cpu")
    train_loader, test_loader = get_data_loaders()
    model = ConvNet().to(device)
    optimizer = optim.SGD(model.parameters(), lr=0.1)

    if checkpoint:
        with open(checkpoint) as f:
            model_state, optimizer_state = torch.load(f)

        model.load_state_dict(model_state)
        optimizer.load_state_dict(optimizer_state)

    model = DistributedDataParallel(model)

    for epoch in range(10):
        train(model, optimizer, train_loader, device)
        acc = test(model, test_loader, device)

        if epoch % 3 == 0:
            with distributed_checkpoint(label=epoch) as f:
                torch.save((model.state_dict(), optimizer.state_dict()), f)
        tune.report(mean_accuracy=acc)


if __name__ == "__main__":
    ray.init(num_cpus=2)

    trainable_cls = DistributedTrainableCreator(train_mnist, num_workers=2)
    # trainable = trainable_cls()
    # for i in range(10):
    #     print(trainable.train())

    tune.run(trainable_cls, num_samples=4, stop={"training_iteration": 2})
