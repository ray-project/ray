# flake8: noqa

# This is an example quickstart for Tune.
# To connect to a cluster, uncomment below:

# import ray
# import argparse
# parser = argparse.ArgumentParser()
# parser.add_argument("--redis-address")
# args = parser.parse_args()
# ray.init(redis_address=args.redis_address)

# __quick_start_begin__
import torch.optim as optim
from ray import tune
from ray.tune.examples.mnist_pytorch import get_data_loaders, ConvNet, train, test


def train_mnist(config):
    train_loader, test_loader = get_data_loaders()
    model = ConvNet()
    optimizer = optim.SGD(model.parameters(), lr=config["lr"])
    for i in range(10):
        train(model, optimizer, train_loader)
        acc = test(model, test_loader)
        tune.track.log(mean_accuracy=acc)


analysis = tune.run(
    train_mnist, config={"lr": tune.grid_search([0.001, 0.01, 0.1])})

print("Best config: ", analysis.get_best_config(metric="mean_accuracy"))

# Get a dataframe for analyzing trial results.
df = analysis.dataframe()
# __quick_start_end__
