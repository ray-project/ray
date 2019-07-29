# flake8: noqa

# __tutorial_imports_begin__
# Original Code: https://github.com/pytorch/examples/blob/master/mnist/main.py
import numpy as np
import torch
import torch.optim as optim
from torchvision import datasets

from ray import tune
from ray.tune import track
from ray.tune.schedulers import ASHAScheduler
from ray.tune.examples.mnist_pytorch import get_data_loaders, ConvNet, train, test

datasets.MNIST("~/data", train=True, download=True)

# __tutorial_imports_end__


# __train_func_begin__
def train_mnist(config):
    model = ConvNet()
    train_loader, test_loader = get_data_loaders()
    optimizer = optim.SGD(
        model.parameters(), lr=config["lr"], momentum=config["momentum"])
    for i in range(10):
        train(model, optimizer, train_loader)
        acc = test(model, test_loader)
        track.log(mean_accuracy=acc)
        if i % 5 == 0:
            # This saves the model to the trial directory
            torch.save(model, "./model.pth")
# __train_func_end__

# __eval_func_begin__
experiment_config = dict(name="train_mnist", stop={"mean_accuracy": 0.98})

search_space = {
    "lr": tune.sample_from(lambda spec: 10**(-10 * np.random.rand())),
    "momentum": tune.uniform(0.1, 0.9)
}

# Note: use `ray.init(redis_address=...)` to enable distributed execution
analysis = tune.run(train_mnist, config=search_space, **experiment_config)
# __eval_func_end__

# __run_scheduler_begin__
analysis = tune.run(
    train_mnist,
    num_samples=30,
    scheduler=ASHAScheduler(metric="mean_accuracy", mode="max"),
    config=search_space,
    **experiment_config)

# Obtain a trial dataframe from all run trials of this `tune.run` call.
dfs = analysis.get_all_trial_dataframes()
# __run_scheduler_end__

# __run_searchalg_begin__
from hyperopt import hp
from ray.tune.suggest.hyperopt import HyperOptSearch

space = {
    "lr": hp.loguniform("lr", 1e-10, 0.1),
    "momentum": hp.uniform("momentum", 0.1, 0.9),
}

hyperopt_search = HyperOptSearch(
    space, max_concurrent=2, reward_attr="mean_accuracy")

analysis = tune.run(
    train_mnist,
    num_samples=10,
    search_alg=hyperopt_search,
    **experiment_config)
# __run_searchalg_end__
