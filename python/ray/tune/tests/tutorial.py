# flake8: noqa
# Original Code: https://github.com/pytorch/examples/blob/master/mnist/main.py

# yapf: disable
# __tutorial_imports_begin__
import numpy as np
import torch
import torch.optim as optim
from torchvision import datasets

from ray import tune
from ray.tune import track
from ray.tune.schedulers import ASHAScheduler
from ray.tune.examples.mnist_pytorch import get_data_loaders, ConvNet, train, test
# __tutorial_imports_end__
# yapf: enable


# yapf: disable
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
# yapf: enable

# __eval_func_begin__
search_space = {
    "lr": tune.sample_from(lambda spec: 10**(-10 * np.random.rand())),
    "momentum": tune.uniform(0.1, 0.9)
}

# Uncomment this to enable distributed execution
# `ray.init(redis_address=...)`

analysis = tune.run(train_mnist, config=search_space)
# __eval_func_end__

#__plot_begin__
dfs = analysis.trial_dataframes
[d.mean_accuracy.plot() for d in dfs.values()]
#__plot_end__

# __run_scheduler_begin__
analysis = tune.run(
    train_mnist,
    num_samples=30,
    scheduler=ASHAScheduler(metric="mean_accuracy", mode="max"),
    config=search_space)

# Obtain a trial dataframe from all run trials of this `tune.run` call.
dfs = analysis.trial_dataframes
# __run_scheduler_end__

# yapf: disable
# __plot_scheduler_begin__
# Plot by epoch
ax = None  # This plots everything on the same plot
for d in dfs.values():
    ax = d.mean_accuracy.plot(ax=ax, legend=False)
# __plot_scheduler_end__
# yapf: enable

# __run_searchalg_begin__
from hyperopt import hp
from ray.tune.suggest.hyperopt import HyperOptSearch

space = {
    "lr": hp.loguniform("lr", 1e-10, 0.1),
    "momentum": hp.uniform("momentum", 0.1, 0.9),
}

hyperopt_search = HyperOptSearch(
    space, max_concurrent=2, reward_attr="mean_accuracy")

analysis = tune.run(train_mnist, num_samples=10, search_alg=hyperopt_search)
# __run_searchalg_end__

# __run_analysis_begin__
import os

df = analysis.dataframe()
logdir = analysis.get_best_logdir("mean_accuracy", mode="max")
model = torch.load(os.path.join(logdir, "model.pth"))
# __run_analysis_end__

from ray.tune.examples.mnist_pytorch_trainable import TrainMNIST

# __trainable_run_begin__
search_space = {
    "lr": tune.sample_from(lambda spec: 10**(-10 * np.random.rand())),
    "momentum": tune.uniform(0.1, 0.9)
}

analysis = tune.run(
    TrainMNIST, config=search_space, stop={"training_iteration": 10})
# __trainable_run_end__
