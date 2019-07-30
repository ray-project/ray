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

# __run_scheduler_begin__
analysis = tune.run(
    train_mnist,
    num_samples=30,
    scheduler=ASHAScheduler(metric="mean_accuracy", mode="max"),
    config=search_space)

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

# __run_analysis_end__
df = analysis.dataframe()
logdir = analysis.get_best_logdir("mean_accuracy", mode="max")
model = torch.load(logdir + "/model.pth")
# __run_analysis_end__

# yapf: disable
# __trainable_example_begin__
from ray.tune import Trainable

class TrainMNIST(Trainable):
    def _setup(self, config):
        torch.manual_seed(config.get("seed"))
        if config.get("use_gpu"):
            torch.cuda.manual_seed(config.get("seed"))

        use_cuda = config.get("use_gpu") and torch.cuda.is_available()
        self.device = torch.device("cuda" if use_cuda else "cpu")
        self.train_loader, self.test_loader = get_data_loaders()
        self.model = ConvNet().to(self.device)
        self.optimizer = optim.SGD(
            self.model.parameters(),
            lr=config.get("lr", 0.01),
            momentum=config.get("momentum", 0.9))

    def _train(self):
        train(
            self.model, self.optimizer, self.train_loader, device=self.device)
        acc = test(self.model, self.test_loader, self.device)
        return {"mean_accuracy": acc}

    def _save(self, checkpoint_dir):
        checkpoint_path = os.path.join(checkpoint_dir, "model.pth")
        torch.save(self.model.state_dict(), checkpoint_path)
        return checkpoint_path

    def _restore(self, checkpoint_path):
        self.model.load_state_dict(torch.load(checkpoint_path))
# __trainable_example_end__
# yapf: enable

# __trainable_run_begin__
search_space = {
    "lr": tune.sample_from(lambda spec: 10**(-10 * np.random.rand())),
    "momentum": tune.uniform(0.1, 0.9)
}

analysis = tune.run(TrainMNIST, num_samples=10, config=search_space)
# __trainable_run_end__
