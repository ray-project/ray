# flake8: noqa

# __function_api_start__
from ray import tune


def objective(x, a, b):  # Define an objective function.
    return a * (x ** 0.5) + b


def trainable(config):  # Pass a "config" dictionary into your trainable.

    for x in range(20):  # "Train" for 20 iterations and compute intermediate scores.
        score = objective(x, config["a"], config["b"])

        tune.report(score=score)  # Send the score to Tune.


# __function_api_end__


# __class_api_start__
from ray import tune


def objective(x, a, b):
    return a * (x ** 2) + b


class Trainable(tune.Trainable):
    def setup(self, config):
        # config (dict): A dict of hyperparameters
        self.x = 0
        self.a = config["a"]
        self.b = config["b"]

    def step(self):  # This is called iteratively.
        score = objective(self.x, self.a, self.b)
        self.x += 1
        return {"score": score}

    # __class_api_end__

    # TODO: this example does not work as advertised. Errors out.
    def save_checkpoint(self, checkpoint_dir):
        pass

    def load_checkpoint(self, checkpoint_dir):
        pass


# __run_tunable_start__
# Pass in a Trainable class or function, along with a search space "config".
tune.run(trainable, config={"a": 2, "b": 4})
# __run_tunable_end__

# __run_tunable_samples_start__
tune.run(trainable, config={"a": 2, "b": 4}, num_samples=10)
# __run_tunable_samples_end__

# __search_space_start__
space = {"a": tune.uniform(0, 1), "b": tune.uniform(0, 1)}
tune.run(trainable, config=space, num_samples=10)
# __search_space_end__

# __config_start__
config = {
    "uniform": tune.uniform(-5, -1),  # Uniform float between -5 and -1
    "quniform": tune.quniform(3.2, 5.4, 0.2),  # Round to increments of 0.2
    "loguniform": tune.loguniform(1e-4, 1e-1),  # Uniform float in log space
    "qloguniform": tune.qloguniform(1e-4, 1e-1, 5e-5),  # Round to increments of 0.00005
    "randn": tune.randn(10, 2),  # Normal distribution with mean 10 and sd 2
    "qrandn": tune.qrandn(10, 2, 0.2),  # Round to increments of 0.2
    "randint": tune.randint(-9, 15),  # Random integer between -9 and 15
    "qrandint": tune.qrandint(-21, 12, 3),  # Round to increments of 3 (includes 12)
    "lograndint": tune.lograndint(1, 10),  # Random integer in log space
    "qlograndint": tune.qlograndint(1, 10, 2),  # Round to increments of 2
    "choice": tune.choice(["a", "b", "c"]),  # Choose one of these options uniformly
    "func": tune.sample_from(
        lambda spec: spec.config.uniform * 0.01
    ),  # Depends on other value
    "grid": tune.grid_search([32, 64, 128]),  # Search over all these values
}
# __config_end__

# __bayes_start__
from ray.tune.suggest.bayesopt import BayesOptSearch

# Define the search space
search_space = {"a": tune.uniform(0, 1), "b": tune.uniform(0, 20)}

algo = BayesOptSearch(random_search_steps=4)

tune.run(
    trainable,
    config=search_space,
    metric="score",
    mode="min",
    search_alg=algo,
    stop={"training_iteration": 20},
)
# __bayes_end__

# __hyperband_start__
from ray.tune.schedulers import HyperBandScheduler

# Create HyperBand scheduler and minimize the score
hyperband = HyperBandScheduler(metric="score", mode="max")

config = {"a": tune.uniform(0, 1), "b": tune.uniform(0, 1)}

tune.run(trainable, config=config, num_samples=20, scheduler=hyperband)
# __hyperband_end__

# __analysis_start__
analysis = tune.run(
    trainable,
    config=config,
    metric="score",
    mode="min",
    search_alg=BayesOptSearch(random_search_steps=4),
    stop={"training_iteration": 20},
)

best_trial = analysis.best_trial  # Get best trial
best_config = analysis.best_config  # Get best trial's hyperparameters
best_logdir = analysis.best_logdir  # Get best trial's logdir
best_checkpoint = analysis.best_checkpoint  # Get best trial's best checkpoint
best_result = analysis.best_result  # Get best trial's last results
best_result_df = analysis.best_result_df  # Get best result as pandas dataframe
# __analysis_end__

# __results_start__
# Get a dataframe with the last results for each trial
df_results = analysis.results_df

# Get a dataframe of results for a specific score or mode
df = analysis.dataframe(metric="score", mode="max")
# __results_end__
