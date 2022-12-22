# flake8: noqa

# __function_api_start__
from ray.air import session


def objective(x, a, b):  # Define an objective function.
    return a * (x**0.5) + b


def trainable(config):  # Pass a "config" dictionary into your trainable.

    for x in range(20):  # "Train" for 20 iterations and compute intermediate scores.
        score = objective(x, config["a"], config["b"])

        session.report({"score": score})  # Send the score to Tune.


# __function_api_end__


# __class_api_start__
from ray import tune


def objective(x, a, b):
    return a * (x**2) + b


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
tuner = tune.Tuner(trainable, param_space={"a": 2, "b": 4})
tuner.fit()
# __run_tunable_end__

# __run_tunable_samples_start__
tuner = tune.Tuner(
    trainable, param_space={"a": 2, "b": 4}, tune_config=tune.TuneConfig(num_samples=10)
)
tuner.fit()
# __run_tunable_samples_end__

# __search_space_start__
space = {"a": tune.uniform(0, 1), "b": tune.uniform(0, 1)}
tuner = tune.Tuner(
    trainable, param_space=space, tune_config=tune.TuneConfig(num_samples=10)
)
tuner.fit()
# __search_space_end__

# __config_start__
config = {
    "uniform": tune.uniform(-5, -1),  # Uniform float between -5 and -1
    "quniform": tune.quniform(3.2, 5.4, 0.2),  # Round to multiples of 0.2
    "loguniform": tune.loguniform(1e-4, 1e-1),  # Uniform float in log space
    "qloguniform": tune.qloguniform(1e-4, 1e-1, 5e-5),  # Round to multiples of 0.00005
    "randn": tune.randn(10, 2),  # Normal distribution with mean 10 and sd 2
    "qrandn": tune.qrandn(10, 2, 0.2),  # Round to multiples of 0.2
    "randint": tune.randint(-9, 15),  # Random integer between -9 and 15
    "qrandint": tune.qrandint(-21, 12, 3),  # Round to multiples of 3 (includes 12)
    "lograndint": tune.lograndint(1, 10),  # Random integer in log space
    "qlograndint": tune.qlograndint(1, 10, 2),  # Round to multiples of 2
    "choice": tune.choice(["a", "b", "c"]),  # Choose one of these options uniformly
    "func": tune.sample_from(
        lambda spec: spec.config.uniform * 0.01
    ),  # Depends on other value
    "grid": tune.grid_search([32, 64, 128]),  # Search over all these values
}
# __config_end__

# __bayes_start__
from ray.tune.search.bayesopt import BayesOptSearch
from ray import air

# Define the search space
search_space = {"a": tune.uniform(0, 1), "b": tune.uniform(0, 20)}

algo = BayesOptSearch(random_search_steps=4)

tuner = tune.Tuner(
    trainable,
    tune_config=tune.TuneConfig(
        metric="score",
        mode="min",
        search_alg=algo,
    ),
    run_config=air.RunConfig(stop={"training_iteration": 20}),
    param_space=search_space,
)
tuner.fit()
# __bayes_end__

# __hyperband_start__
from ray.tune.schedulers import HyperBandScheduler

# Create HyperBand scheduler and minimize the score
hyperband = HyperBandScheduler(metric="score", mode="max")

config = {"a": tune.uniform(0, 1), "b": tune.uniform(0, 1)}

tuner = tune.Tuner(
    trainable,
    tune_config=tune.TuneConfig(
        num_samples=20,
        scheduler=hyperband,
    ),
    param_space=config,
)
tuner.fit()
# __hyperband_end__

# __analysis_start__
tuner = tune.Tuner(
    trainable,
    tune_config=tune.TuneConfig(
        metric="score",
        mode="min",
        search_alg=BayesOptSearch(random_search_steps=4),
    ),
    run_config=air.RunConfig(
        stop={"training_iteration": 20},
    ),
    param_space=config,
)
results = tuner.fit()

best_result = results.get_best_result()  # Get best result object
best_config = best_result.config  # Get best trial's hyperparameters
best_logdir = best_result.log_dir  # Get best trial's logdir
best_checkpoint = best_result.checkpoint  # Get best trial's best checkpoint
best_metrics = best_result.metrics  # Get best trial's last results
best_result_df = best_result.metrics_dataframe  # Get best result as pandas dataframe
# __analysis_end__

# __results_start__
# Get a dataframe with the last results for each trial
df_results = results.get_dataframe()

# Get a dataframe of results for a specific score or mode
df = results.get_dataframe(filter_metric="score", filter_mode="max")
# __results_end__
