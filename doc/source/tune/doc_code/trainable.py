# flake8: noqa

# fmt: off
# __example_objective_start__
def objective(x, a, b):
    return a * (x ** 0.5) + b
# __example_objective_end__
# fmt: on

# __function_api_report_intermediate_metrics_start__
from ray import train, tune


def trainable(config: dict):
    intermediate_score = 0
    for x in range(20):
        intermediate_score = objective(x, config["a"], config["b"])
        train.report({"score": intermediate_score})  # This sends the score to Tune.


tuner = tune.Tuner(trainable, param_space={"a": 2, "b": 4})
results = tuner.fit()
# __function_api_report_intermediate_metrics_end__

# __function_api_report_final_metrics_start__
from ray import train, tune


def trainable(config: dict):
    final_score = 0
    for x in range(20):
        final_score = objective(x, config["a"], config["b"])

    train.report({"score": final_score})  # This sends the score to Tune.


tuner = tune.Tuner(trainable, param_space={"a": 2, "b": 4})
results = tuner.fit()
# __function_api_report_final_metrics_end__

# fmt: off
# __function_api_return_final_metrics_start__
def trainable(config: dict):
    final_score = 0
    for x in range(20):
        final_score = objective(x, config["a"], config["b"])

    return {"score": final_score}  # This sends the score to Tune.
# __function_api_return_final_metrics_end__
# fmt: on

# __class_api_example_start__
from ray import train, tune


class Trainable(tune.Trainable):
    def setup(self, config: dict):
        # config (dict): A dict of hyperparameters
        self.x = 0
        self.a = config["a"]
        self.b = config["b"]

    def step(self):  # This is called iteratively.
        score = objective(self.x, self.a, self.b)
        self.x += 1
        return {"score": score}


tuner = tune.Tuner(
    Trainable,
    run_config=train.RunConfig(
        # Train for 20 steps
        stop={"training_iteration": 20},
        checkpoint_config=train.CheckpointConfig(
            # We haven't implemented checkpointing yet. See below!
            checkpoint_at_end=False
        ),
    ),
    param_space={"a": 2, "b": 4},
)
results = tuner.fit()
# __class_api_example_end__
