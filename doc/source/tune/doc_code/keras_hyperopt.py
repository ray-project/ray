# flake8: noqa

accuracy = 42

# __keras_hyperopt_start__
from ray import tune
from ray.tune.search.hyperopt import HyperOptSearch
import keras


# 1. Wrap a Keras model in an objective function.
def objective(config):
    model = keras.models.Sequential()
    model.add(keras.layers.Dense(784, activation=config["activation"]))
    model.add(keras.layers.Dense(10, activation="softmax"))

    model.compile(loss="binary_crossentropy", optimizer="adam", metrics=["accuracy"])
    # model.fit(...)
    # loss, accuracy = model.evaluate(...)
    return {"accuracy": accuracy}


# 2. Define a search space and initialize the search algorithm.
search_space = {"activation": tune.choice(["relu", "tanh"])}
algo = HyperOptSearch()

# 3. Start a Tune run that maximizes accuracy.
tuner = tune.Tuner(
    objective,
    tune_config=tune.TuneConfig(
        metric="accuracy",
        mode="max",
        search_alg=algo,
    ),
    param_space=search_space,
)
results = tuner.fit()
# __keras_hyperopt_end__
