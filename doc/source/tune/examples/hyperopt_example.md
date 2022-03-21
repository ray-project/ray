---
jupytext:
    text_representation:
        extension: .md
        format_name: myst
kernelspec:
    display_name: Python 3
    language: python
    name: python3
orphan: true
---

# Running Tune experiments with HyperOpt

This example demonstrates the usage of HyperOpt with Ray Tune, using a `AsyncHyperBandScheduler` scheduler
together with `HyperOptSearch`.
Click below to see all the imports we need for this example.
You can also launch directly into a Binder instance to run this notebook yourself.
Just click on the rocket symbol at the top of the navigation.

```{code-cell} python3
:tags: [hide-input]
import time

import ray
from ray import tune
from ray.tune.suggest import ConcurrencyLimiter
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest.hyperopt import HyperOptSearch
```

Let's start by defining a simple evaluation function.
We artificially sleep for a bit (`0.1` seconds) to simulate a long-running ML experiment.
This setup assumes that we're running multiple `step`s of an experiment and try to tune two hyperparameters, namely `x` and `y`.

```{code-cell} python3
def evaluate(step, x, y):
    time.sleep(0.1)
    return (0.1 + x * step / 100) ** (-1) + y * 0.1
```

Next, our ``objective`` function takes a Tune ``config``, evaluates the `score` of your experiment in a training loop,
and uses `tune.report` to report the `score` back to Tune.

```{code-cell} python3
def objective(config):
    for step in range(config["steps"]):
        score = evaluate(step, config["x"], config["y"])
        tune.report(iterations=step, mean_loss=score)
```

```{code-cell} python3
:tags: [remove-cell]
ray.init(configure_logging=False)
```

Let's say we have a hypothesis on what the best parameters currently are (`current_best_params`), then we can
pass this belief into a `HyperOptSearch` searcher and set the maximum concurrent trials to `4` with a `ConcurrencyLimiter`.
We can also define a `scheduler` to go along with our algorithm and set the number of samples for this Tune run to `1000`
(your can decrease this if it takes too long on your machine).


```{code-cell} python3
current_best_params = [
    {"x": 1, "y": 2},
    {"x": 4, "y": 2},
]
searcher = HyperOptSearch(points_to_evaluate=current_best_params)

algo = ConcurrencyLimiter(searcher, max_concurrent=4)
scheduler = AsyncHyperBandScheduler()

num_samples = 1000
```

```{code-cell} python3
:tags: [remove-cell]
# If 1000 samples take too long, you can reduce this number.
# We override this number here for our smoke tests.
num_samples = 10
```

Finally, all that's left is to define a search space, run the experiment and print the best parameters:

```{code-cell} python3
search_space = {
    "steps": 100,
    "x": tune.uniform(0, 20),
    "y": tune.uniform(-100, 100),
}

analysis = tune.run(
    easy_objective,
    search_alg=algo,
    scheduler=scheduler,
    metric="mean_loss",
    mode="min",
    num_samples=num_samples,
    config=search_space,
)

print("Best hyperparameters found were: ", analysis.best_config)
```


```{code-cell} python3
:tags: [remove-cell]
ray.shutdown()
```