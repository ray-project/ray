---
myst:
  html_meta:
    description: "Define Tune search spaces, including custom distributions and conditional spaces where one parameter's range depends on another."
---

(tune-search-space-tutorial)=

# Working with Tune search spaces

Ray Tune has a native interface for specifying search spaces. Specify the search space through `Tuner(param_space=...)`.

To run a grid search, use the `tune.grid_search` primitive:

```python
tuner = tune.Tuner(
    trainable,
    param_space={"bar": tune.grid_search([True, False])})
results = tuner.fit()
```

To specify distributions instead, use the random sampling primitives that {doc}`/tune/api/search_space` describes:

```python
tuner = tune.Tuner(
    trainable,
    param_space={
        "param1": tune.choice([True, False]),
        "bar": tune.uniform(0, 10),
        "alpha": tune.sample_from(lambda _: np.random.uniform(100) ** 2),
        "const": "hello"  # It is also ok to specify constant values.
    })
results = tuner.fit()
```

:::{caution}
If you use a search algorithm, you might not be able to specify lambdas or grid search with this interface, because some search algorithms might not be compatible with them.
:::

To sample multiple times or run multiple trials, set `num_samples=N` in `tune.TuneConfig`. If you provide `grid_search` as an argument, Tune repeats the *same* grid `N` times, as the following examples show:

```python
# 13 different configs.
tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=13), param_space={
    "x": tune.choice([0, 1, 2]),
    }
)
tuner.fit()

# 13 different configs.
tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=13), param_space={
    "x": tune.choice([0, 1, 2]),
    "y": tune.randn([0, 1, 2]),
    }
)
tuner.fit()

# 4 different configs.
tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=1), param_space={"x": tune.grid_search([1, 2, 3, 4])})
tuner.fit()

# 3 different configs.
tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=1), param_space={"x": tune.grid_search([1, 2, 3])})
tuner.fit()

# 6 different configs.
tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=2), param_space={"x": tune.grid_search([1, 2, 3])})
tuner.fit()

# 9 different configs.
tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=1), param_space={
    "x": tune.grid_search([1, 2, 3]),
    "y": tune.grid_search([a, b, c])}
)
tuner.fit()

# 18 different configs.
tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=2), param_space={
    "x": tune.grid_search([1, 2, 3]),
    "y": tune.grid_search([a, b, c])}
)
tuner.fit()

# 45 different configs.
tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=5), param_space={
    "x": tune.grid_search([1, 2, 3]),
    "y": tune.grid_search([a, b, c])}
)
tuner.fit()
```

Grid search and random search primitives are interoperable. You can use each one on its own or combine them, as in the following example:

```python
# 6 different configs.
tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=2), param_space={
    "x": tune.sample_from(...),
    "y": tune.grid_search([a, b, c])
    }
)
tuner.fit()
```

The following example uses `num_samples=10` to repeat the 3x3 grid search 10 times, for a total of 90 trials, each with randomly sampled values of `alpha` and `beta`:

```{code-block} python
:emphasize-lines: 12

 tuner = tune.Tuner(
     my_trainable,
     run_config=tune.RunConfig(name="my_trainable"),
     # num_samples will repeat the entire config 10 times.
     tune_config=tune.TuneConfig(num_samples=10),
     param_space={
         # ``sample_from`` creates a generator to call the lambda once per trial.
         "alpha": tune.sample_from(lambda _: np.random.uniform(100)),
         # ``sample_from`` also supports "conditional search spaces"
         "beta": tune.sample_from(lambda config: config["alpha"] * np.random.normal()),
         "nn_layers": [
             # tune.grid_search will make it so that all values are evaluated.
             tune.grid_search([16, 64, 256]),
             tune.grid_search([16, 64, 256]),
         ],
     },
 )
 tuner.fit()
```

:::{tip}
Avoid passing large objects as values in the search space, because that incurs a performance overhead. Use {func}`tune.with_parameters <ray.tune.with_parameters>` to pass large objects in, or load them inside your trainable from disk or cloud storage. If you load them from disk, make sure that all nodes can access the files. See {ref}`tune-bottlenecks` for more information.
:::

(tune_custom-search)=

## Use custom and conditional search spaces

You often run into awkward search spaces, such as when one hyperparameter depends on another. Use `tune.sample_from(func)` to provide a custom callable function that generates a search space.

The `func` parameter should take a `config` dictionary, which contains the values already sampled for the trial. Through `config`, the function reads other hyperparameters, which is useful for conditional distributions:

```python
tuner = tune.Tuner(
    ...,
    param_space={
        # A random function
        "alpha": tune.sample_from(lambda _: np.random.uniform(100)),
        # Use the `config` dict to access other hyperparameters
        "beta": tune.sample_from(lambda config: config["alpha"] * np.random.normal())
    }
)
tuner.fit()
```

To specify a conditional parameter distribution, reference another hyperparameter through `config` in the lambda function. The following example makes the value of `beta` depend on the value of `alpha` by referencing `config["alpha"]`. It combines a grid search over two nested parameters with random sampling from two lambda functions, generating nine different trials:

```{code-block} python
:emphasize-lines: 4-11

 tuner = tune.Tuner(
     my_trainable,
     run_config=RunConfig(name="my_trainable"),
     param_space={
         "alpha": tune.sample_from(lambda _: np.random.uniform(100)),
         "beta": tune.sample_from(lambda config: config["alpha"] * np.random.normal()),
         "nn_layers": [
             tune.grid_search([16, 64, 256]),
             tune.grid_search([16, 64, 256]),
         ],
     }
 )
```

:::{note}
Not every search algorithm supports this format. Only some search algorithms, such as {ref}`Hyperopt <tune-hyperopt>` and {ref}`Optuna <tune-optuna>`, handle conditional search spaces at all.

To use conditional search spaces with {ref}`Hyperopt <tune-hyperopt>`, you need a [Hyperopt search space](http://hyperopt.github.io/hyperopt/getting-started/search_spaces/). {ref}`Optuna <tune-optuna>` supports conditional search spaces through its define-by-run interface. See {doc}`/tune/examples/optuna-example`.
:::
