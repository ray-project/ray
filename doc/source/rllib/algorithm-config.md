---
myst:
  html_meta:
    description: "Reference for the AlgorithmConfig API: type-safe configuration of training, environment, learner, and framework settings for any RLlib Algorithm."
---

(rllib-algo-configuration-docs)=

# AlgorithmConfig API

RLlib's {py:class}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` API is the auto-validated, type-safe way to configure and build an RLlib {py:class}`~ray.rllib.algorithms.algorithm.Algorithm`.

First create an instance of {py:class}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`, then call its methods to set configuration options. RLlib uses the following [black](https://github.com/psf/black)-compliant format in all parts of its code.

You can chain together more than one method call, including the constructor:

```{testcode}
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

config = (
    # Create an `AlgorithmConfig` instance.
    AlgorithmConfig()
    # Change the learning rate.
    .training(lr=0.0005)
    # Change the number of Learner actors.
    .learners(num_learners=2)
)
```

:::{hint}
To preserve value checking and type safety, never set attributes on your {py:class}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` directly. Always go through the proper methods:

```{testcode}
# WRONG!
config.env = "CartPole-v1"  # <- don't set attributes directly

# CORRECT!
config.environment(env="CartPole-v1")  # call the proper method
```
:::


## Algorithm specific config classes

You don't use the base `AlgorithmConfig` class directly in practice, but always its algorithm-specific subclasses, such as {py:class}`~ray.rllib.algorithms.ppo.ppo.PPOConfig`. Each subclass comes with its own set of additional arguments to the {py:meth}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.training` method.

Normally, pick the specific {py:class}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` subclass that matches the {py:class}`~ray.rllib.algorithms.algorithm.Algorithm` you want to run your learning experiments with. For example, to use {ref}`IMPALA <impala>` as your algorithm, import its specific config class:

```{testcode}
from ray.rllib.algorithms.impala import IMPALAConfig

config = (
    # Create an `IMPALAConfig` instance.
    IMPALAConfig()
    # Specify the RL environment.
    .environment("CartPole-v1")
    # Change the learning rate.
    .training(lr=0.0004)
)
```

To change an algorithm-specific setting, here for `IMPALA`, also use the {py:meth}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.training` method:

```{testcode}
# Change an IMPALA-specific setting (the entropy coefficient).
config.training(entropy_coeff=0.01)
```


You can build the {py:class}`~ray.rllib.algorithms.impala.IMPALA` instance directly from the config object by calling the {py:meth}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.build_algo` method:

```{testcode}
# Build the algorithm instance.
impala = config.build_algo()
```

```{testcode}
:hide:

impala.stop()
```

The config object stored inside any built {py:class}`~ray.rllib.algorithms.algorithm.Algorithm` instance is a copy of your original config. You can further alter your original config object and build another algorithm instance without affecting the one you already built:

```{testcode}
# Further alter the config without affecting the previously built IMPALA object ...
config.training(lr=0.00123)
# ... and build a new IMPALA from it.
another_impala = config.build_algo()
```

```{testcode}
:hide:

another_impala.stop()
```

If you are working with [Ray Tune](https://docs.ray.io/en/latest/tune/index.html), pass your {py:class}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` instance into the constructor of the {py:class}`~ray.tune.tuner.Tuner`:

```python
from ray import tune

tuner = tune.Tuner(
    "IMPALA",
    param_space=config,  # <- your RLlib AlgorithmConfig object
    ..
)
# Run the experiment with Ray Tune.
results = tuner.fit()
```


(rllib-algo-configuration-generic-settings)=

## Generic config settings

Most config settings are generic and apply to all of RLlib's {py:class}`~ray.rllib.algorithms.algorithm.Algorithm` classes. The following sections walk you through the most important settings to understand before you explore other config settings and start hyperparameter tuning.

### RL environment

To configure which {ref}`RL environment <rllib-environments-doc>` your algorithm trains against, use the `env` argument to the {py:meth}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.environment` method:

```{testcode}
config.environment("Humanoid-v5")
```

For more details, see the {ref}`RL environment guide <rllib-environments-doc>`.

:::{tip}
Install both [Atari](https://ale.farama.org/environments/) and [MuJoCo](https://gymnasium.farama.org/environments/mujoco) to run all of RLlib's {ref}`tuned examples <rllib-tuned-examples-docs>`:

```bash
pip install "gymnasium[atari,accept-rom-license,mujoco]"
```
:::

### Learning rate `lr`

Set the learning rate for updating your models through the `lr` argument to the {py:meth}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.training` method:

```{testcode}
config.training(lr=0.0001)
```

(rllib-algo-configuration-train-batch-size)=

### Train batch size

Set the train batch size, per Learner actor, through the `train_batch_size_per_learner` argument to the {py:meth}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.training` method:

```{testcode}
config.training(train_batch_size_per_learner=256)
```

:::{note}
Compute the total, effective train batch size by multiplying `train_batch_size_per_learner` with `(num_learners or 1)`. Or check the value of your config's {py:attr}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.total_train_batch_size` property:

```{testcode}
config.training(train_batch_size_per_learner=256)
config.learners(num_learners=2)
print(config.total_train_batch_size)  # expect: 512 = 256 * 2
```
:::


### Discount factor `gamma`

Set the [RL discount factor](https://www.envisioning.io/vocab/discount-factor?utm_source=chatgpt.com) through the `gamma` argument to the {py:meth}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.training` method:

```{testcode}
config.training(gamma=0.995)
```

### Scaling with `num_env_runners` and `num_learners`

% todo (sven): link to scaling guide, once separated out in its own rst.

Set the number of {py:class}`~ray.rllib.env.env_runner.EnvRunner` actors used to collect training samples through the `num_env_runners` argument to the {py:meth}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.env_runners` method:

```{testcode}
config.env_runners(num_env_runners=4)

# Also use `num_envs_per_env_runner` to vectorize your environment on each EnvRunner actor.
# Note that this option is only available in single-agent setups.
#  The Ray Team is working on a solution for this restriction.
config.env_runners(num_envs_per_env_runner=10)
```

Set the number of {py:class}`~ray.rllib.core.learner.learner.Learner` actors used to update your models through the `num_learners` argument to the {py:meth}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.learners` method. This should correspond to the number of GPUs you have available for training.

```{testcode}
config.learners(num_learners=2)
```

### Disable `explore` behavior

Switch exploratory behavior on or off through the `explore` argument to the {py:meth}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.env_runners` method. To compute actions, the {py:class}`~ray.rllib.env.env_runner.EnvRunner` calls `forward_exploration()` on the RLModule when `explore=True` and `forward_inference()` when `explore=False`. The default value is `explore=True`.

```{testcode}
# Disable exploration behavior.
# When False, the EnvRunner calls `forward_inference()` on the RLModule to compute
# actions instead of `forward_exploration()`.
config.env_runners(explore=False)
```

### Rollout length

The `rollout_fragment_length` argument sets the number of timesteps that each {py:class}`~ray.rllib.env.env_runner.EnvRunner` steps through with each of its RL environment copies. Pass this argument to the {py:meth}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.env_runners` method. Some algorithms, such as {py:class}`~ray.rllib.algorithms.ppo.PPO`, set this value automatically, based on the {ref}`train batch size <rllib-algo-configuration-train-batch-size>`, number of {py:class}`~ray.rllib.env.env_runner.EnvRunner` actors, and number of envs per {py:class}`~ray.rllib.env.env_runner.EnvRunner`.

```{testcode}
config.env_runners(rollout_fragment_length=50)
```

### All available methods and their settings

Besides the most common settings described earlier, the {py:class}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` class and its algo-specific subclasses come with many more configuration options.

{py:class}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` groups its config settings into the following categories, each represented by its own method:

- {ref}`Config settings for the RL environment <rllib-config-env>`
- {ref}`Config settings for training behavior (including algo-specific settings) <rllib-config-training>`
- {ref}`Config settings for EnvRunners <rllib-config-env-runners>`
- {ref}`Config settings for Learners <rllib-config-learners>`
- {ref}`Config settings for adding callbacks <rllib-config-callbacks>`
- {ref}`Config settings for multi-agent setups <rllib-config-multi_agent>`
- {ref}`Config settings for offline RL <rllib-config-offline_data>`
- {ref}`Config settings for evaluating policies <rllib-config-evaluation>`
- {ref}`Config settings for the DL framework <rllib-config-framework>`
- {ref}`Config settings for reporting and logging behavior <rllib-config-reporting>`
- {ref}`Config settings for checkpointing <rllib-config-checkpointing>`
- {ref}`Config settings for debugging <rllib-config-debugging>`
- {ref}`Experimental config settings <rllib-config-experimental>`

To familiarize yourself with RLlib's many config options, browse [RLlib's examples folder](https://github.com/ray-project/ray/tree/master/rllib/examples) or see the {ref}`examples folder overview page <rllib-examples-overview-docs>`.

Each example script usually introduces a config setting or shows you how to implement a customization by combining config options with custom code in your experiment.
