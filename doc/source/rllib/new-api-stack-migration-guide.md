---
myst:
  html_meta:
    description: "Migrate from the old RLlib API stack to the new one: mapping ModelV2, RolloutWorker, and Policy to RLModule, Learner, and ConnectorV2."
---

(rllib-new-api-stack-migration-guide)=

```{testcode}
:hide:

from ray.rllib.algorithms.ppo import PPOConfig
config = PPOConfig()
```

# New API stack migration guide

This page explains how to convert your existing old API stack RLlib classes and code to RLlib's new API stack.


## What's the new API stack?

The new API stack rewrites the core RLlib APIs from scratch and reduces user-facing classes from more than a dozen critical ones to only a handful, without losing features. When designing these interfaces, the Ray team strictly applied the following principles:

* Classes must be usable outside of RLlib.
* Separation of concerns. Answer the question "what should get done, when, and by whom?" and give each class as few non-overlapping, well-defined tasks as possible.
* Offer fine-grained modularity, full interoperability, and frictionless pluggability of classes.
* Use widely accepted third-party standards and APIs wherever possible.

Applying the preceding principles, the Ray team reduced the must-know classes from eight on the old stack to only five on the new stack. The core new API stack classes are:

* {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule`, which replaces `ModelV2` and `PolicyMap` APIs.
* {py:class}`~ray.rllib.core.learner.learner.Learner`, which replaces `RolloutWorker` and some of `Policy`.
* {py:class}`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` and {py:class}`~ray.rllib.env.multi_agent_episode.MultiAgentEpisode`, which replace `ViewRequirement`, `SampleCollector`, `Episode`, and `EpisodeV2`.
* {py:class}`~ray.rllib.connector.connector_v2.ConnectorV2`, which replaces `Connector` and some of `RolloutWorker` and `Policy`.

The {py:class}`~ray.rllib.algorithm.algorithm_config.AlgorithmConfig` and {py:class}`~ray.rllib.algorithm.algorithm.Algorithm` APIs remain as-is. These classes are already established APIs on the old stack.


:::{note}
Even though the new API stack still provides rudimentary support for [TensorFlow](https://tensorflow.org), RLlib supports a single deep learning framework, [PyTorch](https://pytorch.org), and drops TensorFlow support entirely. The Ray team continues to design RLlib to be framework-agnostic and might add support for other frameworks.
:::


## Check your AlgorithmConfig

RLlib turns on the new API stack by default for all RLlib algorithms.

:::{note}
To deactivate the new API stack and switch back to the old one, use the `api_stack()` method in your `AlgorithmConfig` object as follows:

```{testcode}
config.api_stack(
    enable_rl_module_and_learner=False,
    enable_env_runner_and_connector_v2=False,
)
```
:::

There are a few other differences between configuring an old API stack algorithm and its new stack counterpart. Go through the following sections and translate the respective settings. Remove settings that the new stack doesn't support or need.


### AlgorithmConfig.framework()

Even though the new API stack still provides rudimentary support for [TensorFlow](https://tensorflow.org), RLlib supports a single deep learning framework, the [PyTorch](https://pytorch.org) framework.

The new API stack deprecates the following framework-related settings:

```{testcode}
# Make sure you always set the framework to "torch"...
config.framework("torch")

# ... and drop all tf-specific settings.
config.framework(
    eager_tracing=True,
    eager_max_retraces=20,
    tf_session_args={},
    local_tf_session_args={},
)
```


### AlgorithmConfig.resources()

The Ray team deprecated the `num_gpus` and `_fake_gpus` settings. To place your RLModule on one or more GPUs on the Learner side, do the following:

```{testcode}
# The following setting is equivalent to the old stack's `config.resources(num_gpus=2)`.
config.learners(
    num_learners=2,
    num_gpus_per_learner=1,
)
```

:::{hint}
The `num_learners` setting determines how many remote {py:class}`~ray.rllib.core.learner.learner.Learner` workers there are in your Algorithm's {py:class}`~ray.rllib.core.learner.learner_group.LearnerGroup`. If you set this parameter to `0`, your LearnerGroup only contains a local Learner that runs on the main process and shares its compute resources, typically 1 CPU. For asynchronous algorithms such as IMPALA or APPO, always set this parameter greater than 0.
:::

For an example of training with fractional GPUs, see the [fractional GPUs example script](https://github.com/ray-project/ray/blob/master/rllib/examples/gpus/fractional_gpus_per_learner.py). For fractional GPUs, always set `num_learners` to `0` or `1`.

If GPUs aren't available, but you want to learn with more than one {py:class}`~ray.rllib.core.learner.learner.Learner` in a multi-CPU fashion, do the following:

```{testcode}
config.learners(
    num_learners=2,  # or >2
    num_cpus_per_learner=1,  # <- default
    num_gpus_per_learner=0,  # <- default
)
```

The Ray team renamed the setting `num_cpus_for_local_worker` to `num_cpus_for_main_process`.

```{testcode}
config.resources(num_cpus_for_main_process=0)  # default is 1
```


### AlgorithmConfig.training()

#### Train batch size

Because of the new API stack's {py:class}`~ray.rllib.core.learner.learner.Learner` worker architecture, training might happen in a distributed fashion over `n` {py:class}`~ray.rllib.core.learner.learner.Learner` workers, so RLlib provides the train batch size per individual {py:class}`~ray.rllib.core.learner.learner.Learner`. Don't use the `train_batch_size` setting anymore:


```{testcode}
config.training(
    train_batch_size_per_learner=512,
)
```

You don't need to change this setting, even when increasing the number of {py:class}`~ray.rllib.core.learner.learner.Learner`, through `config.learners(num_learners=...)`.

A good rule of thumb for scaling on the learner axis is to keep the `train_batch_size_per_learner` value constant as the number of Learners grows and to increase the learning rate as follows:

`lr = [original_lr] * ([num_learners] ** 0.5)`


#### Neural network configuration

The old stack's `config.training(model=...)` is no longer supported on the new API stack. Instead, use the new {py:meth}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.rl_module` method to configure RLlib's default {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule` or specify and configure a custom {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule`.

See {ref}`RLModules API <rlmodule-guide>`, a general guide that also explains the use of the `config.rl_module()` method.

If you have an old stack `ModelV2` and want to migrate the entire NN logic to the new stack, see {ref}`ModelV2 to RLModule <rllib-modelv2-to-rlmodule>` for migration instructions.


#### Learning rate and coefficient schedules

If you're using schedules for learning rate or other coefficients, such as the `entropy_coeff` setting in PPO, provide the scheduling information directly in the respective setting. Scheduling behavior no longer requires a specific, separate setting.

When defining a schedule, provide a list of 2-tuples. The first item is the global timestep, and the second item is the value the learning rate should reach at that timestep. The reported metrics list this timestep as `num_env_steps_sampled_lifetime`. Always start the first 2-tuple with timestep 0. RLlib linearly interpolates values between two provided timesteps.

For example, to create a learning rate schedule that starts with a value of 1e-5, then increases over 1M timesteps to 1e-4 and stays constant after that, do the following:

```{testcode}
config.training(
    lr=[
        [0, 1e-5],  # <- initial value at timestep 0
        [1000000, 1e-4],  # <- final value at 1M timesteps
    ],
)
```


In the preceding example, the value after 500k timesteps is roughly `5e-5` from linear interpolation.

As another example, to create an entropy coefficient schedule that starts at 0.05, increases over 1M timesteps to 0.1, and then suddenly drops to 0 right after the 1Mth timestep, do the following:

```{testcode}
config.training(
    entropy_coeff=[
        [0, 0.05],  # <- initial value at timestep 0
        [1000000, 0.1],  # <- value at 1M timesteps
        [1000001, 0.0],  # <- sudden drop to 0.0 right after 1M timesteps
    ]
)
```

If you need to configure more complex learning rate scheduling behavior or chain different schedulers into a pipeline, use the experimental `_torch_lr_schedule_classes` config property. See [this example script](https://github.com/ray-project/ray/blob/master/rllib/examples/learners/ppo_with_torch_lr_schedulers.py). This example covers learning rate schedules only, not other coefficients.


### AlgorithmConfig.learners()

This method isn't used on the old API stack because the old stack doesn't use Learner workers.

Use this method to specify the following:

1. the number of `Learner` workers through `.learners(num_learners=...)`.
1. the resources per learner. Use `.learners(num_gpus_per_learner=1)` for GPU training and `.learners(num_gpus_per_learner=0)` for CPU training.
1. the custom Learner class you want to use. See this [custom loss function example script](https://github.com/ray-project/ray/blob/master/rllib/examples/learners/ppo_with_custom_loss_fn.py).
1. a config dict to set for your custom learner: `.learners(learner_config_dict={...})`. Every `Learner` can access the entire `AlgorithmConfig` object through `self.config`, but setting `learner_config_dict` is a convenient way to avoid creating an entirely new `AlgorithmConfig` subclass to support a few extra settings for your custom `Learner` class.


### AlgorithmConfig.env_runners()

```{testcode}
# RolloutWorkers have been replace by EnvRunners. EnvRunners are more efficient and offer
# a more separation-of-concerns design and cleaner code.
config.env_runners(
    num_env_runners=2,  # use this instead of `num_workers`
)

# The following `env_runners` settings are deprecated and should no longer be explicitly
# set on the new stack:
config.env_runners(
    create_env_on_local_worker=False,
    sample_collector=None,
    enable_connectors=True,
    remote_worker_envs=False,
    remote_env_batch_wait_ms=0,
    preprocessor_pref="deepmind",
    enable_tf1_exec_eagerly=False,
    sampler_perf_stats_ema_coef=None,
)
```

:::{hint}
If you want to IDE-debug what's happening inside your `EnvRunners`, set `num_env_runners=0` and run your experiment locally instead of through Ray Tune. To do this with any of RLlib's [example](https://github.com/ray-project/ray/tree/master/rllib/examples) or [tuned_example](https://github.com/ray-project/ray/tree/master/rllib/examples/algorithms) scripts, set the command-line arguments `--no-tune --num-env-runners=0`.
:::

If you use the `observation_filter` setting, perform the following translations:

```{testcode}
# For `observation_filter="NoFilter"`, don't set anything in particular. This is the default.

# For `observation_filter="MeanStdFilter"`, do the following:
from ray.rllib.connectors.env_to_module import MeanStdFilter

config.env_runners(
    env_to_module_connector=lambda env: MeanStdFilter(multi_agent=False),  # <- or True
)
```


:::{hint}
The main switch for whether to explore during sample collection has moved to the {py:meth}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.env_runners` method. For details, see {ref}`the exploration configuration <rllib-algo-config-exploration-docs>`.
:::


(rllib-algo-config-exploration-docs)=

### AlgorithmConfig.exploration()

The main switch for whether to explore during sample collection has moved from the deprecated `AlgorithmConfig.exploration()` method to {py:meth}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.env_runners`.

This setting determines which method your {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule` calls inside the {py:class}`~ray.rllib.env.env_runner.EnvRunner`. It calls {py:meth}`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration` when `explore=True` and {py:meth}`~ray.rllib.core.rl_module.rl_module.RLModule._forward_inference` when `explore=False`.

```{testcode}
config.env_runners(explore=True)  # <- or False
```


The Ray team deprecated the `exploration_config` setting. Instead, define the exact exploratory behavior, such as sampling an action from a distribution, inside the overridden {py:meth}`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration` method of your {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule`.


## Custom callbacks

If you're using custom callbacks on the old API stack, you're subclassing the `DefaultCallbacks` class, which the Ray team renamed to {py:class}`~ray.rllib.callbacks.callbacks.RLlibCallback`. You can continue this approach with the new API stack and pass your custom subclass to your config as follows:

```{testcode}
# config.callbacks(YourCallbacksClass)
```

However, if you're overriding methods that trigger on the {py:class}`~ray.rllib.env.env_runner.EnvRunner` side, such as `on_episode_start/stop/step/etc...`, you might have to translate some call arguments.

The following is a one-to-one translation guide for these types of {py:class}`~ray.rllib.callbacks.callbacks.RLlibCallback` methods:

```{testcode}
from ray.rllib.callbacks.callbacks import RLlibCallback

class YourCallbacksClass(RLlibCallback):

    def on_episode_start(
        self,
        *,
        episode,
        env_runner,
        metrics_logger,
        env,
        env_index,
        rl_module,

        # Old API stack args; don't use or access these inside your method code.
        worker=None,
        base_env=None,
        policies=None,
        **kwargs,
    ):
        # The `SingleAgentEpisode` or `MultiAgentEpisode` that RLlib has just started.
        # See https://docs.ray.io/en/latest/rllib/single-agent-episode.html for more details:
        print(episode)

        # The `EnvRunner` class that collects the episode in question.
        # This class used to be a `RolloutWorker`. On the new stack, this class is either a
        # `SingleAgentEnvRunner` or a `MultiAgentEnvRunner` holding the gymnasium Env,
        # the RLModule, and the 2 connector pipelines, env-to-module and module-to-env.
        print(env_runner)

        # The MetricsLogger object on the EnvRunner (documentation is a WIP).
        print(metrics_logger.peek("episode_return_mean", default=0.0))

        # The gymnasium env that sample collection uses. Note that this env may be a
        # gymnasium.vector.VectorEnv.
        print(env)

        # The env index, in case of a vector env, that handles the `episode`.
        print(env_index)

        # The RL Module that this EnvRunner uses. Note that this module may be a "plain", single-agent
        # `RLModule`, or a `MultiRLModule` in the multi-agent case.
        print(rl_module)

# Change similarly:
# on_episode_created()
# on_episode_step()
# on_episode_end()
```

The following callback methods are no longer available on the new API stack:

* `on_sub_environment_created()`: The new API stack uses [Farama's gymnasium](https://farama.org) vector Envs, which leave RLlib no way to call a callback when each individual env-index is created.
* `on_create_policy()`: This method is no longer available on the new API stack because only `RolloutWorker` calls it.
* `on_postprocess_trajectory()`: The new API stack no longer calls this method because {py:class}`~ray.rllib.connectors.connector_v2.ConnectorV2` pipelines handle trajectory processing entirely. The documentation for {py:class}`~ray.rllib.connectors.connector_v2.ConnectorV2` is under development.

% See :ref:`<rllib-callback-docs>` for a detailed description of RLlib callback APIs. % TODO (sven): ref doesn't work for some weird reason. Getting: undefined label: '<rllib-callback-docs>'

(rllib-modelv2-to-rlmodule)=

## ModelV2 to RLModule

If you're using a custom `ModelV2` class and want to translate the entire NN architecture and possibly action distribution logic to the new API stack, see {ref}`RL Modules <rlmodule-guide>` in addition to this section.

Also, see these example scripts on [how to write a custom CNN-containing RLModule](https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/custom_cnn_rl_module.py) and [how to write a custom LSTM-containing RLModule](https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/custom_lstm_rl_module.py).

There are various options for translating an existing, custom `ModelV2` from the old API stack, to the new API stack's {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule`:

1. Move your ModelV2 code to a new, custom `RLModule` class. See {ref}`RL Modules <rlmodule-guide>` for details.
1. Use an Algorithm checkpoint or a Policy checkpoint from an old API stack training run with the [new stack RLModule convenience wrapper](https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/migrate_modelv2_to_new_api_stack_by_policy_checkpoint.py).
1. Use an existing {py:class}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` object from an old API stack training run with the [new stack RLModule convenience wrapper](https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/migrate_modelv2_to_new_api_stack_by_config.py).

In more complex scenarios, you might have implemented custom policies to modify how models and distributions are constructed.


### Translating Policy.compute_actions_from_input_dict

This old API stack method, as well as `compute_actions` and `compute_single_action`, directly translate to {py:meth}`~ray.rllib.core.rl_module.rl_module.RLModule._forward_inference` and {py:meth}`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration`. {ref}`The RLModule guide explains how to implement this method <rlmodule-guide>`.


### Translating Policy.action_distribution_fn

To translate `action_distribution_fn`, write the following custom RLModule code:

::::{tab-set}

:::{tab-item} Same action dist. class

```{testcode}
:skipif: True

from ray.rllib.models.torch.torch_distributions import YOUR_DIST_CLASS


class MyRLModule(TorchRLModule):
    def setup(self):
        ...
        # Set the following attribute at the end of your custom `setup()`.
        self.action_dist_cls = YOUR_DIST_CLASS
```
:::

:::{tab-item} Different action dist. classes

```{testcode}
:skipif: True

from ray.rllib.models.torch.torch_distributions import (
    YOUR_INFERENCE_DIST_CLASS,
    YOUR_EXPLORATION_DIST_CLASS,
    YOUR_TRAIN_DIST_CLASS,
)

        def get_inference_action_dist_cls(self):
            return YOUR_INFERENCE_DIST_CLASS

        def get_exploration_action_dist_cls(self):
            return YOUR_EXPLORATION_DIST_CLASS

        def get_train_action_dist_cls(self):
            return YOUR_TRAIN_DIST_CLASS
```
:::

::::


### Translating Policy.action_sampler_fn

To translate `action_sampler_fn`, write the following custom RLModule code:

```{testcode}
:skipif: True

from ray.rllib.models.torch.torch_distributions import YOUR_DIST_CLASS


class MyRLModule(TorchRLModule):

    def _forward_exploration(self, batch):
        computation_results = ...
        my_dist = YOUR_DIST_CLASS(computation_results)
        actions = my_dist.sample()
        return {Columns.ACTIONS: actions}

    # Maybe for inference, you would like to sample from the deterministic version
    # of your distribution:
    def _forward_inference(self, batch):
        computation_results = ...
        my_dist = YOUR_DIST_CLASS(computation_results)
        greedy_actions = my_dist.to_deterministic().sample()
        return {Columns.ACTIONS: greedy_actions}
```


### Policy.compute_log_likelihoods

Implement your custom RLModule's {py:meth}`~ray.rllib.core.rl_module.rl_module.RLModule._forward_train` method and return the `Columns.ACTION_LOGP` key together with the corresponding action log probabilities to pass this information to your loss functions, which your code calls after `forward_train()`. The loss logic can then access `Columns.ACTION_LOGP`.


## Custom loss functions and policies

If you're using one or more custom loss functions or custom PyTorch optimizers to train your models, move the logic into the new API stack's {py:class}`~ray.rllib.core.learner.learner.Learner` class instead of customizing inside the old stack's Policy class.

See {ref}`Learner <learner-guide>` for details on how to write a custom Learner.

The following example scripts show how to write:

- [a custom loss function](https://github.com/ray-project/ray/blob/master/rllib/examples/learners/ppo_with_custom_loss_fn.py)
- [a custom Learner with two optimizers, each with a different learning rate](https://github.com/ray-project/ray/blob/master/rllib/examples/learners/separate_vf_lr_and_optimizer.py)

The new API stack doesn't support the Policy class. On the old stack, this class holds a neural network, a connector, and one or more optimizers and losses. On the new stack, the {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule` replaces the neural network, the {py:class}`~ray.rllib.connector.connector_v2.ConnectorV2` replaces the connector, and the {py:class}`~ray.rllib.core.learner.learner.Learner` class replaces the optimizers and losses.

The RLModule API is more flexible than the old stack's Policy API and provides a cleaner separation of concerns. Action inference runs on the EnvRunners, and updating runs on the Learner workers. It also scales better, supporting multi-GPU training on any Ray cluster and multi-node, multi-GPU training on the [Anyscale](https://anyscale.com) platform.


## Custom connectors

If you're using custom connectors from the old API stack, move your logic into the new {py:class}`~ray.rllib.connectors.connector_v2.ConnectorV2` API. Translate your agent connectors into env-to-module ConnectorV2 pieces and your action connectors into module-to-env ConnectorV2 pieces.

The {py:class}`~ray.rllib.connectors.connector_v2.ConnectorV2` documentation is under development.

The following examples show how to write ConnectorV2 pieces for the different pipelines:

1. [Observation frame-stacking](https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/frame_stacking.py).
1. [Add the most recent action and reward to the RLModule's input](https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/prev_actions_prev_rewards.py).
1. [Mean-std filtering on all observations](https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/mean_std_filtering.py).
1. [Flatten any complex observation space to a 1D space](https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/flatten_observations_dict_space.py).
