---
myst:
  html_meta:
    description: "Scale RLlib training by tuning EnvRunner actor count, environments per runner, and Learner actor count for higher sampling and learning throughput."
---

(rllib-scaling-guide)=

# RLlib scaling guide

RLlib is a distributed and scalable RL library, based on [Ray](https://www.ray.io/). An RLlib {py:class}`~ray.rllib.algorithms.algorithm.Algorithm` uses [Ray actors](https://docs.ray.io/en/latest/ray-core/actors.html) wherever parallelization of its sub-components can speed up sample and learning throughput.

```{figure} images/scaling_axes_overview.svg
:width: 600
:align: left

**Scalable axes in RLlib**: All RLlib {py:class}`~ray.rllib.algorithms.algorithm.Algorithm` classes provide three scaling axes:

- The number of {py:class}`~ray.rllib.env.env_runner.EnvRunner` actors in the {py:class}`~ray.rllib.env.env_runner_group.EnvRunnerGroup`,
  settable through `config.env_runners(num_env_runners=n)`.

- The number of vectorized sub-environments on each
  {py:class}`~ray.rllib.env.env_runner.EnvRunner` actor, settable through `config.env_runners(num_envs_per_env_runner=p)`.

- The number of {py:class}`~ray.rllib.core.learner.learner.Learner` actors in the
  {py:class}`~ray.rllib.core.learner.learner_group.LearnerGroup`, settable through `config.learners(num_learners=m)`.
```

## Scaling the number of EnvRunner actors

Control the degree of parallelism of the sampling machinery of the {py:class}`~ray.rllib.algorithms.algorithm.Algorithm` by increasing the number of remote {py:class}`~ray.rllib.env.env_runner.EnvRunner` actors in the {py:class}`~ray.rllib.env.env_runner_group.EnvRunnerGroup` through the config:

```{testcode}
from ray.rllib.algorithms.ppo import PPOConfig

config = (
    PPOConfig()
    # Use 4 EnvRunner actors (default is 2).
    .env_runners(num_env_runners=4)
)
```

To assign resources to each {py:class}`~ray.rllib.env.env_runner.EnvRunner`, use these config settings:

```python
config.env_runners(
    num_cpus_per_env_runner=..,
    num_gpus_per_env_runner=..,
)
```

See this [example of an EnvRunner and RL environment requiring a GPU resource](https://github.com/ray-project/ray/blob/master/rllib/examples/gpus/gpus_on_env_runners.py).

The number of GPUs can be fractional, for example 0.5, to allocate only a fraction of a GPU per {py:class}`~ray.rllib.env.env_runner.EnvRunner`.

There's always one "local" {py:class}`~ray.rllib.env.env_runner.EnvRunner` in the {py:class}`~ray.rllib.env.env_runner_group.EnvRunnerGroup`. To sample using only this local {py:class}`~ray.rllib.env.env_runner.EnvRunner`, set `num_env_runners=0`. This local {py:class}`~ray.rllib.env.env_runner.EnvRunner` sits directly in the main {py:class}`~ray.rllib.algorithms.algorithm.Algorithm` process.

:::{hint}
The Ray team might deprecate the local {py:class}`~ray.rllib.env.env_runner.EnvRunner`. It exists for historical reasons, and whether to keep it in the set is under debate.
:::

## Scaling the number of envs per EnvRunner actor

RLlib vectorizes {ref}`RL environments <rllib-key-concepts-environments>` on {py:class}`~ray.rllib.env.env_runner.EnvRunner` actors through [gymnasium's VectorEnv](https://gymnasium.farama.org/api/vector/) API. To create more than one environment copy per {py:class}`~ray.rllib.env.env_runner.EnvRunner`, set the following in your config:

```{testcode}
from ray.rllib.algorithms.ppo import PPOConfig

config = (
    PPOConfig()
    # Use 10 sub-environments (vector) per EnvRunner.
    .env_runners(num_envs_per_env_runner=10)
)
```

:::{note}
Unlike single-agent environments, RLlib can't vectorize multi-agent setups yet. The Ray team is working on a solution that uses the `gymnasium >= 1.x` custom vectorization feature.
:::

With more than one environment per {py:class}`~ray.rllib.env.env_runner.EnvRunner`, the {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule` runs inference on a batch of data and computes actions for all sub-environments in parallel.

By default, the individual sub-environments in a vector `step` and `reset` in sequence. Only the action computation of the RL environment loop runs in parallel, because observations can move through the model in a batch. However, [gymnasium](https://gymnasium.farama.org/) supports an asynchronous vectorization setting that gives each sub-environment its own Python process. The vector environment can then `step` or `reset` in parallel. Activate this asynchronous vectorization through:

```{testcode}
import gymnasium as gym

config.env_runners(
    gym_env_vectorize_mode=gym.envs.registration.VectorizeMode.ASYNC,  # default is `SYNC`
)
```

This setting can significantly speed up sampling when combined with `num_envs_per_env_runner > 1`, especially when your RL environment's stepping process is time-consuming.

See this [example script](https://github.com/ray-project/ray/blob/master/rllib/examples/envs/async_gym_env_vectorization.py) that demonstrates a large speedup with async vectorization.

## Scaling the number of Learner actors

Learning updates happen in the {py:class}`~ray.rllib.core.learner.learner_group.LearnerGroup`, which manages either a single local {py:class}`~ray.rllib.core.learner.learner.Learner` instance or any number of remote {py:class}`~ray.rllib.core.learner.learner.Learner` actors.

Set the number of remote {py:class}`~ray.rllib.core.learner.learner.Learner` actors through:

```{testcode}
from ray.rllib.algorithms.ppo import PPOConfig

config = (
    PPOConfig()
    # Use 2 remote Learner actors (default is 0) for distributed data parallelism.
    # Choosing 0 creates a local Learner instance on the main Algorithm process.
    .learners(num_learners=2)
)
```

Typically, you use as many {py:class}`~ray.rllib.core.learner.learner.Learner` actors as you have GPUs available for training. Set the number of GPUs per {py:class}`~ray.rllib.core.learner.learner.Learner` to 1:

```{testcode}
config.learners(num_gpus_per_learner=1)
```

:::{warning}
For some algorithms, such as IMPALA and APPO, the performance of a single remote {py:class}`~ray.rllib.core.learner.learner.Learner` actor with `num_learners=1` compared to a single local {py:class}`~ray.rllib.core.learner.learner.Learner` instance with `num_learners=0` depends on whether a GPU is available. With exactly one GPU, run these two algorithms with `num_learners=0, num_gpus_per_learner=1`. With no GPU, set `num_learners=1, num_gpus_per_learner=0`. With more than one GPU, set `num_learners=.., num_gpus_per_learner=1`.
:::

The number of GPUs can be fractional, for example 0.5, to allocate only a fraction of a GPU per {py:class}`~ray.rllib.env.env_runner.EnvRunner`. For example, pack five {py:class}`~ray.rllib.algorithms.algorithm.Algorithm` instances onto one GPU by setting `num_learners=1, num_gpus_per_learner=0.2`. See this [fractional GPU example](https://github.com/ray-project/ray/blob/master/rllib/examples/gpus/fractional_gpus_per_learner.py) for details.

:::{note}
If you specify `num_gpus_per_learner > 0` and your machine doesn't have enough GPUs, the experiment might stall until the Ray autoscaler brings up enough machines to fulfill the resource request. If your cluster has autoscaling turned off, this setting results in a seemingly hanging experiment run.

If you set `num_gpus_per_learner=0`, RLlib builds the {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule` instances on CPUs only, even if GPUs are available on the cluster.
:::

## Outlook: More RLlib elements that should scale

Other components and aspects of RLlib should also scale up.

For example, RLlib scales {py:class}`~ray.rllib.core.learner.learner.Learner` actors only through "distributed data parallel" (DDP), so the model size is limited to whatever fits on a single GPU.

The Ray team is working on closing these gaps. Future areas of improvement include:

- Enable training large models, such as a "large language model" (LLM). The team is working on a "Reinforcement Learning from Human Feedback" (RLHF) prototype setup. The main problems to solve are the model-parallel and tensor-parallel distribution across multiple GPUs, and a reasonably fast transfer of weights between Ray actors.

- Enable training with thousands of multi-agent policies. A possible solution for this scaling problem is to split the {py:class}`~ray.rllib.core.rl_module.multi_rl_module.MultiRLModule` into manageable groups of individual policies across the {py:class}`~ray.rllib.env.env_runner.EnvRunner` and {py:class}`~ray.rllib.core.learner.learner.Learner` actors.

- Enable vector envs for multi-agent.
