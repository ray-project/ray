---
myst:
  html_meta:
    description: "Industry-grade, scalable reinforcement learning with a unified API for single-agent, multi-agent, and offline RL training and deployment."
---

(rllib-index)=

# RLlib: Industry-grade, scalable reinforcement learning

```{image} images/rllib-logo.png
:align: center
```

<!-- todo (sven): redo toctree:
    suggestion:
    getting-started
    key-concepts
    rllib-env (single-agent)
        ...  <- multi-agent
        ...  <- external
        ...  <- hierarchical
    algorithm-configs
        rllib-algorithms (overview of all available algos)
    dev-guide (replaces user-guides)
        debugging
        scaling-guide
        fault-tolerance
        checkpoints
        callbacks
        metrics-logger
    rllib-advanced-api
        algorithm (general description of how algos work)
        rl-modules
        rllib-offline
        single-agent-episode
        multi-agent-episode
        connector-v2
        rllib-learner
        env-runners
    rllib-examples
    new-api-stack-migration-guide
    package_ref/index
-->

```{toctree}
:hidden:

getting-started
key-concepts
env
algorithm-config
algorithms
user-guides
examples
new-api-stack-migration-guide
```


RLlib is an open source library for reinforcement learning (RL). It supports production-grade, scalable, fault-tolerant RL workloads and keeps simple, unified APIs across a wide range of industry applications.

Whether you train policies in a multi-agent setup, from historic offline data, or with externally connected simulators, RLlib covers each of these autonomous decision-making cases, so you can start running experiments quickly.

Industry leaders use RLlib in production in many different verticals, such as [gaming](https://www.anyscale.com/events/2021/06/22/using-reinforcement-learning-to-optimize-iap-offer-recommendations-in-mobile-games), [robotics](https://www.anyscale.com/events/2021/06/23/introducing-amazon-sagemaker-kubeflow-reinforcement-learning-pipelines-for), [finance](https://www.anyscale.com/events/2021/06/22/a-24x-speedup-for-reinforcement-learning-with-rllib-+-ray), [climate and industrial control](https://www.anyscale.com/events/2021/06/23/applying-ray-and-rllib-to-real-life-industrial-use-cases), [manufacturing and logistics](https://www.anyscale.com/events/2022/03/29/alphadow-leveraging-rays-ecosystem-to-train-and-deploy-an-rl-industrial), [automobile](https://www.anyscale.com/events/2021/06/23/using-rllib-in-an-enterprise-scale-reinforcement-learning-solution), and [boat design](https://www.youtube.com/watch?v=cLCK13ryTpw).


## RLlib in 60 seconds

```{figure} images/rllib-index-header.svg
```

A few steps get your first RLlib workload running on your laptop. Install RLlib and [PyTorch](https://pytorch.org):

```bash
pip install "ray[rllib]" torch
```

:::{note}
To run the Atari or MuJoCo examples, install these additional packages:

```bash
pip install "gymnasium[atari,accept-rom-license,mujoco]"
```
:::

That's all you need to start coding against RLlib. This example runs the {ref}`PPO algorithm <ppo>` on the [Taxi domain](https://gymnasium.farama.org/environments/toy_text/taxi/). First, create a `config` for the algorithm. The config defines the {ref}`RL environment <rllib-key-concepts-environments>` and any other settings the algorithm needs.

```{testcode}
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module import FlattenObservations

# Configure the algorithm.
config = (
    PPOConfig()
    .environment("Taxi-v3")
    .env_runners(
        num_env_runners=2,
        # Observations are discrete (ints) -> We need to flatten (one-hot) them.
        env_to_module_connector=lambda env: FlattenObservations(),
    )
    .evaluation(evaluation_num_env_runners=1)
)
```


Next, `build` the algorithm and `train` it for two iterations. One training iteration includes parallel, distributed sample collection by the {py:class}`~ray.rllib.env.env_runner.EnvRunner` actors, followed by loss calculation on the collected data, and a model update step.

```{testcode}
from pprint import pprint

# Build the algorithm.
algo = config.build_algo()

# Train it for 2 iterations ...
for _ in range(2):
    pprint(algo.train())
```

At the end of your script, evaluate the trained algorithm and release its resources:

```{testcode}
# ... and evaluate it.
pprint(algo.evaluate())

# Release the algo's resources (remote actors, like EnvRunners and Learners).
algo.stop()
```


You can use any [Farama-Foundation Gymnasium](https://github.com/Farama-Foundation/Gymnasium) registered environment with the `env` argument.

In `config.env_runners()`, you can specify the number of parallel {py:class}`~ray.rllib.env.env_runner.EnvRunner` actors that collect samples from the environment, among many other settings.

You can also change the neural network architecture with RLlib's {py:class}`~ray.rllib.core.rl_module.default_model_cnofig.DefaultModelConfig`, and set up a separate config for the evaluation {py:class}`~ray.rllib.env.env_runner.EnvRunner` actors through the `config.evaluation()` method.

To learn more about the RLlib training APIs, see {ref}`the RLlib Python API <rllib-python-api>`. For an example of an action inference loop after training, see [this example script](https://github.com/ray-project/ray/blob/master/rllib/examples/inference/policy_inference_after_training.py).

For a quick preview of which algorithms and environments RLlib supports, expand the dropdowns below.

:::{dropdown} **RLlib Algorithms**
:animate: fade-in-slide-down

```{list-table}
:widths: 34 11 11 11 11 11 11

* - **On-Policy**
  -
  -
  -
  -
  -
  -
* - {ref}`PPO (Proximal Policy Optimization) <ppo>`
  - <img src="images/sigils/single-agent.svg" class="inline-figure" width="72" alt="single_agent">
  - <img src="images/sigils/multi-agent.svg" class="inline-figure" width="72" alt="multi_agent">
  - <img src="images/sigils/discr-actions.svg" class="inline-figure" width="72" alt="discr_act">
  - <img src="images/sigils/cont-actions.svg" class="inline-figure" width="72" alt="cont_act">
  - <img src="images/sigils/multi-gpu.svg" class="inline-figure" width="72" alt="multi_gpu">
  - <img src="images/sigils/multi-node-multi-gpu.svg" class="inline-figure" width="72" alt="Only on the Anyscale Platform!">
* - **Off-Policy**
  -
  -
  -
  -
  -
  -
* - {ref}`SAC (Soft Actor Critic) <sac>`
  - <img src="images/sigils/single-agent.svg" class="inline-figure" width="72" alt="single_agent">
  - <img src="images/sigils/multi-agent.svg" class="inline-figure" width="72" alt="multi_agent">
  -
  - <img src="images/sigils/cont-actions.svg" class="inline-figure" width="72" alt="cont_act">
  - <img src="images/sigils/multi-gpu.svg" class="inline-figure" width="72" alt="multi_gpu">
  - <img src="images/sigils/multi-node-multi-gpu.svg" class="inline-figure" width="72" alt="Only on the Anyscale Platform!">
* - {ref}`DQN/Rainbow (Deep Q Networks) <dqn>`
  - <img src="images/sigils/single-agent.svg" class="inline-figure" width="72" alt="single_agent">
  - <img src="images/sigils/multi-agent.svg" class="inline-figure" width="72" alt="multi_agent">
  - <img src="images/sigils/discr-actions.svg" class="inline-figure" width="72" alt="discr_act">
  -
  - <img src="images/sigils/multi-gpu.svg" class="inline-figure" width="72" alt="multi_gpu">
  - <img src="images/sigils/multi-node-multi-gpu.svg" class="inline-figure" width="72" alt="Only on the Anyscale Platform!">
* - **High-throughput Architectures**
  -
  -
  -
  -
  -
  -
* - {ref}`APPO (Asynchronous Proximal Policy Optimization) <appo>`
  - <img src="images/sigils/single-agent.svg" class="inline-figure" width="72" alt="single_agent">
  - <img src="images/sigils/multi-agent.svg" class="inline-figure" width="72" alt="multi_agent">
  - <img src="images/sigils/discr-actions.svg" class="inline-figure" width="72" alt="discr_act">
  - <img src="images/sigils/cont-actions.svg" class="inline-figure" width="72" alt="cont_act">
  - <img src="images/sigils/multi-gpu.svg" class="inline-figure" width="72" alt="multi_gpu">
  - <img src="images/sigils/multi-node-multi-gpu.svg" class="inline-figure" width="72" alt="Only on the Anyscale Platform!">
* - {ref}`IMPALA (Importance Weighted Actor-Learner Architecture) <impala>`
  - <img src="images/sigils/single-agent.svg" class="inline-figure" width="72" alt="single_agent">
  - <img src="images/sigils/multi-agent.svg" class="inline-figure" width="72" alt="multi_agent">
  - <img src="images/sigils/discr-actions.svg" class="inline-figure" width="72" alt="discr_act">
  -
  - <img src="images/sigils/multi-gpu.svg" class="inline-figure" width="72" alt="multi_gpu">
  - <img src="images/sigils/multi-node-multi-gpu.svg" class="inline-figure" width="72" alt="Only on the Anyscale Platform!">
* - **Model-based RL**
  -
  -
  -
  -
  -
  -
* - {ref}`DreamerV3 <dreamerv3>`
  - <img src="images/sigils/single-agent.svg" class="inline-figure" width="72" alt="single_agent">
  -
  - <img src="images/sigils/discr-actions.svg" class="inline-figure" width="72" alt="discr_act">
  - <img src="images/sigils/cont-actions.svg" class="inline-figure" width="72" alt="cont_act">
  - <img src="images/sigils/multi-gpu.svg" class="inline-figure" width="72" alt="multi_gpu">
  - <img src="images/sigils/multi-node-multi-gpu.svg" class="inline-figure" width="72" alt="Only on the Anyscale Platform!">
* - **Offline RL and Imitation Learning**
  -
  -
  -
  -
  -
  -
* - {ref}`BC (Behavior Cloning) <bc>`
  - <img src="images/sigils/single-agent.svg" class="inline-figure" width="72" alt="single_agent">
  -
  - <img src="images/sigils/discr-actions.svg" class="inline-figure" width="72" alt="discr_act">
  - <img src="images/sigils/cont-actions.svg" class="inline-figure" width="72" alt="cont_act">
  -
  -
* - {ref}`CQL (Conservative Q-Learning) <cql>`
  - <img src="images/sigils/single-agent.svg" class="inline-figure" width="72" alt="single_agent">
  -
  -
  - <img src="images/sigils/cont-actions.svg" class="inline-figure" width="72" alt="cont_act">
  -
  -
* - {ref}`MARWIL (Advantage Re-Weighted Imitation Learning) <marwil>`
  - <img src="images/sigils/single-agent.svg" class="inline-figure" width="72" alt="single_agent">
  -
  - <img src="images/sigils/discr-actions.svg" class="inline-figure" width="72" alt="discr_act">
  - <img src="images/sigils/cont-actions.svg" class="inline-figure" width="72" alt="cont_act">
  -
  -
```
:::


:::{dropdown} **RLlib Environments**
:animate: fade-in-slide-down

````{list-table}

* - **Farama-Foundation Environments**
* - [gymnasium](https://gymnasium.farama.org/index.html) <img src="images/sigils/single-agent.svg" class="inline-figure" width="72" alt="single_agent">

    ```bash
    pip install "gymnasium[atari,accept-rom-license,mujoco]"``
    ```

    ```python
    config.environment("CartPole-v1")  # Classic Control
    config.environment("ale_py:ALE/Pong-v5")  # Atari
    config.environment("Hopper-v5")  # MuJoCo
    ```
* - [PettingZoo](https://pettingzoo.farama.org/index.html) <img src="images/sigils/multi-agent.svg" class="inline-figure" width="72" alt="multi_agent">

    ```bash
    pip install "pettingzoo[all]"
    ```

    ```python
    from ray.tune.registry import register_env
    from ray.rllib.env.wrappers.pettingzoo_env import PettingZooEnv
    from pettingzoo.sisl import waterworld_v4
    register_env("env", lambda _: PettingZooEnv(waterworld_v4.env()))
    config.environment("env")
    ```
* - **RLlib Multi-Agent**
* - {ref}`RLlib's MultiAgentEnv API <rllib-multi-agent-environments-doc>` <img src="images/sigils/multi-agent.svg" class="inline-figure" width="72" alt="multi_agent">

    ```python
    from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
    from ray import tune
    tune.register_env("env", lambda cfg: MultiAgentCartPole(cfg))
    config.environment("env", env_config={"num_agents": 2})
    config.multi_agent(
        policies={"p0", "p1"},
        policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
    )
    ```
````
:::


## Why choose RLlib?

:::{dropdown} **Scalable and Fault-Tolerant**
:animate: fade-in-slide-down

RLlib workloads scale along two axes:

- The number of {py:class}`~ray.rllib.env.env_runner.EnvRunner` actors. Set this through `config.env_runners(num_env_runners=...)` to scale the speed of your simulator data collection step. This `EnvRunner` axis is fully fault tolerant. You can train against custom environments that are unstable or that frequently stall, and even place all your `EnvRunner` actors on spot machines.

- The number of {py:class}`~ray.rllib.core.learner.Learner` actors for multi-GPU training. Set this through `config.learners(num_learners=...)`. Normally you set it to the number of available GPUs, and also set `config.learners(num_gpus_per_learner=1)`. If you don't have GPUs, use this setting for DDP-style learning on CPUs instead.
:::

:::{dropdown} **Multi-Agent Reinforcement Learning (MARL)**
:animate: fade-in-slide-down

RLlib natively supports multi-agent reinforcement learning (MARL), so you can run any complex configuration.

- **Independent** multi-agent learning: every agent collects data to update its own policy network and treats other agents as part of the environment. This is the default.
- **Collaborative** training: train a team of agents that share one policy and its parameters, or give some agents their own policy networks. You can share value functions across the whole team or part of it, so you optimize global or local objectives.
- **Adversarial** training: have agents compete against each other. Use self-play, or league-based self-play, to train them through stages of increasing difficulty.
- **Any combination of the preceding.** You can train teams of any size against other teams, where the agents in each team have individual sub-objectives and neutral agents sit out the competition.
:::

:::{dropdown} **Offline RL and Behavior Cloning**
:animate: fade-in-slide-down

RLlib integrates Ray Data for large-scale data ingestion in offline RL and behavior cloning (BC) workloads.

See a basic [tuned behavior cloning example](https://github.com/ray-project/ray/blob/master/rllib/examples/algorithms/bc/cartpole_bc.py), or an example of [pre-training a policy with BC and fine-tuning it with online PPO](https://github.com/ray-project/ray/blob/master/rllib/examples/offline_rl/train_w_bc_finetune_w_ppo.py).
:::

:::{dropdown} **Support for External Env Clients**
:animate: fade-in-slide-down

RLlib supports externally connected RL environments by customizing the {py:class}`~ray.rllib.env.env_runner.EnvRunner` logic. Instead of RLlib-owned, internal Gymnasium environments, you can connect external, TCP-connected environments that act independently and can even run their own action inference, for example through ONNX.

For an example, see [RLlib acting as a server for external env TCP clients](https://github.com/ray-project/ray/blob/master/rllib/examples/envs/env_connecting_to_rllib_w_tcp_client.py).
:::


## Learn more

::::{grid} 1 2 3 3
:gutter: 1
:class-container: container pb-4

:::{grid-item-card}

**RLlib Key Concepts**
^^^
Learn the core concepts of RLlib, such as algorithms, environments, models, and learners.
+++
```{button-ref} rllib-key-concepts
:color: primary
:outline:
:expand:

Key Concepts
```
:::

:::{grid-item-card}

**RL Environments**
^^^
Get started with environments RLlib supports, such as the Farama Foundation's Gymnasium, PettingZoo, and custom formats for vectorized and multi-agent environments.
+++
```{button-ref} rllib-environments-doc
:color: primary
:outline:
:expand:

Environments
```
:::

:::{grid-item-card}

**Models (RLModule)**
^^^
Learn how to configure RLlib's default models and implement your own custom models through the RLModule APIs, which support arbitrary architectures with PyTorch, complex multi-model setups, and multi-agent models with components shared between agents.
+++
```{button-ref} rlmodule-guide
:color: primary
:outline:
:expand:

Models (RLModule)
```
:::

:::{grid-item-card}

**Algorithms**
^^^
See the RL algorithms RLlib provides for on-policy and off-policy training, offline and model-based RL, multi-agent RL, and more.
+++
```{button-ref} rllib-algorithms-doc
:color: primary
:outline:
:expand:

Algorithms
```
:::

::::


## Customize RLlib

RLlib provides APIs for customizing every part of your experimental and production training workflows. For example, you can code your own {ref}`environments <configuring-environments>` in Python with the [Farama Foundation's Gymnasium](https://farama.org) or DeepMind's OpenSpiel, provide custom [PyTorch models](https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/custom_cnn_rl_module.py), write your own [optimizer setups and loss definitions](https://github.com/ray-project/ray/blob/master/rllib/examples/learners/ppo_with_custom_loss_fn.py), or define custom [exploratory behavior](https://github.com/ray-project/ray/blob/master/rllib/examples/curiosity/count_based_curiosity.py).

```{figure} images/rllib-new-api-stack-simple.svg
:align: left
:width: 850

**RLlib's API stack:** Built on Ray, RLlib provides off-the-shelf, distributed, fault-tolerant
algorithms and loss functions, PyTorch default models, multi-GPU training, and multi-agent support.
You customize your experiments by subclassing the existing abstractions.
```


## Cite RLlib

If RLlib helps with your academic research, the Ray RLlib team encourages you to cite these papers:

```
@inproceedings{liang2021rllib,
    title={{RLlib} Flow: Distributed Reinforcement Learning is a Dataflow Problem},
    author={
        Wu, Zhanghao and
        Liang, Eric and
        Luo, Michael and
        Mika, Sven and
        Gonzalez, Joseph E. and
        Stoica, Ion
    },
    booktitle={Conference on Neural Information Processing Systems ({NeurIPS})},
    year={2021},
    url={https://proceedings.neurips.cc/paper/2021/file/2bce32ed409f5ebcee2a7b417ad9beed-Paper.pdf}
}

@inproceedings{liang2018rllib,
    title={{RLlib}: Abstractions for Distributed Reinforcement Learning},
    author={
        Eric Liang and
        Richard Liaw and
        Robert Nishihara and
        Philipp Moritz and
        Roy Fox and
        Ken Goldberg and
        Joseph E. Gonzalez and
        Michael I. Jordan and
        Ion Stoica,
    },
    booktitle = {International Conference on Machine Learning ({ICML})},
    year={2018},
    url={https://arxiv.org/pdf/1712.09381}
}
```
