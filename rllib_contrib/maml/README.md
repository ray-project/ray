# MAML (Model-Agnostic Meta-Learning for Fast Adaptation of Deep Networks)

[MAML](https://arxiv.org/abs/1703.03400) is an on-policy meta RL algorithm. Unlike standard RL algorithms, which aim to maximize the sum of rewards into the future for a single task (e.g. HalfCheetah), meta RL algorithms seek to maximize the sum of rewards for *a given distribution of tasks*.

On a high level, MAML seeks to learn quick adaptation across different tasks (e.g. different velocities for HalfCheetah). Quick adaptation is defined by the number of gradient steps it takes to adapt. MAML aims to maximize the RL objective for each task after `X` gradient steps. Doing this requires partitioning the algorithm into two steps. The first step is data collection. This involves collecting data for each task for each step of adaptation (from `1, 2, ..., X`). The second step is the meta-update step. This second step takes all the aggregated ddata from the first step and computes the meta-gradient.

Code here is adapted from https://github.com/jonasrothfuss, which outperforms vanilla MAML and avoids computation of the higher order gradients during the meta-update step. MAML is evaluated on custom environments that are described in greater detail here.

MAML uses additional metrics to measure performance; episode_reward_mean measures the agent’s returns before adaptation, episode_reward_mean_adapt_N measures the agent’s returns after N gradient steps of inner adaptation, and adaptation_delta measures the difference in performance before and after adaptation.


## Installation

```
conda create -n rllib-maml python=3.10
conda activate rllib-maml
python -m pip install ray==2.3.1
python -m pip install -e '.[development]'
pre-commit install
```

## Usage

```python
from gymnasium.wrappers import TimeLimit

import ray
from ray import air
from ray import tune
from rllib_maml.maml import MAML, MAMLConfig
from ray.rllib.examples.env.cartpole_mass import CartPoleMassEnv
from ray.tune.registry import register_env


if __name__ == "__main__":
    ray.init()
    register_env(
        "cartpole",
        lambda env_cfg: TimeLimit(CartPoleMassEnv(), max_episode_steps=200),
    )

    rollout_fragment_length = 32

    config = (
        MAMLConfig()
        .rollouts(
            num_rollout_workers=4, rollout_fragment_length=rollout_fragment_length
        )
        .framework("torch")
        .environment("cartpole", clip_actions=False)
        .training(
            inner_adaptation_steps=1,
            maml_optimizer_steps=5,
            gamma=0.99,
            lambda_=1.0,
            lr=0.001,
            vf_loss_coeff=0.5,
            inner_lr=0.03,
            use_meta_env=False,
            clip_param=0.3,
            kl_target=0.01,
            kl_coeff=0.001,
            model=dict(fcnet_hiddens=[64, 64]),
            train_batch_size=rollout_fragment_length,
        )
    )

    num_iterations = 100

    tuner = tune.Tuner(
        MAML,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={"training_iteration": num_iterations},
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    results = tuner.fit()

```
