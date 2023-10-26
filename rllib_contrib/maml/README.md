# MAML (Model-Agnostic Meta-Learning for Fast Adaptation of Deep Networks)

[MAML](https://arxiv.org/abs/1703.03400) is an on-policy meta RL algorithm. Unlike standard RL algorithms, which aim to maximize the sum of rewards into the future for a single task (e.g. HalfCheetah), meta RL algorithms seek to maximize the sum of rewards for *a given distribution of tasks*.

On a high level, MAML seeks to learn quick adaptation across different tasks (e.g. different velocities for HalfCheetah). Quick adaptation is defined by the number of gradient steps it takes to adapt. MAML aims to maximize the RL objective for each task after `X` gradient steps. Doing this requires partitioning the algorithm into two steps. The first step is data collection. This involves collecting data for each task for each step of adaptation (from `1, 2, ..., X`). The second step is the meta-update step. This second step takes all the aggregated ddata from the first step and computes the meta-gradient.

Code here is adapted from https://github.com/jonasrothfuss, which outperforms vanilla MAML and avoids computation of the higher order gradients during the meta-update step. MAML is evaluated on custom environments that are described in greater detail here.

MAML uses additional metrics to measure performance; episode_reward_mean measures the agent’s returns before adaptation, episode_reward_mean_adapt_N measures the agent’s returns after N gradient steps of inner adaptation, and adaptation_delta measures the difference in performance before and after adaptation.


## Installation

```
conda create -n rllib-maml python=3.10
conda activate rllib-maml
pip install -r requirements.txt
pip install -e '.[development]'
```

## Usage

[MAML Example](examples/cartpole_mass_maml.py)