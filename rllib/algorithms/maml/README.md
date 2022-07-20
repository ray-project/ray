# Model Agnostic Meta-learning (MAML)

## Overview 

[MAML](https://arxiv.org/abs/1703.03400) is an on-policy meta RL algorithm. Unlike standard RL algorithms, which aim to maximize the sum of rewards into the future for a single task (e.g. HalfCheetah), meta RL algorithms seek to maximize the sum of rewards for *a given distribution of tasks*. 

On a high level, MAML seeks to learn quick adaptation across different tasks (e.g. different velocities for HalfCheetah). Quick adaptation is defined by the number of gradient steps it takes to adapt. MAML aims to maximize the RL objective for each task after `X` gradient steps. Doing this requires partitioning the algorithm into two steps. The first step is data collection. This involves collecting data for each task for each step of adaptation (from `1, 2, ..., X`). The second step is the meta-update step. This second step takes all the aggregated ddata from the first step and computes the meta-gradient. 


## Documentation & Implementation:

MAML. 

  **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#model-agnostic-meta-learning-maml)**

  **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/maml/maml.py)**
