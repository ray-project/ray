# Augmented Random Search (ARS)

## Overview 

[ARS](https://arxiv.org/abs/1803.07055) is a sample-efficient random search method that can outperform model-free RL algorithms. For each iteration, ARS discovers  new policies via random noise from a central policy and sorts these policies by their performance in the environment. At the end of each iteration, the best policies ranked by performance are used to compute the final update for the central policy.

## Documentation & Implementation:

Augmented Random Search (ARS). 

   **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#ars)**

   **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/ars/ars.py)**
