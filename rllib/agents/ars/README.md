# Augmented Random Search (ARS)

## Overview 

[ARS](https://arxiv.org/abs/1803.07055) is a sample-efficient random research method that can outperform model-free RL algorithms. For iteration, ARS iterately discovers new policies from a central policy and sorts these policies by their performance. At the end of the iteration, the best N policies are chosen compute the final update for the central policy.

## Documentation & Implementation:

Augmented Random Search (ARS). 

   **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#ars)**

   **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/ars/ars.py)**
