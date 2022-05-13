# Conservative Q-Learning (CQL)

## Overview 

[CQL](https://arxiv.org/abs/2006.04779) is an offline RL algorithm that mitigates the overestimation of Q-values outside the dataset distribution via convservative critic estimates. CQL does this by adding a simple Q regularizer loss to the standard Belman update loss. This ensures that the critic does not output overly-optimistic Q-values and can be added on top of any off-policy Q-learning algorithm (in this case, we use SAC). 

## Documentation & Implementation:

Conservative Q-Learning (CQL). 

   **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#cql)**

   **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/cql/cql.py)**
