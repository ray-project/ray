# Dreamer

![Alt Text](https://miro.medium.com/max/384/0*yRXGyjtN3YBlhpMF)

## Overview 

[Dreamer](https://arxiv.org/abs/1912.01603) is a model-based off-policy RL algorithm that learns by imagining and works well in visual-based environments. Like all model-based algorithms, Dreamer learns the environment's transiton dynamics via a latent-space model called [PlaNet](https://ai.googleblog.com/2019/02/introducing-planet-deep-planning.html). PlaNet learns to encode visual space into latent vectors, which can be used as pseudo-observations in Dreamer. 

Dreamer is a gradient-based RL algorithm. This means that the agent imagines ahead using its learned transition dynamics model (PlaNet) to discover new rewards and states. Because imagining ahead is fully differentiable, the RL objective (maximizing the sum of rewards) is fully differentiable and does not need to be optimized indirectly such as policy gradient methods. This feature of gradient-based learning, in conjunction with PlaNet, enables the agent to learn in a latent space and achieves much better sample complexity and performance than other visual-based agents. 

For more details, there is a Ray/RLlib [blogpost](https://medium.com/distributed-computing-with-ray/model-based-reinforcement-learning-with-ray-rllib-73f47df33839˜) that better covers the components of PlaNet and the distributed execution plan. 

## Documentation & Implementation:

Dreamer. 

  **[Detailed Documentation](https://docs.ray.io/en/latest/rllib/rllib-algorithms.html#dreamer)**

  **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/algorithms/dreamer/dreamer.py)**
