# Deep Deterministic Policy Gradient (DDPG)

## Overview 

[DDPG](https://arxiv.org/abs/1509.02971) is a model-free off-policy RL algorithm that works well for environments in the continuous-action domain. DDPG employs two networks, a critic Q-network and an actor network. For stable training, DDPG also opts to use target networks to compute labels for the critic's loss function. 

For the critic network, the loss function is the L2 loss between critic output and critic target values. The critic target values are usually computed with a one-step bootstrap from the critic and actor target networks. On the other hand, the actor seeks to maximize the critic Q-values in its loss function. This is done by sampling backpropragable actions (via the reparameterization trick) from the actor and evaluating the critic, with frozen weights, on the generated state-action pairs. Like most off-policy algorithms, DDPG employs a replay buffer, which it samples batches from to compute gradients for the actor and critic networks. 

## Documentation & Implementation:

1) Deep Deterministic Policy Gradient (DDPG) and Twin Delayed DDPG (TD3)

    **[Detailed Documentation](https://docs.ray.io/en/latest/rllib-algorithms.html#ddpg)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/ddpg/ddpg.py)**
    
2) Ape-X variant of DDPG (Prioritized Experience Replay)

    **[Detailed Documentation](https://docs.ray.io/en/latest/rllib-algorithms.html#apex)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/ddpg/ddpg.py)**
