# Deep Q Networks (DQN)

Code in this package is adapted from https://github.com/openai/baselines/tree/master/baselines/deepq.


## Overview 

[DQN](https://www.cs.toronto.edu/~vmnih/docs/dqn.pdf) is a model-free off-policy RL algorithm and one of the first deep RL algorithms developed. DQN proposes using a neural network function approximator in place of the Q-value in Q-learning. The paper proposes two important concepts, a target network and an experience replay buffer. The target network is . Meanwhile, the experience replay stores all data encountered by the agent during training and is sampled from to generate gradient updates for the Q-value networks. 


## Supported DQN Algorithms

Double DQN - As opposed to learning one Q network in vanilla DQN, Double DQN proposes learning two Q networks akin to double Q-learning. As a solution, Double DQN aims to solve the issue of vanilla DQN's overly-optimistic Q-values, which limits performance.

Dueling DQN - Dueling DQN proposes splitting learning a Q-value function approximator into learning two networks: a value and advantage approximator. 

Distributional DQN - Usually, the Q network outputs the predicted Q-value of a state-action pair. Distributional DQN takes this further by predicting the distribution of Q-values (e.g. mean and std of a normal distribution) of a state-action pair. Doing this captures uncertainty of the Q-value and can improve the performance of DQN algorithms. 

APEX-DQN - Standard DQN algorithms propose using a experience replay buffer to sample data uniformly and compute gradients from the sampled data. APEX introduces the notion of weighted replay data, where elements in the replay buffer are more or less likely to be sampled depending on the TD-error. 

Rainbow - Rainbow, as it definition means, means 


## Documentation & Implementation:

1) Vanilla DQN (DQN). 

    **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#dqn)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/ppo/ppo.py)**

2) Double DQN.

    **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#dqn)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/ppo/appo.py)**

3) Distributional DQN

    **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#dqn)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/ppo/ddppo.py)**
