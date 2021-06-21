# Deep Q Networks (DQN)

Code in this package is adapted from https://github.com/openai/baselines/tree/master/baselines/deepq.


## Overview 

[DQN](https://www.cs.toronto.edu/~vmnih/docs/dqn.pdf) is a model-free off-policy RL algorithm and one of the first deep RL algorithms developed. DQN proposes using a neural network as a function approximator for the Q-function in Q-learning. The agent aims to minimize the L2 norm between the Q-value predictions and the Q-value targets, which is computed as 1-step TD. The paper proposes two important concepts, a target network and an experience replay buffer. The target network is a copy of the main Q network and is used to compute Q-value targets for loss-function calculations. To stabilize training, the target network lags slightly behind the main Q-network. Meanwhile, the experience replay stores all data encountered by the agent during training and is uniformly sampled from to generate gradient updates for the Q-value network.


## Supported DQN Algorithms

[Double DQN](https://arxiv.org/pdf/1509.06461.pdf) - As opposed to learning one Q network in vanilla DQN, Double DQN proposes learning two Q networks akin to double Q-learning. As a solution, Double DQN aims to solve the issue of vanilla DQN's overly-optimistic Q-values, which limits performance.

[Dueling DQN](https://arxiv.org/pdf/1511.06581.pdf) - Dueling DQN proposes splitting learning a Q-value function approximator into learning two networks: a value and advantage approximator. 

[Distributional DQN](https://arxiv.org/pdf/1707.06887.pdf) - Usually, the Q network outputs the predicted Q-value of a state-action pair. Distributional DQN takes this further by predicting the distribution of Q-values (e.g. mean and std of a normal distribution) of a state-action pair. Doing this captures uncertainty of the Q-value and can improve the performance of DQN algorithms. 

[APEX-DQN](https://arxiv.org/pdf/1803.00933.pdf) - Standard DQN algorithms propose using a experience replay buffer to sample data uniformly and compute gradients from the sampled data. APEX introduces the notion of weighted replay data, where elements in the replay buffer are more or less likely to be sampled depending on the TD-error. 

[Rainbow](https://arxiv.org/pdf/1710.02298.pdf) - Rainbow DQN, as the word Rainbow suggests, aggregates the many improvements discovered in research to improve DQN performance. This includes a multi-step distributional loss (extended from Distributional DQN), prioritized replay (inspired from APEX-DQN), double Q-networks (inspired from Double DQN), and dueling networks (inspired from Dueling DQN). 


## Documentation & Implementation:

1) Vanilla DQN (DQN). 

    **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#dqn)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/dqn/simple_q.py)**

2) Double DQN.

    **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#dqn)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/dqn/dqn.py)**

3) Dueling DQN

    **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#dqn)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/dqn/dqn.py)**

3) Distributional DQN

    **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#dqn)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/dqn/dqn.py)**
    
4) APEX DQN

    **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#dqn)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/dqn/apex.py)**

5) Rainbow DQN

    **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#dqn)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/dqn/dqn.py)**
