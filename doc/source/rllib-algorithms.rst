RLlib Algorithms
================

Ape-X Distributed Prioritized Experience Replay
-----------------------------------------------
[paper] [implementation]
Ape-X variations of DQN and DDPG (APEX_DQN, APEX_DDPG in RLlib) use a single GPU learner and many CPU workers for experience collection. Experience collection can scale to hundreds of CPU workers due to the distributed prioritization of experience prior to storage in replay buffers.

Tuned examples: PongNoFrameskip-v4

Ape-X can significantly outperform DQN and A3C:
{APEX figure}

Asynchronous Advantage Actor-Critic
-----------------------------------
[paper] [implementation]
RLlib's A3C uses the AsyncGradientsOptimizer to apply gradients computed remotely on policy evaluation actors. It scales to up to 16-32 worker processes, depending on the environment. Both a TensorFlow (LSTM), and PyTorch version are available.

Tuned examples: PongDeterministic-v4

Deep Deterministic Policy Gradients
-----------------------------------
[paper] [implementation]
DDPG is implemented similarly to DQN (below). The algorithm can be scaled by increasing the number of workers, switching to AsyncGradientsOptimizer, or using Ape-X.

Tuned examples:

Deep Q Networks
---------------
[paper] [implementation]
RLlib DQN is implemented using the SyncReplayOptimizer. The algorithm can be scaled by increasing the number of workers, using the AsyncGradientsOptimizer for async DQN, or using Ape-X. Memory usage is reduced by compressing samples in the replay buffer with LZ4.

Tuned examples:

Evolution Strategies
--------------------
[paper] [implementation]
Code here is adapted from https://github.com/openai/evolution-strategies-starter to execute in the distributed setting with Ray.

Tuned examples: Humanoid-v0

RLlib's ES implementation scales further and is faster than a reference Redis implementation:
<benchmark>

Policy Gradients
----------------
[paper] [implementation]
We include a vanilla policy gradients implementation as an example algorithm. This is usually outperformed by PPO.

Tuned examples:

Proximal Policy Optimization
----------------------------

PPO's clipped objective supports multiple SGD passes over the same batch of experiences. RLlib's multi-GPU optimizer pins that data in GPU memory to avoid unnecessary transfers from host memory, substantially improving performance over a naive implementation. RLlib's PPO scales out using multiple workers for experience collection, and also with multiple GPUs for SGD.

Tuned examples:

RLlib's PPO is more cost effective and faster than a reference PPO implementation:
<benchmark>
