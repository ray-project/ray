RLlib Algorithms
================

Ape-X Distributed Prioritized Experience Replay
-----------------------------------------------
`[paper] <https://arxiv.org/abs/1803.00933>`__
`[implementation] <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/dqn/apex.py>`__
Ape-X variations of DQN and DDPG (`APEX_DQN <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/dqn/apex.py>`__, `APEX_DDPG <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/ddpg/apex.py>`__ in RLlib) use a single GPU learner and many CPU workers for experience collection. Experience collection can scale to hundreds of CPU workers due to the distributed prioritization of experience prior to storage in replay buffers.

Tuned examples: `PongNoFrameskip-v4 <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/pong-apex.yaml>`__, `Pendulum-v0 <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/pendulum-apex-ddpg.yaml>`__, `MountainCarContinuous-v0 <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/mountaincarcontinuous-apex-ddpg.yaml>`__

.. figure:: apex.png

    Ape-X using 32 workers in RLlib vs vanilla DQN (orange) and A3C (blue) on PongNoFrameskip-v4.

Asynchronous Advantage Actor-Critic
-----------------------------------
`[paper] <https://arxiv.org/abs/1602.01783>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/a3c/a3c.py>`__
RLlib's A3C uses the AsyncGradientsOptimizer to apply gradients computed remotely on policy evaluation actors. It scales to up to 16-32 worker processes, depending on the environment. Both a TensorFlow (LSTM), and PyTorch version are available.

Tuned examples: `PongDeterministic-v4 <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/pong-a3c.yaml>`__, `PyTorch version <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/pong-a3c-pytorch.yaml>`__

Deep Deterministic Policy Gradients
-----------------------------------
`[paper] <https://arxiv.org/abs/1509.02971>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/ddpg/ddpg.py>`__
DDPG is implemented similarly to DQN (below). The algorithm can be scaled by increasing the number of workers, switching to AsyncGradientsOptimizer, or using Ape-X.

Tuned examples: `Pendulum-v0 <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/pendulum-ddpg.yaml>`__, `MountainCarContinuous-v0 <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/mountaincarcontinuous-ddpg.yaml>`__, `HalfCheetah-v2 <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/halfcheetah-ddpg.yaml>`__

Deep Q Networks
---------------
`[paper] <https://arxiv.org/abs/1312.5602>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/dqn/dqn.py>`__
RLlib DQN is implemented using the SyncReplayOptimizer. The algorithm can be scaled by increasing the number of workers, using the AsyncGradientsOptimizer for async DQN, or using Ape-X. Memory usage is reduced by compressing samples in the replay buffer with LZ4.

Tuned examples: `PongDeterministic-v4 <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/pong-dqn.yaml>`__

Evolution Strategies
--------------------
`[paper] <https://arxiv.org/abs/1703.03864>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/es/es.py>`__
Code here is adapted from https://github.com/openai/evolution-strategies-starter to execute in the distributed setting with Ray.

Tuned examples: `Humanoid-v1 <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/humanoid-es.yaml>`__

.. figure:: es.png
   :width: 500px
   :align: center

   RLlib's ES implementation scales further and is faster than a reference Redis implementation.

Importance Weighted Actor-Learner Architecture
----------------------------------------------

`[paper] <https://arxiv.org/abs/1802.01561>`__
`[implementation] <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/impala/impala.py>`__
In IMPALA, a central learner runs SGD in a tight loop while asynchronously pulling sample batches from many actor processes. RLlib's IMPALA implementation uses DeepMind's reference `V-trace code <https://github.com/deepmind/scalable_agent/blob/master/vtrace.py>`__. Note that we do not provide a deep residual network out of the box, but one can be plugged in as a `custom model <rllib-models.html#custom-models>`__.

Tuned examples: `PongNoFrameskip-v4 <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/pong-impala.yaml>`__, `vectorized configuration <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/pong-impala-vectorized.yaml>`__

.. figure:: impala.png
   :align: center

   RLlib's IMPALA implementation scaling from 16 to 128 workers on PongNoFrameskip-v4. With vectorization, similar learning performance to 128 workers can be achieved with only 32 workers.

Policy Gradients
----------------
`[paper] <https://papers.nips.cc/paper/1713-policy-gradient-methods-for-reinforcement-learning-with-function-approximation.pdf>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/pg/pg.py>`__ We include a vanilla policy gradients implementation as an example algorithm. This is usually outperformed by PPO.

Tuned examples: `CartPole-v0 <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/regression_tests/cartpole-pg.yaml>`__

Proximal Policy Optimization
----------------------------
`[paper] <https://arxiv.org/abs/1707.06347>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/ppo/ppo.py>`__
PPO's clipped objective supports multiple SGD passes over the same batch of experiences. RLlib's multi-GPU optimizer pins that data in GPU memory to avoid unnecessary transfers from host memory, substantially improving performance over a naive implementation. RLlib's PPO scales out using multiple workers for experience collection, and also with multiple GPUs for SGD.

Tuned examples: `Humanoid-v1 <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/humanoid-ppo-gae.yaml>`__, `Hopper-v1 <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/hopper-ppo.yaml>`__, `Pendulum-v0 <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/pendulum-ppo.yaml>`__, `PongDeterministic-v4 <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/pong-ppo.yaml>`__, `Walker2d-v1 <https://github.com/ray-project/ray/blob/master/python/ray/rllib/tuned_examples/walker2d-ppo.yaml>`__

.. figure:: ppo.png
   :width: 500px
   :align: center

   RLlib's PPO is more cost effective and faster than a reference PPO implementation.
