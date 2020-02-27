RLlib Algorithms
================

High-throughput architectures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Distributed Prioritized Experience Replay (Ape-X)
-------------------------------------------------
|tensorflow|
`[paper] <https://arxiv.org/abs/1803.00933>`__
`[implementation] <https://github.com/ray-project/ray/blob/master/rllib/agents/dqn/apex.py>`__
Ape-X variations of DQN, DDPG, and QMIX (`APEX_DQN <https://github.com/ray-project/ray/blob/master/rllib/agents/dqn/apex.py>`__, `APEX_DDPG <https://github.com/ray-project/ray/blob/master/rllib/agents/ddpg/apex.py>`__, `APEX_QMIX <https://github.com/ray-project/ray/blob/master/rllib/agents/qmix/apex.py>`__) use a single GPU learner and many CPU workers for experience collection. Experience collection can scale to hundreds of CPU workers due to the distributed prioritization of experience prior to storage in replay buffers.

.. figure:: apex-arch.svg

    Ape-X architecture

Tuned examples: `PongNoFrameskip-v4 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/pong-apex.yaml>`__, `Pendulum-v0 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/pendulum-apex-ddpg.yaml>`__, `MountainCarContinuous-v0 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/mountaincarcontinuous-apex-ddpg.yaml>`__, `{BeamRider,Breakout,Qbert,SpaceInvaders}NoFrameskip-v4 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/atari-apex.yaml>`__.

**Atari results @10M steps**: `more details <https://github.com/ray-project/rl-experiments>`__

=============  ================================  ========================================
 Atari env     RLlib Ape-X 8-workers             Mnih et al Async DQN 16-workers
=============  ================================  ========================================
BeamRider      6134                              ~6000
Breakout       123                               ~50
Qbert          15302                             ~1200
SpaceInvaders  686                               ~600
=============  ================================  ========================================

**Scalability**:

=============  ================================  ========================================
 Atari env     RLlib Ape-X 8-workers @1 hour     Mnih et al Async DQN 16-workers @1 hour
=============  ================================  ========================================
BeamRider      4873                              ~1000
Breakout       77                                ~10
Qbert          4083                              ~500
SpaceInvaders  646                               ~300
=============  ================================  ========================================

.. figure:: apex.png

    Ape-X using 32 workers in RLlib vs vanilla DQN (orange) and A3C (blue) on PongNoFrameskip-v4.

**Ape-X specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. literalinclude:: ../../rllib/agents/dqn/apex.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

Importance Weighted Actor-Learner Architecture (IMPALA)
-------------------------------------------------------
|tensorflow|
`[paper] <https://arxiv.org/abs/1802.01561>`__
`[implementation] <https://github.com/ray-project/ray/blob/master/rllib/agents/impala/impala.py>`__
In IMPALA, a central learner runs SGD in a tight loop while asynchronously pulling sample batches from many actor processes. RLlib's IMPALA implementation uses DeepMind's reference `V-trace code <https://github.com/deepmind/scalable_agent/blob/master/vtrace.py>`__. Note that we do not provide a deep residual network out of the box, but one can be plugged in as a `custom model <rllib-models.html#custom-models-tensorflow>`__. Multiple learner GPUs and experience replay are also supported.

.. figure:: impala-arch.svg

    IMPALA architecture

Tuned examples: `PongNoFrameskip-v4 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/pong-impala.yaml>`__, `vectorized configuration <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/pong-impala-vectorized.yaml>`__, `multi-gpu configuration <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/pong-impala-fast.yaml>`__, `{BeamRider,Breakout,Qbert,SpaceInvaders}NoFrameskip-v4 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/atari-impala.yaml>`__

**Atari results @10M steps**: `more details <https://github.com/ray-project/rl-experiments>`__

=============  ==================================  ====================================
 Atari env     RLlib IMPALA 32-workers             Mnih et al A3C 16-workers
=============  ==================================  ====================================
BeamRider      2071                                ~3000
Breakout       385                                 ~150
Qbert          4068                                ~1000
SpaceInvaders  719                                 ~600
=============  ==================================  ====================================

**Scalability:**

=============  ===============================  =================================
 Atari env     RLlib IMPALA 32-workers @1 hour  Mnih et al A3C 16-workers @1 hour
=============  ===============================  =================================
BeamRider      3181                             ~1000
Breakout       538                              ~10
Qbert          10850                            ~500
SpaceInvaders  843                              ~300
=============  ===============================  =================================

.. figure:: impala.png

   Multi-GPU IMPALA scales up to solve PongNoFrameskip-v4 in ~3 minutes using a pair of V100 GPUs and 128 CPU workers.
   The maximum training throughput reached is ~30k transitions per second (~120k environment frames per second).

**IMPALA-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. literalinclude:: ../../rllib/agents/impala/impala.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

Asynchronous Proximal Policy Optimization (APPO)
------------------------------------------------
|tensorflow|
`[paper] <https://arxiv.org/abs/1707.06347>`__
`[implementation] <https://github.com/ray-project/ray/blob/master/rllib/agents/ppo/appo.py>`__
We include an asynchronous variant of Proximal Policy Optimization (PPO) based on the IMPALA architecture. This is similar to IMPALA but using a surrogate policy loss with clipping. Compared to synchronous PPO, APPO is more efficient in wall-clock time due to its use of asynchronous sampling. Using a clipped loss also allows for multiple SGD passes, and therefore the potential for better sample efficiency compared to IMPALA. V-trace can also be enabled to correct for off-policy samples.

.. tip::

    APPO is not always more efficient; it is often better to use `standard PPO <rllib-algorithms.html#proximal-policy-optimization-ppo>`__ or `IMPALA <rllib-algorithms.html#importance-weighted-actor-learner-architecture-impala>`__.

.. figure:: impala-arch.svg

    APPO architecture (same as IMPALA)

Tuned examples: `PongNoFrameskip-v4 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/pong-appo.yaml>`__

**APPO-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. literalinclude:: ../../rllib/agents/ppo/appo.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

Decentralized Distributed Proximal Policy Optimization (DD-PPO)
---------------------------------------------------------------
|pytorch|
`[paper] <https://arxiv.org/abs/1911.00357>`__
`[implementation] <https://github.com/ray-project/ray/blob/master/rllib/agents/ppo/ddppo.py>`__
Unlike APPO or PPO, with DD-PPO policy improvement is no longer done centralized in the trainer process. Instead, gradients are computed remotely on each rollout worker and all-reduced at each mini-batch using `torch distributed <https://pytorch.org/docs/stable/distributed.html>`__. This allows each worker's GPU to be used both for sampling and for training.

.. tip::

    DD-PPO is best for envs that require GPUs to function, or if you need to scale out SGD to multiple nodes. If you don't meet these requirements, `standard PPO <#proximal-policy-optimization-ppo>`__ will be more efficient.

.. figure:: ddppo-arch.svg

    DD-PPO architecture (both sampling and learning are done on worker GPUs)

Tuned examples: `CartPole-v0 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/regression_tests/cartpole-ddppo.yaml>`__, `BreakoutNoFrameskip-v4 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/atari-ddppo.yaml>`__

**DDPPO-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. literalinclude:: ../../rllib/agents/ppo/ddppo.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

Gradient-based
~~~~~~~~~~~~~~

Advantage Actor-Critic (A2C, A3C)
---------------------------------
|pytorch| |tensorflow|
`[paper] <https://arxiv.org/abs/1602.01783>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/rllib/agents/a3c/a3c.py>`__
RLlib implements A2C and A3C using SyncSamplesOptimizer and AsyncGradientsOptimizer respectively for policy optimization. These algorithms scale to up to 16-32 worker processes depending on the environment.

A2C also supports microbatching (i.e., gradient accumulation), which can be enabled by setting the ``microbatch_size`` config. Microbatching allows for training with a ``train_batch_size`` much larger than GPU memory. See also the `microbatch optimizer implementation <https://github.com/ray-project/ray/blob/master/rllib/optimizers/microbatch_optimizer.py>`__.

.. figure:: a2c-arch.svg

    A2C architecture

Tuned examples: `PongDeterministic-v4 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/pong-a3c.yaml>`__, `PyTorch version <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/pong-a3c-pytorch.yaml>`__, `{BeamRider,Breakout,Qbert,SpaceInvaders}NoFrameskip-v4 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/atari-a2c.yaml>`__

.. tip::
    Consider using `IMPALA <#importance-weighted-actor-learner-architecture-impala>`__ for faster training with similar timestep efficiency.

**Atari results @10M steps**: `more details <https://github.com/ray-project/rl-experiments>`__

=============  ========================  ==============================
 Atari env     RLlib A2C 5-workers       Mnih et al A3C 16-workers
=============  ========================  ==============================
BeamRider      1401                      ~3000
Breakout       374                       ~150
Qbert          3620                      ~1000
SpaceInvaders  692                       ~600
=============  ========================  ==============================

**A3C-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. literalinclude:: ../../rllib/agents/a3c/a3c.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

Deep Deterministic Policy Gradients (DDPG, TD3)
-----------------------------------------------
|tensorflow|
`[paper] <https://arxiv.org/abs/1509.02971>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/rllib/agents/ddpg/ddpg.py>`__
DDPG is implemented similarly to DQN (below). The algorithm can be scaled by increasing the number of workers, switching to AsyncGradientsOptimizer, or using Ape-X. The improvements from `TD3 <https://spinningup.openai.com/en/latest/algorithms/td3.html>`__ are available as ``TD3``.

.. figure:: dqn-arch.svg

    DDPG architecture (same as DQN)

Tuned examples: `Pendulum-v0 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/pendulum-ddpg.yaml>`__, `MountainCarContinuous-v0 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/mountaincarcontinuous-ddpg.yaml>`__, `HalfCheetah-v2 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/halfcheetah-ddpg.yaml>`__, `TD3 Pendulum-v0 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/pendulum-td3.yaml>`__, `TD3 InvertedPendulum-v2 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/invertedpendulum-td3.yaml>`__, `TD3 Mujoco suite (Ant-v2, HalfCheetah-v2, Hopper-v2, Walker2d-v2) <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/mujoco-td3.yaml>`__.

**DDPG-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. literalinclude:: ../../rllib/agents/ddpg/ddpg.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

Deep Q Networks (DQN, Rainbow, Parametric DQN)
----------------------------------------------
|tensorflow|
`[paper] <https://arxiv.org/abs/1312.5602>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/rllib/agents/dqn/dqn.py>`__
RLlib DQN is implemented using the SyncReplayOptimizer. The algorithm can be scaled by increasing the number of workers, using the AsyncGradientsOptimizer for async DQN, or using Ape-X. Memory usage is reduced by compressing samples in the replay buffer with LZ4. All of the DQN improvements evaluated in `Rainbow <https://arxiv.org/abs/1710.02298>`__ are available, though not all are enabled by default. See also how to use `parametric-actions in DQN <rllib-models.html#variable-length-parametric-action-spaces>`__.

.. figure:: dqn-arch.svg

    DQN architecture

Tuned examples: `PongDeterministic-v4 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/pong-dqn.yaml>`__, `Rainbow configuration <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/pong-rainbow.yaml>`__, `{BeamRider,Breakout,Qbert,SpaceInvaders}NoFrameskip-v4 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/atari-basic-dqn.yaml>`__, `with Dueling and Double-Q <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/atari-duel-ddqn.yaml>`__, `with Distributional DQN <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/atari-dist-dqn.yaml>`__.

.. tip::
    Consider using `Ape-X <#distributed-prioritized-experience-replay-ape-x>`__ for faster training with similar timestep efficiency.

.. hint::
    For a complete `rainbow <https://arxiv.org/pdf/1710.02298.pdf>`__ setup,
    make the following changes to the default DQN config:
    ``"n_step": [between 1 and 10],
    "noisy": True,
    "num_atoms": [more than 1],
    "v_min": -10.0,
    "v_max": 10.0``
    (set ``v_min`` and ``v_max`` according to your expected range of returns).

**Atari results @10M steps**: `more details <https://github.com/ray-project/rl-experiments>`__

=============  ========================  =============================  ==============================  ===============================
 Atari env     RLlib DQN                 RLlib Dueling DDQN             RLlib Dist. DQN                 Hessel et al. DQN
=============  ========================  =============================  ==============================  ===============================
BeamRider      2869                      1910                           4447                            ~2000
Breakout       287                       312                            410                             ~150
Qbert          3921                      7968                           15780                           ~4000
SpaceInvaders  650                       1001                           1025                            ~500
=============  ========================  =============================  ==============================  ===============================

**DQN-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. literalinclude:: ../../rllib/agents/dqn/dqn.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

Policy Gradients
----------------
|pytorch| |tensorflow|
`[paper] <https://papers.nips.cc/paper/1713-policy-gradient-methods-for-reinforcement-learning-with-function-approximation.pdf>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/rllib/agents/pg/pg.py>`__ We include a vanilla policy gradients implementation as an example algorithm.

.. figure:: a2c-arch.svg

    Policy gradients architecture (same as A2C)

Tuned examples: `CartPole-v0 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/regression_tests/cartpole-pg-tf.yaml>`__

**PG-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. literalinclude:: ../../rllib/agents/pg/pg.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

Proximal Policy Optimization (PPO)
----------------------------------
|pytorch| |tensorflow|
`[paper] <https://arxiv.org/abs/1707.06347>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/rllib/agents/ppo/ppo.py>`__
PPO's clipped objective supports multiple SGD passes over the same batch of experiences. RLlib's multi-GPU optimizer pins that data in GPU memory to avoid unnecessary transfers from host memory, substantially improving performance over a naive implementation. PPO scales out using multiple workers for experience collection, and also to multiple GPUs for SGD.

.. tip::

    If you need to scale out with GPUs on multiple nodes, consider using `decentralized PPO <#decentralized-distributed-proximal-policy-optimization-dd-ppo>`__.

.. figure:: ppo-arch.svg

    PPO architecture

Tuned examples: `Humanoid-v1 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/humanoid-ppo-gae.yaml>`__, `Hopper-v1 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/hopper-ppo.yaml>`__, `Pendulum-v0 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/pendulum-ppo.yaml>`__, `PongDeterministic-v4 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/pong-ppo.yaml>`__, `Walker2d-v1 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/walker2d-ppo.yaml>`__, `HalfCheetah-v2 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/halfcheetah-ppo.yaml>`__, `{BeamRider,Breakout,Qbert,SpaceInvaders}NoFrameskip-v4 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/atari-ppo.yaml>`__


**Atari results**: `more details <https://github.com/ray-project/rl-experiments>`__

=============  ==============  ==============  ==================
 Atari env     RLlib PPO @10M  RLlib PPO @25M  Baselines PPO @10M
=============  ==============  ==============  ==================
BeamRider      2807            4480            ~1800
Breakout       104             201             ~250
Qbert          11085           14247           ~14000
SpaceInvaders  671             944             ~800
=============  ==============  ==============  ==================


**Scalability:** `more details <https://github.com/ray-project/rl-experiments>`__

=============  =========================  =============================
MuJoCo env     RLlib PPO 16-workers @ 1h  Fan et al PPO 16-workers @ 1h
=============  =========================  =============================
HalfCheetah    9664                       ~7700
=============  =========================  =============================

.. figure:: ppo.png
   :width: 500px

   RLlib's multi-GPU PPO scales to multiple GPUs and hundreds of CPUs on solving the Humanoid-v1 task. Here we compare against a reference MPI-based implementation.

**PPO-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. literalinclude:: ../../rllib/agents/ppo/ppo.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

Soft Actor Critic (SAC)
------------------------
|tensorflow|
`[paper] <https://arxiv.org/pdf/1801.01290>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/rllib/agents/sac/sac.py>`__

.. figure:: dqn-arch.svg

    SAC architecture (same as DQN)

RLlib's soft-actor critic implementation is ported from the `official SAC repo <https://github.com/rail-berkeley/softlearning>`__ to better integrate with RLlib APIs. Note that SAC has two fields to configure for custom models: ``policy_model`` and ``Q_model``, and currently has no support for non-continuous action distributions.

Tuned examples: `Pendulum-v0 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/regression_tests/pendulum-sac.yaml>`__, `HalfCheetah-v3 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/halfcheetah-sac.yaml>`__

**MuJoCo results @3M steps:** `more details <https://github.com/ray-project/rl-experiments>`__

=============  ==========  ===================
MuJoCo env     RLlib SAC   Haarnoja et al SAC
=============  ==========  ===================
HalfCheetah    13000       ~15000
=============  ==========  ===================

**SAC-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. literalinclude:: ../../rllib/agents/sac/sac.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

Derivative-free
~~~~~~~~~~~~~~~

Augmented Random Search (ARS)
-----------------------------
|tensorflow|
`[paper] <https://arxiv.org/abs/1803.07055>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/rllib/agents/ars/ars.py>`__
ARS is a random search method for training linear policies for continuous control problems. Code here is adapted from https://github.com/modestyachts/ARS to integrate with RLlib APIs.

Tuned examples: `CartPole-v0 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/regression_tests/cartpole-ars.yaml>`__, `Swimmer-v2 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/swimmer-ars.yaml>`__

**ARS-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. literalinclude:: ../../rllib/agents/ars/ars.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

Evolution Strategies
--------------------
|tensorflow|
`[paper] <https://arxiv.org/abs/1703.03864>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/rllib/agents/es/es.py>`__
Code here is adapted from https://github.com/openai/evolution-strategies-starter to execute in the distributed setting with Ray.

Tuned examples: `Humanoid-v1 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/humanoid-es.yaml>`__

**Scalability:**

.. figure:: es.png
   :width: 500px

   RLlib's ES implementation scales further and is faster than a reference Redis implementation on solving the Humanoid-v1 task.

**ES-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. literalinclude:: ../../rllib/agents/es/es.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

QMIX Monotonic Value Factorisation (QMIX, VDN, IQN)
---------------------------------------------------
|pytorch|
`[paper] <https://arxiv.org/abs/1803.11485>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/rllib/agents/qmix/qmix.py>`__ Q-Mix is a specialized multi-agent algorithm. Code here is adapted from https://github.com/oxwhirl/pymarl_alpha  to integrate with RLlib multi-agent APIs. To use Q-Mix, you must specify an agent `grouping <rllib-env.html#grouping-agents>`__ in the environment (see the `two-step game example <https://github.com/ray-project/ray/blob/master/rllib/examples/twostep_game.py>`__). Currently, all agents in the group must be homogeneous. The algorithm can be scaled by increasing the number of workers or using Ape-X.

Tuned examples: `Two-step game <https://github.com/ray-project/ray/blob/master/rllib/examples/twostep_game.py>`__

**QMIX-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. literalinclude:: ../../rllib/agents/qmix/qmix.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

Multi-Agent Deep Deterministic Policy Gradient (contrib/MADDPG)
---------------------------------------------------------------
|tensorflow|
`[paper] <https://arxiv.org/abs/1706.02275>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/rllib/contrib/maddpg/maddpg.py>`__ MADDPG is a specialized multi-agent algorithm. Code here is adapted from https://github.com/openai/maddpg to integrate with RLlib multi-agent APIs. Please check `justinkterry/maddpg-rllib <https://github.com/justinkterry/maddpg-rllib>`__ for examples and more information.

**MADDPG-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

Tuned examples: `Multi-Agent Particle Environment <https://github.com/wsjeon/maddpg-rllib/tree/master/plots>`__, `Two-step game <https://github.com/ray-project/ray/blob/master/rllib/examples/twostep_game.py>`__

.. literalinclude:: ../../rllib/contrib/maddpg/maddpg.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

Advantage Re-Weighted Imitation Learning (MARWIL)
-------------------------------------------------
|tensorflow|
`[paper] <http://papers.nips.cc/paper/7866-exponentially-weighted-imitation-learning-for-batched-historical-data>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/rllib/agents/marwil/marwil.py>`__ MARWIL is a hybrid imitation learning and policy gradient algorithm suitable for training on batched historical data. When the ``beta`` hyperparameter is set to zero, the MARWIL objective reduces to vanilla imitation learning. MARWIL requires the `offline datasets API <rllib-offline.html>`__ to be used.

Tuned examples: `CartPole-v0 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/cartpole-marwil.yaml>`__

**MARWIL-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. literalinclude:: ../../rllib/agents/marwil/marwil.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

Single-Player Alpha Zero (contrib/AlphaZero)
--------------------------------------------
|pytorch|
`[paper] <https://arxiv.org/abs/1712.01815>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/rllib/contrib/alpha_zero>`__ AlphaZero is an RL agent originally designed for two-player games. This version adapts it to handle single player games. The code can be used with the SyncSamplesOptimizer as well as with a modified version of the SyncReplayOptimizer, and it scales to any number of workers. It also implements the ranked rewards `(R2) <https://arxiv.org/abs/1807.01672>`__ strategy to enable self-play even in the one-player setting. The code is mainly purposed to be used for combinatorial optimization.

Tuned examples: `CartPole-v0 <https://github.com/ray-project/ray/blob/master/rllib/contrib/alpha_zero/examples/train_cartpole.py>`__

**AlphaZero-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. literalinclude:: ../../rllib/contrib/alpha_zero/core/alpha_zero_trainer.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

.. |tensorflow| image:: tensorflow.png
    :width: 24

.. |pytorch| image:: pytorch.png
    :width: 24
