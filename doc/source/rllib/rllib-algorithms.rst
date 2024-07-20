.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-algorithms-doc:

Algorithms
==========

Available Algorithms - Overview
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+-------------------------------------------------------------------------+----------------+---------------+-------------+------------------------+
| **Algorithm**                                                           | Single-agent   | Multi-agent   | Multi-GPU   | Multi-node + Multi-GPU |
+-------------------------------------------------------------------------+----------------+---------------+-------------+------------------------+
| **On-Policy**                                                                                                                                   |
+-------------------------------------------------------------------------+----------------+---------------+-------------+------------------------+
| :ref:`PPO (Proximal Policy Optimization) <ppo>`                         | |single_agent| | |multi_agent| | |multi_gpu| | |multi_node_multi_gpu| |
+-------------------------------------------------------------------------+----------------+---------------+-------------+------------------------+
| **Off-Policy**                                                                                                                                  |
+-------------------------------------------------------------------------+----------------+---------------+-------------+------------------------+
| :ref:`SAC (Soft Actor Critic) <sac>`                                    | |single_agent| | |multi_agent| | |multi_gpu| | |multi_node_multi_gpu| |
+-------------------------------------------------------------------------+----------------+---------------+-------------+------------------------+
| :ref:`DQN/Rainbow (Deep Q Networks) <dqn>`                              | |single_agent| | |multi_agent| | |multi_gpu| | |multi_node_multi_gpu| |
+-------------------------------------------------------------------------+----------------+---------------+-------------+------------------------+
| **High-throughput on- and off policy**                                                                                                          |
+-------------------------------------------------------------------------+----------------+---------------+-------------+------------------------+
| :ref:`IMPALA (Importance Weighted Actor-Learner Architecture) <impala>` | |single_agent| | |multi_agent| | |multi_gpu| | |multi_node_multi_gpu| |
+-------------------------------------------------------------------------+----------------+---------------+-------------+------------------------+
| :ref:`APPO (Asynchronous Proximal Policy Optimization) <appo>`          | |single_agent| | |multi_agent| | |multi_gpu| | |multi_node_multi_gpu| |
+-------------------------------------------------------------------------+----------------+---------------+-------------+------------------------+
| **Model-based RL**                                                                                                                              |
+-------------------------------------------------------------------------+----------------+---------------+-------------+------------------------+
| :ref:`DreamerV3 <dreamerv3>`                                            | |single_agent| |               | |multi_gpu| | |multi_node_multi_gpu| |
+-------------------------------------------------------------------------+----------------+---------------+-------------+------------------------+
| **Offline RL and Imitation Learning**                                                                                                           |
+-------------------------------------------------------------------------+----------------+---------------+-------------+------------------------+
| :ref:`BC (Behavior Cloning) <bc>`                                       | |single_agent| |               |             |                        |
+-------------------------------------------------------------------------+----------------+---------------+-------------+------------------------+



On-policy RL
~~~~~~~~~~~~

.. _ppo:

Proximal Policy Optimization (PPO)
----------------------------------
`[paper] <https://arxiv.org/abs/1707.06347>`__
`[implementation] <https://github.com/ray-project/ray/blob/master/rllib/algorithms/ppo/ppo.py>`__
PPO's clipped objective supports multiple SGD passes over the same batch of experiences in one training iteration.
PPO scales out on both axes, supporting multiple EnvRunner workers for sample collection and multiple GPU- or CPU-based Learner workers for updating
the model.

.. figure:: images/ppo-arch.svg

    PPO architecture

Tuned examples:



**PPO-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. autoclass:: ray.rllib.algorithms.ppo.ppo.PPOConfig
   :members: training


Off-Policy
~~~~~~~~~~

.. _dqn:

Deep Q Networks (DQN, Rainbow, Parametric DQN)
----------------------------------------------
`[paper] <https://arxiv.org/abs/1312.5602>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/rllib/algorithms/dqn/dqn.py>`__
DQN can be scaled by increasing the number of workers or using Ape-X. Memory usage is reduced by compressing samples in the replay buffer with LZ4. All of the DQN improvements evaluated in `Rainbow <https://arxiv.org/abs/1710.02298>`__ are available, though not all are enabled by default. See also how to use `parametric-actions in DQN <rllib-models.html#variable-length-parametric-action-spaces>`__.

.. figure:: images/dqn-arch.svg

    DQN architecture

Tuned examples: `PongDeterministic-v4 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/dqn/pong-dqn.yaml>`__, `Rainbow configuration <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/dqn/pong-rainbow.yaml>`__, `{BeamRider,Breakout,Qbert,SpaceInvaders}NoFrameskip-v4 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/dqn/atari-dqn.yaml>`__, `with Dueling and Double-Q <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/dqn/atari-duel-ddqn.yaml>`__, `with Distributional DQN <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/dqn/atari-dist-dqn.yaml>`__.

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

.. autoclass:: ray.rllib.algorithms.dqn.dqn.DQNConfig
   :members: training


.. _sac:

Soft Actor Critic (SAC)
------------------------
`[original paper] <https://arxiv.org/pdf/1801.01290>`__, `[follow up paper] <https://arxiv.org/pdf/1812.05905.pdf>`__, `[discrete actions paper] <https://arxiv.org/pdf/1910.07207v2.pdf>`__
`[implementation] <https://github.com/ray-project/ray/blob/master/rllib/algorithms/sac/sac.py>`__

.. figure:: images/dqn-arch.svg

    SAC architecture (same as DQN)

RLlib's soft-actor critic implementation is ported from the `official SAC repo <https://github.com/rail-berkeley/softlearning>`__ to better integrate with RLlib APIs.
Note that SAC has two fields to configure for custom models: ``policy_model_config`` and ``q_model_config``, the ``model`` field of the config is ignored.

Tuned examples (continuous actions):
`Pendulum-v1 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/sac/pendulum-sac.yaml>`__,
`HalfCheetah-v3 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/sac/halfcheetah-sac.yaml>`__,
Tuned examples (discrete actions):
`CartPole-v1 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/sac/cartpole-sac.yaml>`__

**MuJoCo results @3M steps:** `more details <https://github.com/ray-project/rl-experiments>`__

=============  ==========  ===================
MuJoCo env     RLlib SAC   Haarnoja et al SAC
=============  ==========  ===================
HalfCheetah    13000       ~15000
=============  ==========  ===================

**SAC-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. autoclass:: ray.rllib.algorithms.sac.sac.SACConfig
   :members: training


High-Throughput On- and Off-Policy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. _appo:

Asynchronous Proximal Policy Optimization (APPO)
------------------------------------------------
`[paper] <https://arxiv.org/abs/1707.06347>`__
`[implementation] <https://github.com/ray-project/ray/blob/master/rllib/algorithms/appo/appo.py>`__
We include an asynchronous variant of Proximal Policy Optimization (PPO) based on the IMPALA architecture. This is similar to IMPALA but using a surrogate policy loss with clipping. Compared to synchronous PPO, APPO is more efficient in wall-clock time due to its use of asynchronous sampling. Using a clipped loss also allows for multiple SGD passes, and therefore the potential for better sample efficiency compared to IMPALA. V-trace can also be enabled to correct for off-policy samples.

.. tip::

    APPO isn't always more efficient; it's often better to use :ref:`standard PPO <ppo>` or :ref:`IMPALA <impala>`.

.. figure:: images/impala-arch.svg

    APPO architecture (same as IMPALA)

Tuned examples: `PongNoFrameskip-v4 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/appo/pong-appo.yaml>`__

**APPO-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. autoclass:: ray.rllib.algorithms.appo.appo.APPOConfig
   :members: training


.. _impala:

Importance Weighted Actor-Learner Architecture (IMPALA)
-------------------------------------------------------
`[paper] <https://arxiv.org/abs/1802.01561>`__
`[implementation] <https://github.com/ray-project/ray/blob/master/rllib/algorithms/impala/impala.py>`__
In IMPALA, a central learner runs SGD in a tight loop while asynchronously pulling sample batches from many actor processes. RLlib's IMPALA implementation uses DeepMind's reference `V-trace code <https://github.com/deepmind/scalable_agent/blob/master/vtrace.py>`__. Note that we don't provide a deep residual network out of the box, but one can be plugged in as a `custom model <rllib-models.html#custom-models-tensorflow>`__. Multiple learner GPUs and experience replay are also supported.

.. figure:: images/impala-arch.svg

    IMPALA architecture

Tuned examples: `PongNoFrameskip-v4 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/impala/pong-impala.yaml>`__, `vectorized configuration <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/impala/pong-impala-vectorized.yaml>`__, `multi-gpu configuration <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/impala/pong-impala-fast.yaml>`__, `{BeamRider,Breakout,Qbert,SpaceInvaders}NoFrameskip-v4 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/impala/atari-impala.yaml>`__

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

.. figure:: images/impala.png

   Multi-GPU IMPALA scales up to solve PongNoFrameskip-v4 in ~3 minutes using a pair of V100 GPUs and 128 CPU workers.
   The maximum training throughput reached is ~30k transitions per second (~120k environment frames per second).

**IMPALA-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. autoclass:: ray.rllib.algorithms.impala.impala.ImpalaConfig
   :members: training


Model-based RL
~~~~~~~~~~~~~~

.. _dreamerv3:

DreamerV3
---------
`[paper] <https://arxiv.org/pdf/2301.04104v1.pdf>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/rllib/algorithms/dreamerv3/dreamerv3.py>`__

DreamerV3 trains a world model in supervised fashion using real environment
interactions. The world model's objective is to correctly predict all aspects
of the transition dynamics of the RL environment, which includes (besides predicting the
correct next observations) predicting the received rewards as well as a boolean episode
continuation flag.
An RSSM (recurrent state space model) is used to train in turn the world model
(from actual env data) as well as the critic and actor networks, both of which are trained
on "dreamed" trajectories produced by the world model.

DreamerV3 can be used in all types of environments, including those with image- or vector based
observations, continuous- or discrete actions, as well as sparse or dense reward functions.

Tuned examples: `Atari 100k <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/dreamerv3/atari_100k.py>`__, `Atari 200M <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/dreamerv3/atari_200M.py>`__, `DeepMind Control Suite <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/dreamerv3/dm_control_suite_vision.py>`__


**Pong-v5 results (1, 2, and 4 GPUs)**:

.. figure:: images/dreamerv3/pong_1_2_and_4gpus.svg

    Episode mean rewards for the Pong-v5 environment (with the "100k" setting, in which only 100k environment steps are allowed):
    Note that despite the stable sample efficiency - shown by the constant learning
    performance per env step - the wall time improves almost linearly as we go from 1 to 4 GPUs.
    **Left**: Episode reward over environment timesteps sampled. **Right**: Episode reward over wall-time.


**Atari 100k results (1 vs 4 GPUs)**:

.. figure:: images/dreamerv3/atari100k_1_vs_4gpus.svg

    Episode mean rewards for various Atari 100k tasks on 1 vs 4 GPUs.
    **Left**: Episode reward over environment timesteps sampled.
    **Right**: Episode reward over wall-time.


**DeepMind Control Suite (vision) results (1 vs 4 GPUs)**:

.. figure:: images/dreamerv3/dmc_1_vs_4gpus.svg

    Episode mean rewards for various Atari 100k tasks on 1 vs 4 GPUs.
    **Left**: Episode reward over environment timesteps sampled.
    **Right**: Episode reward over wall-time.


Offline RL and Imitation Learning
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. _bc:

Behavior Cloning (BC; derived from MARWIL implementation)
---------------------------------------------------------
`[paper] <http://papers.nips.cc/paper/7866-exponentially-weighted-imitation-learning-for-batched-historical-data>`__
`[implementation] <https://github.com/ray-project/ray/blob/master/rllib/algorithms/bc/bc.py>`__

Our behavioral cloning implementation is directly derived from our `MARWIL`_ implementation,
with the only difference being the ``beta`` parameter force-set to 0.0. This makes
BC try to match the behavior policy, which generated the offline data, disregarding any resulting rewards.
BC requires the `offline datasets API <rllib-offline.html>`__ to be used.

Tuned examples: `CartPole-v1 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/bc/cartpole-bc.yaml>`__

**BC-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. autoclass:: ray.rllib.algorithms.bc.bc.BCConfig
   :members: training


.. _cql:

Conservative Q-Learning (CQL)
-----------------------------
`[paper] <https://arxiv.org/abs/2006.04779>`__ `[implementation] <https://github.com/ray-project/ray/blob/master/rllib/algorithms/cql/cql.py>`__

In offline RL, the algorithm has no access to an environment, but can only sample from a fixed dataset of pre-collected state-action-reward tuples.
In particular, CQL (Conservative Q-Learning) is an offline RL algorithm that mitigates the overestimation of Q-values outside the dataset distribution via
conservative critic estimates. It does so by adding a simple Q regularizer loss to the standard Bellman update loss.
This ensures that the critic does not output overly-optimistic Q-values. This conservative
correction term can be added on top of any off-policy Q-learning algorithm (here, we provide this for SAC).

RLlib's CQL is evaluated against the Behavior Cloning (BC) benchmark at 500K gradient steps over the dataset. The only difference between the BC- and CQL configs is the ``bc_iters`` parameter in CQL, indicating how many gradient steps we perform over the BC loss. CQL is evaluated on the `D4RL <https://github.com/rail-berkeley/d4rl>`__ benchmark, which has pre-collected offline datasets for many types of environments.

Tuned examples: `HalfCheetah Random <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/cql/halfcheetah-cql.yaml>`__, `Hopper Random <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/cql/hopper-cql.yaml>`__

**CQL-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. autoclass:: ray.rllib.algorithms.cql.cql.CQLConfig
   :members: training

.. _marwil:

Monotonic Advantage Re-Weighted Imitation Learning (MARWIL)
-----------------------------------------------------------
`[paper] <http://papers.nips.cc/paper/7866-exponentially-weighted-imitation-learning-for-batched-historical-data>`__
`[implementation] <https://github.com/ray-project/ray/blob/master/rllib/algorithms/marwil/marwil.py>`__

MARWIL is a hybrid imitation learning and policy gradient algorithm suitable for training on batched historical data.
When the ``beta`` hyperparameter is set to zero, the MARWIL objective reduces to vanilla imitation learning (see `BC`_).
MARWIL requires the `offline datasets API <rllib-offline.html>`__ to be used.

Tuned examples: `CartPole-v1 <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples/marwil/cartpole-marwil.yaml>`__

**MARWIL-specific configs** (see also `common configs <rllib-training.html#common-parameters>`__):

.. autoclass:: ray.rllib.algorithms.marwil.marwil.MARWILConfig
   :members: training
