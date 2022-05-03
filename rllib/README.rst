RLlib: Industry-Grade Reinforcement Learning with TF and Torch
==============================================================

**RLlib** is an open-source library for reinforcement learning (RL), offering support for
production-level, highly distributed RL workloads, while maintaining
unified and simple APIs for a large variety of industry applications.

Whether you would like to train your agents in multi-agent setups,
purely from offline (historic) datasets, or using externally
connected simulators, RLlib offers simple solutions for your decision making needs.

You **don't need** to be an **RL expert** to use RLlib, nor do you need to learn Ray or any
other of its libraries! If you either have your problem coded (in python) as an
`RL environment <https://medium.com/distributed-computing-with-ray/anatomy-of-a-custom-environment-for-rllib-327157f269e5>`_
or own lots of pre-recorded, historic behavioral data to learn from, you will be
up and running in only a few days.

RLlib is already used in production by industry leaders in many different verticals, such as
`climate control <https://www.anyscale.com/events/2021/06/23/applying-ray-and-rllib-to-real-life-industrial-use-cases>`_,
`manufacturing and logistics <https://www.anyscale.com/events/2021/06/22/offline-rl-with-rllib>`_,
`finance <https://www.anyscale.com/events/2021/06/22/a-24x-speedup-for-reinforcement-learning-with-rllib-+-ray>`_,
`gaming <https://www.anyscale.com/events/2021/06/22/using-reinforcement-learning-to-optimize-iap-offer-recommendations-in-mobile-games>`_,
`automobile <https://www.anyscale.com/events/2021/06/23/using-rllib-in-an-enterprise-scale-reinforcement-learning-solution>`_,
`robotics <https://www.anyscale.com/events/2021/06/23/introducing-amazon-sagemaker-kubeflow-reinforcement-learning-pipelines-for>`_,
`boat design <https://www.youtube.com/watch?v=cLCK13ryTpw>`_,
and many others.


Installation and Setup
----------------------

Install RLlib and run your first experiment on your laptop in seconds:

**TensorFlow:**

.. code-block:: bash

    $ conda create -n rllib python=3.8
    $ conda activate rllib
    $ pip install "ray[rllib]" tensorflow "gym[atari]" "gym[accept-rom-license]" atari_py
    $ # Run a test job:
    $ rllib train --run APPO --env CartPole-v0


**PyTorch:**

.. code-block:: bash

    $ conda create -n rllib python=3.8
    $ conda activate rllib
    $ pip install "ray[rllib]" torch "gym[atari]" "gym[accept-rom-license]" atari_py
    $ # Run a test job:
    $ rllib train --run APPO --env CartPole-v0 --torch


Algorithms Supported
----------------------

Offline RL:  

- `Behavior Cloning (BC; derived from MARWIL implementation) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#bc>`__ 
- `Conservative Q-Learning (CQL) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#cql>`__ 
- `Importance Sampling and Weighted Importance Sampling (OPE) <https://docs.ray.io/en/latest/rllib/rllib-offline.html#is>`__ 
- `Monotonic Advantage Re-Weighted Imitation Learning (MARWIL) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#marwil>`__ 

Model-free On-policy RL (for Games):

- `Synchronous Proximal Policy Optimization (APPO) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#appo>`__ 
- `Decentralized Distributed Proximal Policy Optimization (DD-PPO)  <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#ddppo>`__ 
- `Proximal Policy Optimization (PPO) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#ppo>`__ 
- `Importance Weighted Actor-Learner Architecture (IMPALA) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#impala>`__   
- `Advantage Actor-Critic (A2C, A3C) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#a3c>`__ 
- `Vanilla Policy Gradient (PG) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#pg>`__ 
- `Model-agnostic Meta-Learning (contrib/MAML) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#maml>`__ 

Model-free Off-policy RL:

- `Distributed Prioritized Experience Replay (Ape-X DQN, Ape-X DDPG)] <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#apex>`__ 
- `Recurrent Replay Distributed DQN (R2D2) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#r2d2>`__ 
- `Deep Q Networks (DQN, Rainbow, Parametric DQN) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#dqn>`__ 
- `Deep Deterministic Policy Gradients (DDPG, TD3) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#ddpg>`__ 
- `Soft Actor Critic (SAC) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#sac>`__ 

Model-based RL: 

- `Image-only Dreamer (contrib/Dreamer) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#dreamer>`__ 
- `Model-Based Meta-Policy-Optimization (MB-MPO) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#mbmpo>`__ 

Derivative-free algorithms: 

- `Augmented Random Search (ARS) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#ars>`__ 
- `Evolution Strategies (ES) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#es>`__ 

RL for recommender systems: 

- `SlateQ <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#slateq>`__ 

Bandits: 

- `Linear Upper Confidence Bound (BanditLinUCBTrainer) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#lin-ucb>`__ 
- `Linear Thompson Sampling (BanditLinTSTrainer) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#lints>`__ 

Multi-agent:  

- `Single-Player Alpha Zero (contrib/AlphaZero)  <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#alphazero>`__ 
- `Parameter Sharing <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#parameter>`__ 
- `QMIX Monotonic Value Factorisation (QMIX, VDN, IQN)) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#qmix>`__ 
- `Multi-Agent Deep Deterministic Policy Gradient (contrib/MADDPG) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#maddpg>`__ 
- `Shared Critic Methods <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#sc>`__ 

Others:  

- `Curiosity (ICM: Intrinsic Curiosity Module) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#curiosity>`__ 
- `Random encoders (contrib/RE3) <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#re3>`__ 
- `Fully Independent Learning <https://docs.ray.io/en/master/rllib/rllib-algorithms.html#fil>`__ 

A list of all the algorithms can be found `here <https://docs.ray.io/en/master/rllib/rllib-algorithms.html>`__ .  


Quick First Experiment
----------------------

.. code-block:: python

    import gym
    from ray.rllib.agents.ppo import PPOTrainer


    # Define your problem using python and openAI's gym API:
    class ParrotEnv(gym.Env):
        """Environment in which an agent must learn to repeat the seen observations.

        Observations are float numbers indicating the to-be-repeated values,
        e.g. -1.0, 5.1, or 3.2.

        The action space is always the same as the observation space.

        Rewards are r=-abs(observation - action), for all steps.
        """

        def __init__(self, config):
            # Make the space (for actions and observations) configurable.
            self.action_space = config.get(
                "parrot_shriek_range", gym.spaces.Box(-1.0, 1.0, shape=(1, )))
            # Since actions should repeat observations, their spaces must be the
            # same.
            self.observation_space = self.action_space
            self.cur_obs = None
            self.episode_len = 0

        def reset(self):
            """Resets the episode and returns the initial observation of the new one.
            """
            # Reset the episode len.
            self.episode_len = 0
            # Sample a random number from our observation space.
            self.cur_obs = self.observation_space.sample()
            # Return initial observation.
            return self.cur_obs

        def step(self, action):
            """Takes a single step in the episode given `action`

            Returns:
                New observation, reward, done-flag, info-dict (empty).
            """
            # Set `done` flag after 10 steps.
            self.episode_len += 1
            done = self.episode_len >= 10
            # r = -abs(obs - action)
            reward = -sum(abs(self.cur_obs - action))
            # Set a new observation (random sample).
            self.cur_obs = self.observation_space.sample()
            return self.cur_obs, reward, done, {}


    # Create an RLlib Trainer instance to learn how to act in the above
    # environment.
    trainer = PPOTrainer(
        config={
            # Env class to use (here: our gym.Env sub-class from above).
            "env": ParrotEnv,
            # Config dict to be passed to our custom env's constructor.
            "env_config": {
                "parrot_shriek_range": gym.spaces.Box(-5.0, 5.0, (1, ))
            },
            # Parallelize environment rollouts.
            "num_workers": 3,
        })

    # Train for n iterations and report results (mean episode rewards).
    # Since we have to guess 10 times and the optimal reward is 0.0
    # (exact match between observation and action value),
    # we can expect to reach an optimal episode reward of 0.0.
    for i in range(5):
        results = trainer.train()
        print(f"Iter: {i}; avg. reward={results['episode_reward_mean']}")


After training, you may want to perform action computations (inference) in your environment.
Below is a minimal example on how to do this. Also
`check out our more detailed examples here <https://github.com/ray-project/ray/tree/master/rllib/examples/inference_and_serving>`_
(in particular for `normal models <https://github.com/ray-project/ray/blob/master/rllib/examples/inference_and_serving/policy_inference_after_training.py>`_,
`LSTMs <https://github.com/ray-project/ray/blob/master/rllib/examples/inference_and_serving/policy_inference_after_training_with_lstm.py>`_,
and `attention nets <https://github.com/ray-project/ray/blob/master/rllib/examples/inference_and_serving/policy_inference_after_training_with_attention.py>`_).


.. code-block:: python

    # Perform inference (action computations) based on given env observations.
    # Note that we are using a slightly simpler env here (-3.0 to 3.0, instead
    # of -5.0 to 5.0!), however, this should still work as the agent has
    # (hopefully) learned to "just always repeat the observation!".
    env = ParrotEnv({"parrot_shriek_range": gym.spaces.Box(-3.0, 3.0, (1, ))})
    # Get the initial observation (some value between -10.0 and 10.0).
    obs = env.reset()
    done = False
    total_reward = 0.0
    # Play one episode.
    while not done:
        # Compute a single action, given the current observation
        # from the environment.
        action = trainer.compute_single_action(obs)
        # Apply the computed action in the environment.
        obs, reward, done, info = env.step(action)
        # Sum up rewards for reporting purposes.
        total_reward += reward
    # Report results.
    print(f"Shreaked for 1 episode; total-reward={total_reward}")


For a more detailed `"60 second" example, head to our main documentation  <https://docs.ray.io/en/master/rllib/index.html>`_.


Highlighted Features
--------------------

The following is a summary of RLlib's most striking features (for an in-depth overview,
check out our `documentation <http://docs.ray.io/en/master/rllib/index.html>`_):

The most **popular deep-learning frameworks**: `PyTorch <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_torch_policy.py>`_ and `TensorFlow
(tf1.x/2.x static-graph/eager/traced) <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_tf_policy.py>`_.

**Highly distributed learning**: Our RLlib algorithms (such as our "PPO" or "IMPALA")
allow you to set the ``num_workers`` config parameter, such that your workloads can run
on 100s of CPUs/nodes thus parallelizing and speeding up learning.

**Vectorized (batched) and remote (parallel) environments**: RLlib auto-vectorizes
your ``gym.Envs`` via the ``num_envs_per_worker`` config. Environment workers can
then batch and thus significantly speedup the action computing forward pass.
On top of that, RLlib offers the ``remote_worker_envs`` config to create
`single environments (within a vectorized one) as ray Actors <https://github.com/ray-project/ray/blob/master/rllib/examples/remote_base_env_with_custom_api.py>`_,
thus parallelizing even the env stepping process.

| **Multi-agent RL** (MARL): Convert your (custom) ``gym.Envs`` into a multi-agent one
  via a few simple steps and start training your agents in any of the following fashions:
| 1) Cooperative with `shared <https://github.com/ray-project/ray/blob/master/rllib/examples/centralized_critic.py>`_ or
  `separate <https://github.com/ray-project/ray/blob/master/rllib/examples/two_step_game.py>`_
  policies and/or value functions.
| 2) Adversarial scenarios using `self-play <https://github.com/ray-project/ray/blob/master/rllib/examples/self_play_with_open_spiel.py>`_
  and `league-based training <https://github.com/ray-project/ray/blob/master/rllib/examples/self_play_league_based_with_open_spiel.py>`_.
| 3) `Independent learning <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent_independent_learning.py>`_
  of neutral/co-existing agents.


**External simulators**: Don't have your simulation running as a gym.Env in python?
No problem! RLlib supports an external environment API and comes with a pluggable,
off-the-shelve
`client <https://github.com/ray-project/ray/blob/master/rllib/examples/serving/cartpole_client.py>`_/
`server <https://github.com/ray-project/ray/blob/master/rllib/examples/serving/cartpole_server.py>`_
setup that allows you to run 100s of independent simulators on the "outside"
(e.g. a Windows cloud) connecting to a central RLlib Policy-Server that learns
and serves actions. Alternatively, actions can be computed on the client side
to save on network traffic.

**Offline RL and imitation learning/behavior cloning**: You don't have a simulator
for your particular problem, but tons of historic data recorded by a legacy (maybe
non-RL/ML) system? This branch of reinforcement learning is for you!
RLlib's comes with several `offline RL <https://github.com/ray-project/ray/blob/master/rllib/examples/offline_rl.py>`_
algorithms (*CQL*, *MARWIL*, and *DQfD*), allowing you to either purely
`behavior-clone <https://github.com/ray-project/ray/blob/master/rllib/agents/marwil/tests/test_bc.py>`_
your existing system or learn how to further improve over it.


In-Depth Documentation
----------------------

For an in-depth overview of RLlib and everything it has to offer, including
hand-on tutorials of important industry use cases and workflows, head over to
our `documentation pages <https://docs.ray.io/en/master/rllib/index.html>`_.


Cite our Paper
--------------

If you've found RLlib useful for your research, please cite our `paper <https://arxiv.org/abs/1712.09381>`_ as follows:

.. code-block::

    @inproceedings{liang2018rllib,
        Author = {Eric Liang and
                  Richard Liaw and
                  Robert Nishihara and
                  Philipp Moritz and
                  Roy Fox and
                  Ken Goldberg and
                  Joseph E. Gonzalez and
                  Michael I. Jordan and
                  Ion Stoica},
        Title = {{RLlib}: Abstractions for Distributed Reinforcement Learning},
        Booktitle = {International Conference on Machine Learning ({ICML})},
        Year = {2018}
    }
