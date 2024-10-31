.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst


Examples
========

This page contains an index of all the python scripts in the
`examples folder <https://github.com/ray-project/ray/blob/master/rllib/examples>`__
of RLlib, demonstrating the different use cases and features of the library.

.. note::

    RLlib is currently in a transition state from "old API stack" to "new API stack".
    Some of the examples here haven't been translated yet to the new stack and are tagged
    with the following comment line on top: ``# @OldAPIStack``. The moving of all example
    scripts over to the "new API stack" is work in progress and expected to be completed
    by the end of 2024.

.. note::

    If any new-API-stack example is broken, or if you'd like to add an example to this page,
    feel free to raise an issue on `RLlib's github repository <https://github.com/ray-project/ray/issues/new/choose>`__.


Folder Structure
++++++++++++++++
The `examples folder <https://github.com/ray-project/ray/blob/master/rllib/examples>`__ is
structured into several sub-directories, the contents of all of which are described in detail below.


How to run an example script
++++++++++++++++++++++++++++

Most of the example scripts are self-executable, meaning you can just ``cd`` into the respective
directory and run the script as-is with python:

.. code-block:: bash

    $ cd ray/rllib/examples/multi_agent
    $ python multi_agent_pendulum.py --enable-new-api-stack --num-agents=2


Use the `--help` command line argument to have each script print out its supported command line options.

Most of the scripts share a common subset of generally applicable command line arguments,
for example `--num-env-runners`, `--no-tune`, or `--wandb-key`.


All example sub-folders
+++++++++++++++++++++++


Algorithms
----------

- `How to write a custom Algorith.training_step() method combining on- and off-policy training <https://github.com/ray-project/ray/blob/master/rllib/examples/algorithms/custom_training_step_on_and_off_policy_combined.py>`__:
   Example of how to override the :py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.training_step` method
   to train two different policies in parallel (also using multi-agent API).

Checkpoints
-----------

- `How to extract a checkpoint from n Tune trials using one or more custom criteria. <https://github.com/ray-project/ray/blob/master/rllib/examples/checkpoints/checkpoint_by_custom_criteria.py>`__:
   Example of how to find a :ref:`checkpoint <rllib-saving-and-loading-algos-and-policies-docs>` after a `Tuner.fit()` with some custom defined criteria.

Connectors
----------

.. note::
    RLlib's Connector API has been re-written from scratch for the new API stack.
    Connector-pieces and -pipelines are now referred to as :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2`
    (as opposed to ``Connector``, which only continue to work on the old API stack).


- `How to frame-stack Atari image observations <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/frame_stacking.py>`__:
   An example using Atari framestacking in a very efficient manner, not in the environment itself (as a `gym.Wrapper`),
   but by stacking the observations on-the-fly using `EnvToModule` and `LearnerConnector` pipelines.
   This method of framestacking is more efficient as it avoids having to send large observation
   tensors through the network (ray).

- `How to mean/std-filter observations <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/mean_std_filtering.py>`__:
   An example of a :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` that filters all observations from the environment using a
   plain mean/std-filter (shift by the mean and divide by std-dev). This example demonstrates
   how a stateful :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` class has its states
   (here the means and standard deviations of the individual observation items) coming from the different
   :py:class:`~ray.rllib.env.env_runner.EnvRunner` instances a) merged into one common state and
   then b) broadcast again back to the remote :py:class:`~ray.rllib.env.env_runner.EnvRunner` workers.

- `How to include previous-actions and/or previous rewards in RLModule inputs <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/prev_actions_prev_rewards.py>`__:
   An example of a :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` that adds the n previous actions
   and/or the m previous rewards to the RLModule's input dict (to perform its forward passes, both
   for inference and training).

- `How to train with nested action spaces <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/nested_action_spaces.py>`__:
   Learning in arbitrarily nested action spaces, using an env in which the action space equals the
   observation space (both are complex, nested Dicts) and the policy has to pick actions
   that closely match (or are identical) to the previously seen observations.

- `How to train with nested observation spaces <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/nested_observation_spaces.py>`__:
   Learning in arbitrarily nested observation spaces
   (using a CartPole-v1 variant with a nested Dict observation space).

Curriculum Learning
-------------------

-  `How to set up curriculum learning with the custom callbacks API <https://github.com/ray-project/ray/blob/master/rllib/examples/curriculum/curriculum_learning.py>`__:
    Example of how to make the environment go through different levels of difficulty (from easy to harder to solve)
    and thus help the learning algorithm to cope with an otherwise unsolvable task.
    Also see the :doc:`curriculum learning how-to </rllib/rllib-advanced-api>` from the documentation.

Environments
------------
- `How to register a custom gymnasium environment <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/custom_gym_env.py>`__:
   Example showing how to write your own RL environment using ``gymnasium`` and register it to run train your algorithm against this env with RLlib.

- `How to set up rendering and recording of the environment trajectories during training with W&B <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/env_rendering_and_recording.py>`__:
   Example showing how you can render and record episode trajectories of your gymnasium envs and log the videos to WandB.

Evaluation
----------

- `How to run evaluation with a custom evaluation function <https://github.com/ray-project/ray/blob/master/rllib/examples/evaluation/custom_evaluation.py>`__:
   Example of how to write a custom evaluation function that's called instead of the default behavior, which is running with the evaluation worker set through n episodes.

- `How to run evaluation in parallel to training <https://github.com/ray-project/ray/blob/master/rllib/examples/evaluation/evaluation_parallel_to_training.py>`__:
   Example showing how the evaluation workers and the "normal" rollout workers can run (to some extend) in parallel to speed up training.

GPU (for Training and Sampling)
-------------------------------

- `How to use fractional GPUs for training an RLModule <https://github.com/ray-project/ray/blob/master/rllib/examples/gpus/fractional_gpus_per_learner.py>`__:
   If your model is small and easily fits on a single GPU and you want to therefore train
   other models alongside it to save time and cost, this script shows you how to set up
   your RLlib config with a fractional number of GPUs on the learner (model training)
   side.

Inference (of Models/Policies)
------------------------------

- `How to do inference with an already trained policy <https://github.com/ray-project/ray/blob/master/rllib/examples/inference/policy_inference_after_training.py>`__:
   Example of how to perform inference (compute actions) on an already trained policy.
- `How to do inference with an already trained (LSTM) policy <https://github.com/ray-project/ray/blob/master/rllib/examples/inference/policy_inference_after_training_with_lstm.py>`__:
   Example of how to perform inference (compute actions) on an already trained (LSTM) policy.

Multi-Agent RL
--------------

- `How to set up independent multi-agent training <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/pettingzoo_independent_learning.py>`__:
   Set up RLlib to run any algorithm in (independent) multi-agent mode against a multi-agent environment.
- `How to set up shared-parameter multi-agent training <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/pettingzoo_parameter_sharing.py>`__:
   Set up RLlib to run any algorithm in (shared-parameter) multi-agent mode against a multi-agent environment.
- `How to compare a heuristic policy versus a trained one on rock-paper-scissors <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/rock_paper_scissors_heuristic_vs_learned.py>`__ and `Rock-paper-scissors learned vs learned <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/rock_paper_scissors_learned_vs_learned.py>`__:
   Two examples of different heuristic and learned policies competing against each other in the rock-paper-scissors environment.
- `How to use agent grouping in a multi-agent environment (two-step game) <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/two_step_game_with_grouped_agents.py>`__:
   Example on how to use agent grouping in a multi-agent environment (the two-step game from the `QMIX paper <https://arxiv.org/pdf/1803.11485.pdf>`__).
- `How to set up multi-agent training versus a PettingZoo environment <https://github.com/Farama-Foundation/PettingZoo/blob/master/tutorials/Ray/rllib_pistonball.py>`__:
   Example on how to use RLlib to learn in `PettingZoo <https://www.pettingzoo.ml>`__ multi-agent environments.
- `How to hand-code a (heuristic) policy <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/custom_heuristic_policy.py>`__:
   Example of running a custom hand-coded policy alongside trainable policies.
- `How to train a single policy (weight sharing) controlling more than one agent <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/multi_agent_cartpole.py>`__:
   Example of how to define weight-sharing layers between two different policies.

Ray Serve and RLlib
-------------------

- `How to use a trained RLlib algorithm with Ray Serve <https://github.com/ray-project/ray/tree/master/rllib/examples/ray_serve/ray_serve_with_rllib.py>`__
   This script offers a simple workflow for 1) training a policy with RLlib first, 2) creating a new policy 3) restoring its weights from the trained
   one and serving the new policy with Ray Serve.

Ray Tune and RLlib
------------------
- `How to define a custom progress reporter and use it with Ray Tune and RLlib <https://github.com/ray-project/ray/blob/master/rllib/examples/ray_tune/custom_progress_reported.py>`__:
   Example of how to write your own progress reporter (for a multi-agent experiment) and use it with Ray Tune and RLlib.

- `How to define and plug in your custom logger into Ray Tune and RLlib <https://github.com/ray-project/ray/blob/master/rllib/examples/ray_tune/custom_logger.py>`__:
   How to setup a custom Logger object in RLlib and use it with Ray Tune.

- `How to custom tune an experiment <https://github.com/ray-project/ray/blob/master/rllib/examples/ray_tune/custom_experiment.py>`__:
   How to run a custom Ray Tune experiment with RLlib with custom training- and evaluation phases.

RLModules
---------

- `How to configure an autoregressive action distribution <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/autoregressive_actions.py>`__:
   Learning with an auto-regressive action distribution (for example, two action components, where distribution of the second component depends on the first's actually sampled value).


Tuned Examples
++++++++++++++

The `tuned examples <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples>`__ folder
contains python config files (yaml for the old API stack) that can be executed analogously to
all other example scripts described here in order to run tuned learning experiments
for the different algorithms and different environment types.

For example, see this tuned Atari example for PPO, which learns to solve the Pong environment
in roughly 5min. It can be run like this on a single g5.24xlarge (or g6.24xlarge) machine with
4 GPUs and 96 CPUs:

.. code-block:: bash

    $ cd ray/rllib/tuned_examples/ppo
    $ python atari_ppo.py --env=ale_py:ALE/Pong-v5 --num-learners=4 --num-env-runners=95

Note that some of the files in this folder are used for RLlib's daily or weekly
release tests as well.


Community Examples
++++++++++++++++++
- `Arena AI <https://sites.google.com/view/arena-unity/home>`__:
   A General Evaluation Platform and Building Toolkit for Single/Multi-Agent Intelligence
   with RLlib-generated baselines.
- `CARLA <https://github.com/layssi/Carla_Ray_Rlib>`__:
   Example of training autonomous vehicles with RLlib and `CARLA <http://carla.org/>`__ simulator.
- `The Emergence of Adversarial Communication in Multi-Agent Reinforcement Learning <https://arxiv.org/pdf/2008.02616.pdf>`__:
   Using Graph Neural Networks and RLlib to train multiple cooperative and adversarial agents to solve the
   "cover the area"-problem, thereby learning how to best communicate (or - in the adversarial case - how to disturb communication) (`code <https://github.com/proroklab/adversarial_comms>`__).
- `Flatland <https://flatland.aicrowd.com/intro.html>`__:
   A dense traffic simulating environment with RLlib-generated baselines.
- `GFootball <https://github.com/google-research/football/blob/master/gfootball/examples/run_multiagent_rllib.py>`__:
   Example of setting up a multi-agent version of `GFootball <https://github.com/google-research>`__ with RLlib.
- `mobile-env <https://github.com/stefanbschneider/mobile-env>`__:
   An open, minimalist Gymnasium environment for autonomous coordination in wireless mobile networks.
   Includes an example notebook using Ray RLlib for multi-agent RL with mobile-env.
- `Neural MMO <https://github.com/NeuralMMO/environment>`__:
   A multiagent AI research environment inspired by Massively Multiplayer Online (MMO) role playing games –
   self-contained worlds featuring thousands of agents per persistent macrocosm, diverse skilling systems, local and global economies, complex emergent social structures,
   and ad-hoc high-stakes single and team based conflict.
- `NeuroCuts <https://github.com/neurocuts/neurocuts>`__:
   Example of building packet classification trees using RLlib / multi-agent in a bandit-like setting.
- `NeuroVectorizer <https://github.com/ucb-bar/NeuroVectorizer>`__:
   Example of learning optimal LLVM vectorization compiler pragmas for loops in C and C++ codes using RLlib.
- `Roboschool / SageMaker <https://github.com/aws/amazon-sagemaker-examples/tree/0cd3e45f425b529bf06f6155ca71b5e4bc515b9b/reinforcement_learning/rl_roboschool_ray>`__:
   Example of training robotic control policies in SageMaker with RLlib.
- `Sequential Social Dilemma Games <https://github.com/eugenevinitsky/sequential_social_dilemma_games>`__:
   Example of using the multi-agent API to model several `social dilemma games <https://arxiv.org/abs/1702.03037>`__.
- `Simple custom environment for single RL with Ray and RLlib <https://github.com/lcipolina/Ray_tutorials/blob/main/RLLIB_Ray2_0.ipynb>`__:
   Create a custom environment and train a single agent RL using Ray 2.0 with Tune.
- `StarCraft2 <https://github.com/oxwhirl/smac>`__:
   Example of training in StarCraft2 maps with RLlib / multi-agent.
- `Traffic Flow <https://berkeleyflow.readthedocs.io/en/latest/flow_setup.html>`__:
   Example of optimizing mixed-autonomy traffic simulations with RLlib / multi-agent.


Blog Posts
++++++++++

- `Attention Nets and More with RLlib’s Trajectory View API <https://medium.com/distributed-computing-with-ray/attention-nets-and-more-with-rllibs-trajectory-view-api-d326339a6e65>`__:
   Blog describing RLlib's new "trajectory view API" and how it enables implementations of GTrXL (attention net) architectures.
- `Reinforcement Learning with RLlib in the Unity Game Engine <https://medium.com/distributed-computing-with-ray/reinforcement-learning-with-rllib-in-the-unity-game-engine-1a98080a7c0d>`__:
   How-To guide about connecting RLlib with the Unity3D game engine for running visual- and physics-based RL experiments.
- `Lessons from Implementing 12 Deep RL Algorithms in TF and PyTorch <https://medium.com/distributed-computing-with-ray/lessons-from-implementing-12-deep-rl-algorithms-in-tf-and-pytorch-1b412009297d>`__:
   Discussion on how the Ray Team ported 12 of RLlib's algorithms from TensorFlow to PyTorch and the lessons learned.
- `Scaling Multi-Agent Reinforcement Learning <http://bair.berkeley.edu/blog/2018/12/12/rllib>`__:
   Blog post of a brief tutorial on multi-agent RL and its design in RLlib.
- `Functional RL with Keras and TensorFlow Eager <https://medium.com/riselab/functional-rl-with-keras-and-tensorflow-eager-7973f81d6345>`__:
   Exploration of a functional paradigm for implementing reinforcement learning (RL) algorithms.
