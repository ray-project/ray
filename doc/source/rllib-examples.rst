RLlib Examples
==============

This page is an index of examples for the various use cases and features of RLlib.

If any example is broken, or if you'd like to add an example to this page, feel free to raise an issue on our Github repository.

Tuned Examples
--------------

- `Tuned examples <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples>`__:
   Collection of tuned hyperparameters by algorithm.
- `MuJoCo and Atari benchmarks <https://github.com/ray-project/rl-experiments>`__:
   Collection of reasonably optimized Atari and MuJoCo results.

Blog Posts
----------

- `Scaling Multi-Agent Reinforcement Learning <http://bair.berkeley.edu/blog/2018/12/12/rllib>`__:
   This blog post is a brief tutorial on multi-agent RL and its design in RLlib.
- `Functional RL with Keras and TensorFlow Eager <https://medium.com/riselab/functional-rl-with-keras-and-tensorflow-eager-7973f81d6345>`__:
   Exploration of a functional paradigm for implementing reinforcement learning (RL) algorithms.

Training Workflows
------------------

- `Custom training workflows <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_train_fn.py>`__:
   Example of how to use Tune's support for custom training functions to implement custom training workflows.
- `Curriculum learning <rllib-training.html#example-curriculum-learning>`__:
   Example of how to adjust the configuration of an environment over time.
- `Custom metrics <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_metrics_and_callbacks.py>`__:
   Example of how to output custom training metrics to TensorBoard.
- `Using rollout workers directly for control over the whole training workflow <https://github.com/ray-project/ray/blob/master/rllib/examples/rollout_worker_custom_workflow.py>`__:
   Example of how to use RLlib's lower-level building blocks to implement a fully customized training workflow.

Custom Envs and Models
----------------------

- `Local Unity3D multi-agent environment example <https://github.com/ray-project/ray/tree/master/rllib/examples/unity3d_env_local.py>`__:
   Example of how to setup an RLlib Trainer against a locally running Unity3D editor instance to
   learn any Unity3D game (including support for multi-agent).
   Use this example to try things out and watch the game and the learning progress live in the editor.
   Providing a compiled game, this example could also run in distributed fashion with `num_workers > 0`.
   For a more heavy-weight, distributed, cloud-based example, see `Unity3D client/server`_ below.
- `Registering a custom env and model <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_env.py>`__:
   Example of defining and registering a gym env and model for use with RLlib.
- `Custom Keras model <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_keras_model.py>`__:
   Example of using a custom Keras model.
- `Custom Keras RNN model <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_rnn_model.py>`__:
   Example of using a custom Keras- or PyTorch RNN model.
- `Registering a custom model with supervised loss <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_loss.py>`__:
   Example of defining and registering a custom model with a supervised loss.
- `Subprocess environment <https://github.com/ray-project/ray/blob/master/rllib/tests/test_env_with_subprocess.py>`__:
   Example of how to ensure subprocesses spawned by envs are killed when RLlib exits.
- `Batch normalization <https://github.com/ray-project/ray/blob/master/rllib/examples/batch_norm_model.py>`__:
   Example of adding batch norm layers to a custom model.
- `Parametric actions <https://github.com/ray-project/ray/blob/master/rllib/examples/parametric_actions_cartpole.py>`__:
   Example of how to handle variable-length or parametric action spaces.
- `Eager execution <https://github.com/ray-project/ray/blob/master/rllib/examples/eager_execution.py>`__:
   Example of how to leverage TensorFlow eager to simplify debugging and design of custom models and policies.

Serving and Offline
-------------------

.. _Unity3D client/server:

- `Unity3D client/server <https://github.com/ray-project/ray/tree/master/rllib/examples/serving/unity3d_server.py>`__:
   Example of how to setup n distributed Unity3D (compiled) games in the cloud that function as data collecting
   clients against a central RLlib Policy server learning how to play the game.
   The n distributed clients could themselves be servers for external/human players and allow for control
   being fully in the hands of the Unity entities instead of RLlib.
   Note: Uses Unity's MLAgents SDK (>=1.0) and supports all provided MLAgents example games and multi-agent setups.
- `CartPole client/server <https://github.com/ray-project/ray/tree/master/rllib/examples/serving/cartpole_server.py>`__:
   Example of online serving of predictions for a simple CartPole policy.
- `Saving experiences <https://github.com/ray-project/ray/blob/master/rllib/examples/saving_experiences.py>`__:
   Example of how to externally generate experience batches in RLlib-compatible format.

Multi-Agent and Hierarchical
----------------------------

- `Rock-paper-scissors <https://github.com/ray-project/ray/blob/master/rllib/examples/rock_paper_scissors_multiagent.py>`__:
   Example of different heuristic and learned policies competing against each other in rock-paper-scissors.
- `Two-step game <https://github.com/ray-project/ray/blob/master/rllib/examples/two_step_game.py>`__:
   Example of the two-step game from the `QMIX paper <https://arxiv.org/pdf/1803.11485.pdf>`__.
- `PPO with centralized critic on two-step game <https://github.com/ray-project/ray/blob/master/rllib/examples/centralized_critic.py>`__:
   Example of customizing PPO to leverage a centralized value function.
- `Centralized critic in the env <https://github.com/ray-project/ray/blob/master/rllib/examples/centralized_critic_2.py>`__:
   A simpler method of implementing a centralized critic by augmentating agent observations with global information.
- `Hand-coded policy <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent_custom_policy.py>`__:
   Example of running a custom hand-coded policy alongside trainable policies.
- `Weight sharing between policies <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent_cartpole.py>`__:
   Example of how to define weight-sharing layers between two different policies.
- `Multiple trainers <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent_two_trainers.py>`__:
   Example of alternating training between two DQN and PPO trainers.
- `Hierarchical training <https://github.com/ray-project/ray/blob/master/rllib/examples/hierarchical_training.py>`__:
   Example of hierarchical training using the multi-agent API.

Community Examples
------------------
- `Arena AI <https://sites.google.com/view/arena-unity/home>`__:
   A General Evaluation Platform and Building Toolkit for Single/Multi-Agent Intelligence
   with RLlib-generated baselines.
- `CARLA <https://github.com/layssi/Carla_Ray_Rlib>`__:
   Example of training autonomous vehicles with RLlib and `CARLA <http://carla.org/>`__ simulator.
- `The Emergence of Adversarial Communication in Multi-Agent Reinforcement Learning <https://arxiv.org/pdf/2008.02616.pdf>`__:
   Using Graph Neural Networks and RLlib to train multiple cooperative and adversarial agents to solve the
   "cover the area"-problem, thereby learning how to best communicate (or - in the adversarial case - how to disturb communication).
- `Flatland <https://flatland.aicrowd.com/intro.html>`__:
   A dense traffic simulating environment with RLlib-generated baselines.
- `GFootball <https://github.com/google-research/football/blob/master/gfootball/examples/run_multiagent_rllib.py>`__:
   Example of setting up a multi-agent version of `GFootball <https://github.com/google-research>`__ with RLlib.
- `Neural MMO <https://jsuarez5341.github.io/neural-mmo/build/html/rst/userguide.html>`__:
   A multiagent AI research environment inspired by Massively Multiplayer Online (MMO) role playing games –
   self-contained worlds featuring thousands of agents per persistent macrocosm, diverse skilling systems, local and global economies, complex emergent social structures,
   and ad-hoc high-stakes single and team based conflict.
- `NeuroCuts <https://github.com/neurocuts/neurocuts>`__:
   Example of building packet classification trees using RLlib / multi-agent in a bandit-like setting.
- `NeuroVectorizer <https://github.com/ucb-bar/NeuroVectorizer>`__:
   Example of learning optimal LLVM vectorization compiler pragmas for loops in C and C++ codes using RLlib.
- `Roboschool / SageMaker <https://github.com/awslabs/amazon-sagemaker-examples/tree/master/reinforcement_learning/rl_roboschool_ray>`__:
   Example of training robotic control policies in SageMaker with RLlib.
- `Sequential Social Dilemma Games <https://github.com/eugenevinitsky/sequential_social_dilemma_games>`__:
   Example of using the multi-agent API to model several `social dilemma games <https://arxiv.org/abs/1702.03037>`__.
- `StarCraft2 <https://github.com/oxwhirl/smac>`__:
   Example of training in StarCraft2 maps with RLlib / multi-agent.
- `Traffic Flow <https://berkeleyflow.readthedocs.io/en/latest/flow_setup.html>`__:
   Example of optimizing mixed-autonomy traffic simulations with RLlib / multi-agent.
