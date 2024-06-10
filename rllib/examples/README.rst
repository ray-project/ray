.. |newstack| image:: images/tensorflow.png
    :class: inline-figure
    :width: 16


Examples
========

This page contains an index of all the python scripts in the `examples/ folder <https://github.com/ray-project/ray/blob/master/rllib/examples>`
of RLlib, demonstrating the different use cases and features of the library.

.. note::

    RLlib is currently in a transition state from "old API stack" to "new API stack".
    Some of the examples here haven't been translated yet to the new stack and are tagged
    with the following comment line on top: ``# @OldAPIStack``. The moving of all example
    scripts over to the "new API stack" is work in progress and expected to be completed
    by the end of 2024.

.. note::

    If any new-API-stack example is broken, or if you'd like to add an example to this page,
    feel free to raise an issue on RLlib's `Github repository <https://github.com/ray-project/ray/issues/new/choose>`.


Folder Structure
++++++++++++++++
The `examples/ folder <https://github.com/ray-project/ray/blob/master/rllib/examples>` is
structured into several sub-directories, the contents of all of which are described in detail below.


How to run an example script
++++++++++++++++++++++++++++

Most of the example scripts are self-executable, meaning you can just ``cd`` into the respective
directory and type:

.. code-block:: bash

    $ cd examples/multi_agent
    $ python multi_agent_pendulum.py --enable-new-api-stack --num-agents=2


Use the `--help` command line argument to have each script print out its supported command line options.

Most of the scripts share a common subset of generally applicable command line arguments,
for example `--num-env-runners`, `--no-tune`, or `--wandb-key`.


All sub-folders
+++++++++++++++


Algorithms
----------

.. include:: algorithms/README.rst

Catalogs
--------

.. include:: catalogs/README.rst

Checkpoints
-----------

.. include:: checkpoints/README.rst

Connectors
----------

.. include:: connectors/README.rst

Curriculum Learning
-------------------

.. include:: curriculum/README.rst

Debugging
---------

.. include:: debugging/README.rst

Environments
------------

.. include:: envs/README.rst

Evaluation
----------

.. include:: evaluation/README.rst

GPU (for Training and Sampling)
-------------------------------

.. include:: gpus/README.rst

Hierarchical Training
---------------------

.. include:: hierarchical/README.rst

Inference (of Models/Policies)
------------------------------

.. include:: inference/README.rst

Learners
--------

.. include:: learners/README.rst

Multi-Agent RL
--------------

.. include:: multi_agent/README.rst

Offline RL
----------

.. include:: offline/README.rst

Ray Serve and RLlib
-------------------

.. include:: ray_serve/README.rst

Ray Tune and RLlib
------------------

.. include:: ray_tune/README.rst

RLModules
---------

.. include:: rl_modules/README.rst


Tuned Examples
++++++++++++++


- `Tuned examples <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples>`__:
   Collection of tuned hyperparameters sorted by algorithm.




**************************************************************


Environments and Adapters
-------------------------

- |oldstack| `Registering a custom env and model <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/custom_gym_env.py>`__:
   Example of defining and registering a gym env and model for use with RLlib.
- |oldstack| `Local Unity3D multi-agent environment example <https://github.com/ray-project/ray/tree/master/rllib/examples/envs/external_envs/unity3d_env_local.py>`__:
   Example of how to setup an RLlib Algorithm against a locally running Unity3D editor instance to
   learn any Unity3D game (including support for multi-agent).
   Use this example to try things out and watch the game and the learning progress live in the editor.
   Providing a compiled game, this example could also run in distributed fashion with `num_env_runners > 0`.
   For a more heavy-weight, distributed, cloud-based example, see ``Unity3D client/server`` below.

Custom- and Complex Models
--------------------------

- |oldstack| `Custom Keras model <https://github.com/ray-project/ray/blob/master/rllib/examples/_old_api_stack/custom_keras_model.py>`__:
   Example of using a custom Keras model.
- |oldstack| `Registering a custom model with supervised loss <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_model_loss_and_metrics.py>`__:
   Example of defining and registering a custom model with a supervised loss.
- |oldstack| `Batch normalization <https://github.com/ray-project/ray/blob/master/rllib/examples/_old_api_stack/models/batch_norm_model.py>`__:
   Example of adding batch norm layers to a custom model.
- |oldstack| `Custom model API example <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_model_api.py>`__:
   Shows how to define a custom Model API in RLlib, such that it can be used inside certain algorithms.
- |oldstack| `Trajectory View API utilizing model <https://github.com/ray-project/ray/blob/master/rllib/examples/_old_api_stack/models/trajectory_view_utilizing_models.py>`__:
   An example on how a model can use the trajectory view API to specify its own input.
- |oldstack| `MobileNetV2 wrapping example model <https://github.com/ray-project/ray/blob/master/rllib/examples/_old_api_stack/models/mobilenet_v2_with_lstm_models.py>`__:
   Implementations of `tf.keras.applications.mobilenet_v2.MobileNetV2` and `torch.hub (mobilenet_v2)`-wrapping example models.
- |oldstack| `Differentiable Neural Computer <https://github.com/ray-project/ray/blob/master/rllib/examples/_old_api_stack/models/neural_computer.py>`__:
   Example of DeepMind's Differentiable Neural Computer for partially observable environments.


Training Workflows
------------------

- `Custom training workflows <https://github.com/ray-project/ray/blob/master/rllib/examples/ray_tune/custom_train_function.py>`__:
   Example of how to use Tune's support for custom training functions to implement custom training workflows.
- `Custom logger <https://github.com/ray-project/ray/blob/master/rllib/examples/ray_tune/custom_logger.py>`__:
   How to setup a custom Logger object in RLlib.
- `Custom metrics <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_metrics_and_callbacks.py>`__:
   Example of how to output custom training metrics to TensorBoard.
- `Custom tune experiment <https://github.com/ray-project/ray/blob/master/rllib/examples/ray_tune/custom_experiment.py>`__:
   How to run a custom Ray Tune experiment with RLlib with custom training- and evaluation phases.


Evaluation:
-----------
- `Custom evaluation function <https://github.com/ray-project/ray/blob/master/rllib/examples/evaluation/custom_evaluation.py>`__:
   Example of how to write a custom evaluation function that's called instead of the default behavior, which is running with the evaluation worker set through n episodes.
- `Parallel evaluation and training <https://github.com/ray-project/ray/blob/master/rllib/examples/evaluation/evaluation_parallel_to_training.py>`__:
   Example showing how the evaluation workers and the "normal" rollout workers can run (to some extend) in parallel to speed up training.


Serving and Offline
-------------------
- `Offline RL with CQL <https://github.com/ray-project/ray/tree/master/rllib/examples/offline_rl/offline_rl.py>`__:
   Example showing how to run an offline RL training job using a historic-data JSON file.
- `Another example for using RLlib with Ray Serve <https://github.com/ray-project/ray/tree/master/rllib/examples/ray_serve/ray_serve_with_rllib.py>`__
   This script offers a simple workflow for 1) training a policy with RLlib first, 2) creating a new policy 3) restoring its weights from the trained
   one and serving the new policy with Ray Serve.
- `Unity3D client/server <https://github.com/ray-project/ray/tree/master/rllib/examples/envs/external_envs/unity3d_server.py>`__:
   Example of how to setup n distributed Unity3D (compiled) games in the cloud that function as data collecting
   clients against a central RLlib Policy server learning how to play the game.
   The n distributed clients could themselves be servers for external/human players and allow for control
   being fully in the hands of the Unity entities instead of RLlib.
   Note: Uses Unity's MLAgents SDK (>=1.0) and supports all provided MLAgents example games and multi-agent setups.
- `CartPole client/server <https://github.com/ray-project/ray/tree/master/rllib/examples/envs/external_envs/cartpole_server.py>`__:
   Example of online serving of predictions for a simple CartPole policy.
- `Saving experiences <https://github.com/ray-project/ray/blob/master/rllib/examples/offline_rl/saving_experiences.py>`__:
   Example of how to externally generate experience batches in RLlib-compatible format.
- `Finding a checkpoint using custom criteria <https://github.com/ray-project/ray/blob/master/rllib/examples/checkpoints/checkpoint_by_custom_criteria.py>`__:
   Example of how to find a :ref:`checkpoint <rllib-saving-and-loading-algos-and-policies-docs>` after a `Tuner.fit()` with some custom defined criteria.


Multi-Agent and Hierarchical
----------------------------

- `Simple independent multi-agent setup vs a PettingZoo env <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/pettingzoo_independent_learning.py>`__:
   Setup RLlib to run any algorithm in (independent) multi-agent mode against a multi-agent environment.
- `More complex (shared-parameter) multi-agent setup vs a PettingZoo env <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/pettingzoo_parameter_sharing.py>`__:
   Setup RLlib to run any algorithm in (shared-parameter) multi-agent mode against a multi-agent environment.
- `Rock-paper-scissors heuristic vs learned <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/rock_paper_scissors_heuristic_vs_learned.py>`__ and `Rock-paper-scissors learned vs learned <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/rock_paper_scissors_learned_vs_learned.py>`__:
   Two examples of different heuristic and learned policies competing against each other in the rock-paper-scissors environment.
- `Two-step game <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/two_step_game_with_grouped_agents.py>`__:
   Example on how to use agent grouping in a multi-agent environment (the two-step game from the `QMIX paper <https://arxiv.org/pdf/1803.11485.pdf>`__).
- `PettingZoo multi-agent example <https://github.com/Farama-Foundation/PettingZoo/blob/master/tutorials/Ray/rllib_pistonball.py>`__:
   Example on how to use RLlib to learn in `PettingZoo <https://www.pettingzoo.ml>`__ multi-agent environments.
- `PPO with centralized critic on two-step game <https://github.com/ray-project/ray/blob/master/rllib/examples/centralized_critic.py>`__:
   Example of customizing PPO to leverage a centralized value function.
- `Centralized critic in the env <https://github.com/ray-project/ray/blob/master/rllib/examples/centralized_critic_2.py>`__:
   A simpler method of implementing a centralized critic by augmenting agent observations with global information.
- `Hand-coded policy <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/custom_heuristic_policy.py>`__:
   Example of running a custom hand-coded policy alongside trainable policies.
- `Weight sharing between policies <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/multi_agent_cartpole.py>`__:
   Example of how to define weight-sharing layers between two different policies.
- `Multiple algorithms <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/two_algorithms.py>`__:
   Example of alternating training between DQN and PPO.
- `Hierarchical training <https://github.com/ray-project/ray/blob/master/rllib/examples/hierarchical/hierarchical_training.py>`__:
   Example of hierarchical training using the multi-agent API.


Special Action- and Observation Spaces
--------------------------------------

- |newstack| `Autoregressive action distribution example <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/autoregressive_actions.py>`__:
   Learning with an auto-regressive action distribution (for example, two action components, where distribution of the second component depends on the first's actually sampled value).
- |oldstack| `Parametric actions <https://github.com/ray-project/ray/blob/master/rllib/examples/_old_api_stack/parametric_actions_cartpole.py>`__:
   Example of how to handle variable-length or parametric action spaces.
- |oldstack| `Using the "Repeated" space of RLlib for variable lengths observations <https://github.com/ray-project/ray/blob/master/rllib/examples/_old_api_stack/complex_struct_space.py>`__:
   How to use RLlib's `Repeated` space to handle variable length observations.


Community Examples
------------------
- |oldstack| `Arena AI <https://sites.google.com/view/arena-unity/home>`__:
   A General Evaluation Platform and Building Toolkit for Single/Multi-Agent Intelligence
   with RLlib-generated baselines.
- |oldstack| `CARLA <https://github.com/layssi/Carla_Ray_Rlib>`__:
   Example of training autonomous vehicles with RLlib and `CARLA <http://carla.org/>`__ simulator.
- |oldstack| `The Emergence of Adversarial Communication in Multi-Agent Reinforcement Learning <https://arxiv.org/pdf/2008.02616.pdf>`__:
   Using Graph Neural Networks and RLlib to train multiple cooperative and adversarial agents to solve the
   "cover the area"-problem, thereby learning how to best communicate (or - in the adversarial case - how to disturb communication) (`code <https://github.com/proroklab/adversarial_comms>`__).
- |oldstack| `Flatland <https://flatland.aicrowd.com/intro.html>`__:
   A dense traffic simulating environment with RLlib-generated baselines.
- |oldstack| `GFootball <https://github.com/google-research/football/blob/master/gfootball/examples/run_multiagent_rllib.py>`__:
   Example of setting up a multi-agent version of `GFootball <https://github.com/google-research>`__ with RLlib.
- |oldstack| `mobile-env <https://github.com/stefanbschneider/mobile-env>`__:
   An open, minimalist Gymnasium environment for autonomous coordination in wireless mobile networks.
   Includes an example notebook using Ray RLlib for multi-agent RL with mobile-env.
- |oldstack| `Neural MMO <https://github.com/NeuralMMO/environment>`__:
   A multiagent AI research environment inspired by Massively Multiplayer Online (MMO) role playing games –
   self-contained worlds featuring thousands of agents per persistent macrocosm, diverse skilling systems, local and global economies, complex emergent social structures,
   and ad-hoc high-stakes single and team based conflict.
- |oldstack| `NeuroCuts <https://github.com/neurocuts/neurocuts>`__:
   Example of building packet classification trees using RLlib / multi-agent in a bandit-like setting.
- |oldstack| `NeuroVectorizer <https://github.com/ucb-bar/NeuroVectorizer>`__:
   Example of learning optimal LLVM vectorization compiler pragmas for loops in C and C++ codes using RLlib.
- |oldstack| `Roboschool / SageMaker <https://github.com/aws/amazon-sagemaker-examples/tree/0cd3e45f425b529bf06f6155ca71b5e4bc515b9b/reinforcement_learning/rl_roboschool_ray>`__:
   Example of training robotic control policies in SageMaker with RLlib.
- |oldstack| `Sequential Social Dilemma Games <https://github.com/eugenevinitsky/sequential_social_dilemma_games>`__:
   Example of using the multi-agent API to model several `social dilemma games <https://arxiv.org/abs/1702.03037>`__.
- |oldstack| `Simple custom environment for single RL with Ray and RLlib <https://github.com/lcipolina/Ray_tutorials/blob/main/RLLIB_Ray2_0.ipynb>`__:
   Create a custom environment and train a single agent RL using Ray 2.0 with Tune.
- |oldstack| `StarCraft2 <https://github.com/oxwhirl/smac>`__:
   Example of training in StarCraft2 maps with RLlib / multi-agent.
- |oldstack| `Traffic Flow <https://berkeleyflow.readthedocs.io/en/latest/flow_setup.html>`__:
   Example of optimizing mixed-autonomy traffic simulations with RLlib / multi-agent.


Blog Posts
----------

- |oldstack| `Attention Nets and More with RLlib’s Trajectory View API <https://medium.com/distributed-computing-with-ray/attention-nets-and-more-with-rllibs-trajectory-view-api-d326339a6e65>`__:
   Blog describing RLlib's new "trajectory view API" and how it enables implementations of GTrXL (attention net) architectures.
- |oldstack| `Reinforcement Learning with RLlib in the Unity Game Engine <https://medium.com/distributed-computing-with-ray/reinforcement-learning-with-rllib-in-the-unity-game-engine-1a98080a7c0d>`__:
   How-To guide about connecting RLlib with the Unity3D game engine for running visual- and physics-based RL experiments.
- |oldstack| `Lessons from Implementing 12 Deep RL Algorithms in TF and PyTorch <https://medium.com/distributed-computing-with-ray/lessons-from-implementing-12-deep-rl-algorithms-in-tf-and-pytorch-1b412009297d>`__:
   Discussion on how the Ray Team ported 12 of RLlib's algorithms from TensorFlow to PyTorch and the lessons learned.
- |oldstack| `Scaling Multi-Agent Reinforcement Learning <http://bair.berkeley.edu/blog/2018/12/12/rllib>`__:
   Blog post of a brief tutorial on multi-agent RL and its design in RLlib.
- |oldstack| `Functional RL with Keras and TensorFlow Eager <https://medium.com/riselab/functional-rl-with-keras-and-tensorflow-eager-7973f81d6345>`__:
   Exploration of a functional paradigm for implementing reinforcement learning (RL) algorithms.
