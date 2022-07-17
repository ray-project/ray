.. include:: /_includes/rllib/announcement.rst

.. include:: /_includes/rllib/we_are_hiring.rst

Examples
========

This page is an index of examples for the various use cases and features of RLlib.

If any example is broken, or if you'd like to add an example to this page,
feel free to raise an issue on our Github repository.

Tuned Examples
--------------

- `Tuned examples <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples>`__:
   Collection of tuned hyperparameters by algorithm.
- `MuJoCo and Atari benchmarks <https://github.com/ray-project/rl-experiments>`__:
   Collection of reasonably optimized Atari and MuJoCo results.

Blog Posts
----------

- `Attention Nets and More with RLlib’s Trajectory View API <https://medium.com/distributed-computing-with-ray/attention-nets-and-more-with-rllibs-trajectory-view-api-d326339a6e65>`__:
   This blog describes RLlib's new "trajectory view API" and how it enables implementations of GTrXL (attention net) architectures.
- `Reinforcement Learning with RLlib in the Unity Game Engine <https://medium.com/distributed-computing-with-ray/reinforcement-learning-with-rllib-in-the-unity-game-engine-1a98080a7c0d>`__:
   A how-to on connectig RLlib with the Unity3D game engine for running visual- and physics-based RL experiments.
- `Lessons from Implementing 12 Deep RL Algorithms in TF and PyTorch <https://medium.com/distributed-computing-with-ray/lessons-from-implementing-12-deep-rl-algorithms-in-tf-and-pytorch-1b412009297d>`__:
   Discussion on how we ported 12 of RLlib's algorithms from TensorFlow to PyTorch and what we learnt on the way.
- `Scaling Multi-Agent Reinforcement Learning <http://bair.berkeley.edu/blog/2018/12/12/rllib>`__:
   This blog post is a brief tutorial on multi-agent RL and its design in RLlib.
- `Functional RL with Keras and TensorFlow Eager <https://medium.com/riselab/functional-rl-with-keras-and-tensorflow-eager-7973f81d6345>`__:
   Exploration of a functional paradigm for implementing reinforcement learning (RL) algorithms.

Environments and Adapters
-------------------------

- `Registering a custom env and model <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_env.py>`__:
   Example of defining and registering a gym env and model for use with RLlib.
- `Local Unity3D multi-agent environment example <https://github.com/ray-project/ray/tree/master/rllib/examples/unity3d_env_local.py>`__:
   Example of how to setup an RLlib Algorithm against a locally running Unity3D editor instance to
   learn any Unity3D game (including support for multi-agent).
   Use this example to try things out and watch the game and the learning progress live in the editor.
   Providing a compiled game, this example could also run in distributed fashion with `num_workers > 0`.
   For a more heavy-weight, distributed, cloud-based example, see ``Unity3D client/server`` below.
- `Rendering and recording of an environment <https://github.com/ray-project/ray/blob/master/rllib/examples/env_rendering_and_recording.py>`__:
   Example showing how to switch on rendering and recording of an env.
- `Coin Game Example <https://github.com/ray-project/ray/blob/master/rllib/examples/coin_game_env.py>`__:
   Coin Game Env Example (provided by the "Center on Long Term Risk").
- `DMLab Watermaze example <https://github.com/ray-project/ray/blob/master/rllib/examples/dmlab_watermaze.py>`__:
   Example for how to use a DMLab environment (Watermaze).
- `RecSym environment example (for recommender systems) using the SlateQ algorithm <https://github.com/ray-project/ray/blob/master/rllib/examples/recommender_system_with_recsim_and_slateq.py>`__:
   Script showing how to train SlateQ on a RecSym environment.
- `SUMO (Simulation of Urban MObility) environment example <https://github.com/ray-project/ray/blob/master/rllib/examples/sumo_env_local.py>`__:
   Example demonstrating how to use the SUMO simulator in connection with RLlib.
- `VizDoom example script using RLlib's auto-attention wrapper <https://github.com/ray-project/ray/blob/master/rllib/examples/vizdoom_with_attention_net.py>`__:
   Script showing how to run PPO with an attention net against a VizDoom gym environment.
- `Subprocess environment <https://github.com/ray-project/ray/blob/master/rllib/env/tests/test_env_with_subprocess.py>`__:
   Example of how to ensure subprocesses spawned by envs are killed when RLlib exits.


Custom- and Complex Models
--------------------------

- `Attention Net (GTrXL) learning the "repeat-after-me" environment <https://github.com/ray-project/ray/blob/master/rllib/examples/attention_net.py>`__:
   Example showing how to use the auto-attention wrapper for your default- and custom models in RLlib.
- `LSTM model learning the "repeat-after-me" environment <https://github.com/ray-project/ray/blob/master/rllib/examples/lstm_auto_wrapping.py>`__:
   Example showing how to use the auto-LSTM wrapper for your default- and custom models in RLlib.
- `Custom Keras model <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_keras_model.py>`__:
   Example of using a custom Keras model.
- `Custom Keras/PyTorch RNN model <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_rnn_model.py>`__:
   Example of using a custom Keras- or PyTorch RNN model.
- `Registering a custom model with supervised loss <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_loss.py>`__:
   Example of defining and registering a custom model with a supervised loss.
- `Batch normalization <https://github.com/ray-project/ray/blob/master/rllib/examples/batch_norm_model.py>`__:
   Example of adding batch norm layers to a custom model.
- `Eager execution <https://github.com/ray-project/ray/blob/master/rllib/examples/eager_execution.py>`__:
   Example of how to leverage TensorFlow eager to simplify debugging and design of custom models and policies.
- `Custom "Fast" Model <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_fast_model.py>`__:
   Example of a "fast" Model learning only one parameter for tf and torch.
- `Custom model API example <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_model_api.py>`__:
   Shows how to define a custom Model API in RLlib, such that it can be used inside certain algorithms.
- `Trajectory View API utilizing model <https://github.com/ray-project/ray/blob/master/rllib/examples/trajectory_view_api.py>`__:
   An example on how a model can use the trajectory view API to specify its own input.
- `MobileNetV2 wrapping example model <https://github.com/ray-project/ray/blob/master/rllib/examples/mobilenet_v2_with_lstm.py>`__:
   Implementations of `tf.keras.applications.mobilenet_v2.MobileNetV2` and `torch.hub (mobilenet_v2)`-wrapping example models.
- `Differentiable Neural Computer <https://github.com/ray-project/ray/blob/master/rllib/examples/models/neural_computer.py>`__:
   Example of DeepMind's Differentiable Neural Computer for partially-observable environments.


Training Workflows
------------------

- `Custom training workflows <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_train_fn.py>`__:
   Example of how to use Tune's support for custom training functions to implement custom training workflows.
- `Curriculum learning with the TaskSettableEnv API <https://github.com/ray-project/ray/blob/master/rllib/examples/curriculum_learning.py>`__:
   Example of how to advance the environment through different phases (tasks) over time.
   Also see the `curriculum learning how-to <rllib-training.html#example-curriculum-learning>`__ from the documentation here.
- `Custom logger <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_logger.py>`__:
   How to setup a custom Logger object in RLlib.
- `Custom metrics <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_metrics_and_callbacks.py>`__:
   Example of how to output custom training metrics to TensorBoard.
- `Custom Policy class (TensorFlow) <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_tf_policy.py>`__:
   How to setup a custom TFPolicy.
- `Custom Policy class (PyTorch) <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_torch_policy.py>`__:
   How to setup a custom TorchPolicy.
- `Using rollout workers directly for control over the whole training workflow <https://github.com/ray-project/ray/blob/master/rllib/examples/rollout_worker_custom_workflow.py>`__:
   Example of how to use RLlib's lower-level building blocks to implement a fully customized training workflow.
- `Custom execution plan function handling two different Policies (DQN and PPO) at the same time <https://github.com/ray-project/ray/blob/master/rllib/examples/two_trainer_workflow.py>`__:
   Example of how to use the exec. plan of an Algorithm to trin two different policies in parallel (also using multi-agent API).
- `Custom tune experiment <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_experiment.py>`__:
   How to run a custom Ray Tune experiment with RLlib with custom training- and evaluation phases.


Evaluation:
-----------
- `Custom evaluation function <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_eval.py>`__:
   Example of how to write a custom evaluation function that is called instead of the default behavior, which is running with the evaluation worker set through n episodes.
- `Parallel evaluation and training <https://github.com/ray-project/ray/blob/master/rllib/examples/parallel_evaluation_and_training.py>`__:
   Example showing how the evaluation workers and the "normal" rollout workers can run (to some extend) in parallel to speed up training.


Serving and Offline
-------------------
- `Offline RL with CQL <https://github.com/ray-project/ray/tree/master/rllib/examples/offline_rl.py>`__:
   Example showing how to run an offline RL training job using a historic-data json file.
- :ref:`Serving RLlib models with Ray Serve <serve-rllib-tutorial>`: Example of using Ray Serve to serve RLlib models
   with HTTP and JSON interface. **This is the recommended way to expose RLlib for online serving use case**.
- `Another example for using RLlib with Ray Serve <https://github.com/ray-project/ray/tree/master/rllib/examples/inference_and_serving/serve_and_rllib.py>`__
   This script offers a simple workflow for 1) training a policy with RLlib first, 2) creating a new policy 3) restoring its weights from the trained
   one and serving the new policy via Ray Serve.
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
- `Finding a checkpoint using custom criteria <https://github.com/ray-project/ray/blob/master/rllib/examples/checkpoint_by_custom_criteria.py>`__:
   Example of how to find a checkpoint after a `tune.run` via some custom defined criteria.


Multi-Agent and Hierarchical
----------------------------

- `Simple independent multi-agent setup vs a PettingZoo env <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent_independent_learning.py>`__:
   Setup RLlib to run any algorithm in (independent) multi-agent mode against a multi-agent environment.
- `More complex (shared-parameter) multi-agent setup vs a PettingZoo env <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent_parameter_sharing.py>`__:
   Setup RLlib to run any algorithm in (shared-parameter) multi-agent mode against a multi-agent environment.
- `Rock-paper-scissors <https://github.com/ray-project/ray/blob/master/rllib/examples/rock_paper_scissors_multiagent.py>`__:
   Example of different heuristic and learned policies competing against each other in rock-paper-scissors.
- `Two-step game <https://github.com/ray-project/ray/blob/master/rllib/examples/two_step_game.py>`__:
   Example of the two-step game from the `QMIX paper <https://arxiv.org/pdf/1803.11485.pdf>`__.
- `PettingZoo multi-agent example <https://github.com/Farama-Foundation/PettingZoo/blob/master/tutorials/rllib_pistonball.py>`__:
   Example on how to use RLlib to learn in `PettingZoo <https://www.pettingzoo.ml>`__ multi-agent environments.
- `PPO with centralized critic on two-step game <https://github.com/ray-project/ray/blob/master/rllib/examples/centralized_critic.py>`__:
   Example of customizing PPO to leverage a centralized value function.
- `Centralized critic in the env <https://github.com/ray-project/ray/blob/master/rllib/examples/centralized_critic_2.py>`__:
   A simpler method of implementing a centralized critic by augmentating agent observations with global information.
- `Hand-coded policy <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent_custom_policy.py>`__:
   Example of running a custom hand-coded policy alongside trainable policies.
- `Weight sharing between policies <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent_cartpole.py>`__:
   Example of how to define weight-sharing layers between two different policies.
- `Multiple algorithms <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent_two_trainers.py>`__:
   Example of alternating training between DQN and PPO.
- `Hierarchical training <https://github.com/ray-project/ray/blob/master/rllib/examples/hierarchical_training.py>`__:
   Example of hierarchical training using the multi-agent API.
- `Iterated Prisoner's Dilemma environment example <https://github.com/ray-project/ray/blob/master/rllib/examples/iterated_prisoners_dilemma_env.py>`__:
   Example of an iterated prisoner's dilemma environment solved by RLlib.


GPU examples
------------
- `Example showing how to setup fractional GPUs <https://github.com/ray-project/ray/blob/master/rllib/examples/partial_gpus.py>`__:
   Example of how to setup fractional GPUs for learning (driver) and environment rollouts (remote workers).


Special Action- and Observation Spaces
--------------------------------------

- `Nested action spaces <https://github.com/ray-project/ray/blob/master/rllib/examples/nested_action_spaces.py>`__:
   Learning in arbitrarily nested action spaces.
- `Parametric actions <https://github.com/ray-project/ray/blob/master/rllib/examples/parametric_actions_cartpole.py>`__:
   Example of how to handle variable-length or parametric action spaces (see also `this example here <https://github.com/ray-project/ray/blob/master/rllib/examples/random_parametric_agent.py>`__).
- `Custom observation filters <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_observation_filters.py>`__:
   How to filter raw observations coming from the environment for further processing by the Agent's model(s).
- `Using the "Repeated" space of RLlib for variable lengths observations <https://github.com/ray-project/ray/blob/master/rllib/examples/complex_struct_space.py>`__:
   How to use RLlib's `Repeated` space to handle variable length observations.
- `Autoregressive action distribution example <https://github.com/ray-project/ray/blob/master/rllib/examples/autoregressive_action_dist.py>`__:
   Learning with auto-regressive action dependencies (e.g. 2 action components; distribution for 2nd component depends on the 1st component's actually sampled value).


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
- `Neural MMO <https://github.com/NeuralMMO/environment>`__:
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

.. include:: /_includes/rllib/announcement_bottom.rst
