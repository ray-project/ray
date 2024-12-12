.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst


Examples
========

This page contains an index of all the python scripts in the
`examples folder <https://github.com/ray-project/ray/blob/master/rllib/examples>`__
of RLlib, demonstrating the different use cases and features of the library.

.. note::

    RLlib is currently in a transition state from old- to new API stack.
    Some of the example scripts haven't been translated yet to the new stack and are tagged
    with the following comment line on top: ``# @OldAPIStack``. The moving of all example
    scripts over to the new stack is work in progress.

.. note::

    If any (new API stack) example is broken, or if you'd like to add an example to this page,
    feel free to raise an issue on `RLlib's github repository <https://github.com/ray-project/ray/issues/new/choose>`__.


Folder Structure
----------------
The `examples folder <https://github.com/ray-project/ray/blob/master/rllib/examples>`__ is
structured into several sub-directories, the contents of all of which are described in detail below.


How to run an example script
----------------------------

Most of the example scripts are self-executable, meaning you can ``cd`` into the respective
directory and run the script as-is with python:

.. code-block:: bash

    $ cd ray/rllib/examples/multi_agent
    $ python multi_agent_pendulum.py --enable-new-api-stack --num-agents=2


Use the `--help` command line argument to have each script print out its supported command line options.

Most of the scripts share a common subset of generally applicable command line arguments,
for example `--num-env-runners` (to scale the number of EnvRunner actors), `--no-tune` (to switch off running with Ray Tune),
`--wandb-key` (to log to W&B), or `--verbose` (to control log chattiness).


All example sub-folders
-----------------------


Actions
+++++++
- `Nested Action Spaces <https://github.com/ray-project/ray/blob/master/rllib/examples/actions/nested_action_spaces.py>`__:
   Sets up an environment with nested action spaces using custom (single- or multi-agent) configurations. This example demonstrates
   how RLlib manages complex action structures, such as multi-dimensional or hierarchical action spaces.


Checkpoints
+++++++++++

- `Checkpoint by Custom Criteria <https://github.com/ray-project/ray/blob/master/rllib/examples/checkpoints/checkpoint_by_custom_criteria.py>`__:
   Shows how to create checkpoints based on custom criteria, giving users control over when to save model snapshots during training.

- `Continue Training From Checkpoint <https://github.com/ray-project/ray/blob/master/rllib/examples/checkpoints/continue_training_from_checkpoint.py>`__:
   Illustrates resuming training from a saved checkpoint, useful for extending training sessions or recovering from interruptions.

- `Restore 1 (out of N) Agents from Checkpoint <https://github.com/ray-project/ray/blob/master/rllib/examples/checkpoints/restore_1_of_n_agents_from_checkpoint.py>`__:
   Restores one specific agent from a multi-agent checkpoint, allowing selective loading for environments where only certain agents need
   to resume training.


Connectors
++++++++++

.. note::
    RLlib's Connector API has been re-written from scratch for the new API stack.
    Connector-pieces and -pipelines are now referred to as :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2`
    (as opposed to ``Connector``, which only continue to work on the old API stack).


- `Flatten and One-Hot Observations <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/flatten_observations_dict_space.py>`__:
   Demonstrates how to one-hot discrete observation spaces and/or flatten complex observations (Dict or Tuple), allowing RLlib to process arbitrary
   observation data as flattened (1D) vectors. Useful for environments with complex, discrete, or hierarchical observations.

- `Observation Frame-Stacking <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/frame_stacking.py>`__:
   Implements frame stacking, where consecutive frames are stacked together to provide temporal context to the agent.
   This technique is common in environments with continuous state changes, like video frames in Atari games.
   Using connectors for frame stacking is more efficient as it avoids having to send large observation tensors through the network (ray).

- `Mean/Std Filtering <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/mean_std_filtering.py>`__:
   Adds mean and standard deviation normalization for observations (shift by the mean and divide by std-dev), improving learning stability
   by scaling observations to a normalized range. This can enhance performance in environments with highly variable state magnitudes.

- `Prev-Actions, Prev-Rewards Connector <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/prev_actions_prev_rewards.py>`__:
   Augments observations with previous actions and rewards, giving the agent a short-term memory of past events, which can improve
   decision-making in partially observable or sequentially dependent tasks.


Curiosity
+++++++++

- `Count-Based Curiosity <https://github.com/ray-project/ray/blob/master/rllib/examples/curiosity/count_based_curiosity.py>`__:
   Implements count-based intrinsic motivation to encourage exploration of less visited states.
   Using curiosity is beneficial in sparse-reward environments where agents may struggle to find rewarding paths.
   However, count-based methods are only feasible for environments with small observation spaces.

- `Euclidian Distance-Based Curiosity <https://github.com/ray-project/ray/blob/master/rllib/examples/curiosity/euclidian_distance_based_curiosity.py>`__:
   Uses Euclidean distance between states and the initial state to measure novelty, encouraging exploration by rewarding the agent for reaching "far away"
   regions of the environment.
   Suitable for sparse-reward tasks, where diverse exploration is key to success.

- `Intrinsic-Curiosity-Model (ICM) Based Curiosity <https://github.com/ray-project/ray/blob/master/rllib/examples/curiosity/intrinsic_curiosity_model_based_curiosity.py>`__:
   Adds an `Intrinsic Curiosity Model (ICM) <https://arxiv.org/pdf/1705.05363.pdf>`__ that learns to predict the next state as well as the action in
   between two states to measure novelty. The higher the loss of the ICM, the higher the "novelty" and thus the intrinsic reward.
   Ideal for complex environments with large observation spaces where reward signals are sparse.


Curriculum Learning
+++++++++++++++++++

- `Custom Env Rendering Method <https://github.com/ray-project/ray/blob/master/rllib/examples/curriculum/curriculum_learning.py>`__:
   Demonstrates curriculum learning, where the environment difficulty increases as the agent improves.
   This approach enables gradual learning, allowing agents to master simpler tasks before progressing to more challenging ones,
   ideal for environments with hierarchical or staged difficulties. Also see the :doc:`curriculum learning how-to </rllib/rllib-advanced-api>` from the documentation.


Environments
++++++++++++

- `Custom Env Rendering Method <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/custom_env_render_method.py>`__:
   Demonstrates how to add a custom `render()` method to a (custom) environment, allowing visualizations of agent interactions.

- `Custom gymnasium Env <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/custom_gym_env.py>`__:
   Implements a custom `gymnasium <https://gymnasium.farama.org>`__ environment from scratch, showing how to define observation and action spaces,
   arbitrary reward functions, as well as, step- and reset logic.

- `Env connecting to RLlib through a TCP client <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/env_connecting_to_rllib_w_tcp_client.py>`__:
   An external environment, running outside of RLlib and acting as a client, connects to RLlib as a server. The external env performs its own
   action inference using an ONNX model, sends collected data back to RLlib for training, and receives model updates from time to time from RLlib.

- `Env Rendering and Recording <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/env_rendering_and_recording.py>`__:
   Illustrates environment rendering and recording setups within RLlib, capturing visual outputs for later review (ex. on WandB), which is essential
   for tracking agent behavior in training.

- `Env with Protobuf Observations <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/env_w_protobuf_observations.py>`__:
   Uses Protobuf for observations, demonstrating an advanced way of handling serialized data in environments. This approach is useful for
   integrating complex external data sources as observations.


Evaluation
++++++++++

- `Custom Evaluation <https://github.com/ray-project/ray/blob/master/rllib/examples/evaluation/custom_evaluation.py>`__:
   Configures custom evaluation metrics for agent performance, allowing users to define specific success criteria beyond standard RLlib evaluation metrics.

- `Evaluation Parallel to Training <https://github.com/ray-project/ray/blob/master/rllib/examples/evaluation/evaluation_parallel_to_training.py>`__:
   Runs evaluation episodes in parallel with training, reducing training time by offloading evaluation to separate processes.
   This is beneficial in scenarios where frequent evaluation is required without interrupting learning.


Fault Tolerance
+++++++++++++++

- `Crashing and Stalling Env <https://github.com/ray-project/ray/blob/master/rllib/examples/fault_tolerance/crashing_and_stalling_env.py>`__:
   Simulates an environment that randomly crashes and/or stalls, allowing users to test RLlib's fault-tolerance mechanisms.
   This script is useful for evaluating how RLlib handles interruptions and recovers from unexpected failures during training.


GPU (for Training and Sampling)
+++++++++++++++++++++++++++++++

- `Float16 Training and Inference <https://github.com/ray-project/ray/blob/master/rllib/examples/gpus/float16_training_and_inference.py>`__:
   Configures a setup for mixed-precision (float16) training and inference, optimizing performance by reducing memory usage and speeding up computation.
   This is especially useful for large-scale models on compatible GPUs.

- `Fractional GPUs per Learner <https://github.com/ray-project/ray/blob/master/rllib/examples/gpus/fractional_gpus_per_learner.py>`__:
   Demonstrates allocating fractional GPUs to individual learners, enabling finer resource allocation in multi-model setups.
   Useful for saving resources when training smaller models, many of which can fit on a single GPU.

- `Mixed Precision Training and Float16 Inference <https://github.com/ray-project/ray/blob/master/rllib/examples/gpus/mixed_precision_training_float16_inference.py>`__:
   Uses mixed precision (float32 and float16) for training, while switching to float16 precision for inference, balancing stability during training
   with performance improvements during evaluation.


Hierarchical Training
+++++++++++++++++++++

- `Hierarchical RL Training <https://github.com/ray-project/ray/blob/master/rllib/examples/hierarchical/hierarchical_training.py>`__:
   Showcases a hierarchical RL setup inspired by automatic subgoal discovery and subpolicy specialization. A high-level policy selects subgoals and assigns one of three
   specialized low-level policies to achieve them within a time limit, encouraging specialization and efficient task-solving.
   The agent has to navigate a complex grid-world environment. The example highlights the advantages of hierarchical
   learning over flat approaches by demonstrating significantly improved learning performance in challenging, goal-oriented tasks.


Inference (of Models/Policies)
++++++++++++++++++++++++++++++

- `Policy Inference after Training <https://github.com/ray-project/ray/blob/master/rllib/examples/inference/policy_inference_after_training.py>`__:
   Demonstrates performing inference with a trained policy, showing how to load a trained model and use it to make decisions in a simulated environment.

- `Policy Inference after Training (with ConnectorV2) <https://github.com/ray-project/ray/blob/master/rllib/examples/inference/policy_inference_after_training_w_connector.py>`__:
   Runs inference with a trained (LSTM-based) policy using connectors, which preprocess observations and actions, allowing for more modular and flexible inference setups.


Learners
++++++++

- `Custom Loss Function (simple) <https://github.com/ray-project/ray/blob/master/rllib/examples/learners/custom_loss_fn_simple.py>`__:
   Implements a custom loss function for training, demonstrating how users can define tailored loss objectives for specific environments or
   behaviors.

- `Custom Torch Learning Rate Schedulers <https://github.com/ray-project/ray/blob/master/rllib/examples/learners/ppo_with_torch_lr_schedulers.py>`__:
   Adds learning rate scheduling to PPO, showing how to adjust the learning rate dynamically using PyTorch schedulers for improved
   training stability.

- `Separate Learning Rate and Optimizer for Value-Function <https://github.com/ray-project/ray/blob/master/rllib/examples/learners/separate_vf_lr_and_optimizer.py>`__:
   Configures a separate learning rate and a separate optimizer for the value function (vs the policy network), enabling differentiated
   training dynamics between policy and value estimation in RL algorithms.


Metrics
+++++++

- `Logging Custom Metrics in EnvRunners <https://github.com/ray-project/ray/blob/master/rllib/examples/metrics/custom_metrics_in_env_runners.py>`__:
   Demonstrates adding custom metrics to :py:class:`~ray.rllib.env.env_runner.EnvRunner` actors, providing a way to track specific
   performance- and environment indicators beyond the standard RLlib metrics.


Multi-Agent RL
++++++++++++++

- `Custom Heuristic Policy <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/custom_heuristic_policy.py>`__:
   Demonstrates running a hybrid policy setup within the `MultiAgentCartPole` environment, where one agent follows
   a hand-coded random policy while another agent trains with PPO. This example highlights integrating static and dynamic policies,
   suitable for environments with a mix of fixed-strategy and adaptive agents.

- `Different Spaces for Agents <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/different_spaces_for_agents.py>`__:
   Configures agents with differing observation and action spaces within the same environment, showcasing RLlib's support for heterogeneous agents with varying space requirements in a single multi-agent environment.

- `Grouped Agents (Two-Step Game) <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/two_step_game_with_grouped_agents.py>`__:
   Implements a multi-agent, grouped setup within a two-step game environment (from the `QMIX paper <https://arxiv.org/pdf/1803.11485.pdf>`__).
   N agents are grouped into M teams (N >= M) for which policies and rewards are shared. This example demonstrates RLlib's ability to manage
   collective objectives and interactions among grouped agents.

- `Multi-Agent CartPole <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/multi_agent_cartpole.py>`__:
   Runs a multi-agent version of the CartPole environment with each agent independently learning to balance its pole.
   This example serves as a foundational test for multi-agent reinforcement learning scenarios in simple, independent tasks.

- `Multi-Agent Pendulum <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/multi_agent_pendulum.py>`__:
   Extends the classic Pendulum environment into a multi-agent setting, where multiple agents attempt to balance
   their respective pendulums.
   This example highlights RLlib's support for environments with replicated dynamics but distinct agent policies.

- `PettingZoo Independent Learning <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/pettingzoo_independent_learning.py>`__:
   Integrates RLlib with `PettingZoo <https://pettingzoo.farama.org/>`__ to facilitate independent learning among multiple agents.
   Each agent independently optimizes its policy within a shared environment.

- `PettingZoo Parameter Sharing <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/pettingzoo_parameter_sharing.py>`__:
   Uses `PettingZoo <https://pettingzoo.farama.org/>`__ for an environment where all agents share a single policy.

- `PettingZoo Shared Value Function <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/pettingzoo_shared_value_function.py>`__:
   Also using PettingZoo, this example explores shared value functions among agents.
   It demonstrates collaborative learning scenarios where agents collectively estimate a value function rather than individual policies.

- `Rock-Paper-Scissors Heuristic vs Learned <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/rock_paper_scissors_heuristic_vs_learned.py>`__:
   Simulates a rock-paper-scissors game with one heuristic-driven agent and one learning agent.
   It provides insights into performance when combining fixed and adaptive strategies in adversarial games.

- `Rock-Paper-Scissors Learned vs Learned <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/rock_paper_scissors_learned_vs_learned.py>`__:
   Sets up a rock-paper-scissors game where both agents are trained and therefore learn strategies against each other.
   Useful for evaluating performance in simple adversarial settings.

- `Self-Play (League-Based) with OpenSpiel <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/self_play_league_based_with_open_spiel.py>`__:
   Uses OpenSpiel to demonstrate league-based self-play, where agents play against various (frozen or still-learning) versions of themselves to
   improve through competitive interaction.

- `Self-Play with OpenSpiel <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/self_play_with_open_spiel.py>`__:
   Similar to the league-based self-play, but simpler. This script leverages OpenSpiel for two-player games, allowing agents to improve
   through direct self-play without building a complex, structured league.


Offline RL
++++++++++

- `Train with Behavioral Cloning (BC), Finetune with PPO <https://github.com/ray-project/ray/blob/master/rllib/examples/offline_rl/train_w_bc_finetune_w_ppo.py>`__:
   Combines behavioral cloning pre-training with PPO fine-tuning, providing a two-phase training strategy where imitation learning (offline)
   is followed by online reinforcement learning.


Ray Serve and RLlib
+++++++++++++++++++

- `Custom Experiment <https://github.com/ray-project/ray/blob/master/rllib/examples/ray_serve/ray_serve_with_rllib.py>`__:
   Integrates RLlib with `Ray Serve <https://docs.ray.io/en/latest/serve/index.html>`__, showcasing how to deploy trained
   :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` instances as RESTful services. This setup is ideal for deploying models
   in production environments with API-based interactions.


Ray Tune and RLlib
++++++++++++++++++

- `Custom Experiment <https://github.com/ray-project/ray/blob/master/rllib/examples/ray_tune/custom_experiment.py>`__:
   Configures a custom experiment with `Ray Tune <https://docs.ray.io/en/latest/tune/index.html>`__, demonstrating advanced options
   for custom training- and evaluation phases

- `Custom Logger <https://github.com/ray-project/ray/blob/master/rllib/examples/ray_tune/custom_logger.py>`__:
   Shows how to implement a custom logger within `Ray Tune <https://docs.ray.io/en/latest/tune/index.html>`__,
   allowing users to define specific logging behaviors and outputs during training.

- `Custom Progress Reporter <https://github.com/ray-project/ray/blob/master/rllib/examples/ray_tune/custom_progress_reporter.py>`__:
   Demonstrates a custom progress reporter in `Ray Tune <https://docs.ray.io/en/latest/tune/index.html>`__, which enables
   tracking and displaying specific training metrics or status updates in a customized format.


RLModules
+++++++++

- `Action Masking <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/action_masking_rl_module.py>`__:
   Implements an :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` with action masking, where certain (disallowed) actions are
   masked based on parts of the observation dict, useful for environments with conditional action availability.

- `Auto-Regressive Actions <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/autoregressive_actions_rl_module.py>`__:
   Configures an RL module that generates actions in an autoregressive manner, where the second component of an action depends on
   the previously sampled first component of the same action.

- `Custom CNN-Based RLModule <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/custom_cnn_rl_module.py>`__:
   Demonstrates a custom CNN architecture realized as an :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`, enabling convolutional
   feature extraction tailored to the environment's visual observations.

- `Custom LSTM-Based RLModule <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/custom_lstm_rl_module.py>`__:
   Uses a custom LSTM within an :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`, allowing for temporal sequence processing,
   beneficial for partially observable environments with sequential dependencies.

- `Migrate ModelV2 to RLModule (new API stack) by config <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/migrate_modelv2_to_new_api_stack_by_config.py>`__:
   Shows how to migrate a ModelV2-based setup (old API stack) to the new API stack's :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`,
   using an (old API stack) :py:class:`~ray.rllib.algorithm.algorithm_config.AlgorithmConfig` instance.

- `Migrate ModelV2 to RLModule (new API stack) by Policy Checkpoint <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/migrate_modelv2_to_new_api_stack_by_policy_checkpoint.py>`__:
   Migrates a ModelV2 (old API stack) to the new API stack's :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` by directly loading a
   policy checkpoint, enabling smooth transitions to the new API stack while preserving learned parameters.

- `Pretrain Single-Agent Policy, then Train in Multi-Agent Env <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/pretraining_single_agent_training_multi_agent.py>`__:
   Demonstrates pretraining a single-agent model and transferring it to a multi-agent setting, useful for initializing
   multi-agent scenarios with pre-trained policies.


Tuned Examples
--------------

The `tuned examples <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples>`__ folder
contains python config files that can be executed analogously to
all other example scripts described here to run tuned learning experiments
for the different algorithms and environment types.

For example, see this tuned Atari example for PPO, which learns to solve the Pong environment
in roughly 5 minutes. It can be run as follows on a single g5.24xlarge (or g6.24xlarge) machine with
4 GPUs and 96 CPUs:

.. code-block:: bash

    $ cd ray/rllib/tuned_examples/ppo
    $ python atari_ppo.py --env=ale_py:ALE/Pong-v5 --num-learners=4 --num-env-runners=95

Note that some of the files in this folder are used for RLlib's daily or weekly release tests as well.


Community Examples
------------------

.. note::

    The community examples listed here all refer to the old API stack of RLlib.


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
----------

.. note::

    The blog posts listed here all refer to the old API stack of RLlib.


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
