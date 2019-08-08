RLlib Examples
==============

This page is an index of examples for the various use cases and features of RLlib.

If any example is broken, or if you'd like to add an example to this page, feel free to raise an issue on our Github repository.

Tuned Examples
--------------

- `Tuned examples <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples>`__:
   Collection of tuned algorithm hyperparameters.
- `Atari benchmarks <https://github.com/ray-project/rl-experiments>`__:
   Collection of reasonably optimized Atari results.

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

- `Registering a custom env and model <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_env.py>`__:
   Example of defining and registering a gym env and model for use with RLlib.
- `Custom Keras model <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_keras_model.py>`__:
   Example of using a custom Keras model.
- `Custom Keras RNN model <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_keras_rnn_model.py>`__:
   Example of using a custom Keras RNN model.
- `Registering a custom model with supervised loss <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_loss.py>`__:
   Example of defining and registering a custom model with a supervised loss.
- `Subprocess environment <https://github.com/ray-project/ray/blob/master/rllib/tests/test_env_with_subprocess.py>`__:
   Example of how to ensure subprocesses spawned by envs are killed when RLlib exits.
- `Batch normalization <https://github.com/ray-project/ray/blob/master/rllib/examples/batch_norm_model.py>`__:
   Example of adding batch norm layers to a custom model.
- `Parametric actions <https://github.com/ray-project/ray/blob/master/rllib/examples/parametric_action_cartpole.py>`__:
   Example of how to handle variable-length or parametric action spaces.
- `Eager execution <https://github.com/ray-project/ray/blob/master/rllib/examples/eager_execution.py>`__:
   Example of how to leverage TensorFlow eager to simplify debugging and design of custom models and policies.

Serving and Offline
-------------------
- `CartPole server <https://github.com/ray-project/ray/tree/master/rllib/examples/serving>`__:
   Example of online serving of predictions for a simple CartPole policy.
- `Saving experiences <https://github.com/ray-project/ray/blob/master/rllib/examples/saving_experiences.py>`__:
   Example of how to externally generate experience batches in RLlib-compatible format.

Multi-Agent and Hierarchical
----------------------------

- `Rock-paper-scissors <https://github.com/ray-project/ray/blob/master/rllib/examples/rock_paper_scissors_multiagent.py>`__:
   Example of different heuristic and learned policies competing against each other in rock-paper-scissors.
- `Two-step game <https://github.com/ray-project/ray/blob/master/rllib/examples/twostep_game.py>`__:
   Example of the two-step game from the `QMIX paper <https://arxiv.org/pdf/1803.11485.pdf>`__.
- `Hand-coded policy <https://github.com/ray-project/ray/blob/master/rllib/examples/multiagent_custom_policy.py>`__:
   Example of running a custom hand-coded policy alongside trainable policies.
- `Weight sharing between policies <https://github.com/ray-project/ray/blob/master/rllib/examples/multiagent_cartpole.py>`__:
   Example of how to define weight-sharing layers between two different policies.
- `Multiple trainers <https://github.com/ray-project/ray/blob/master/rllib/examples/multiagent_two_trainers.py>`__:
   Example of alternating training between two DQN and PPO trainers.
- `Hierarchical training <https://github.com/ray-project/ray/blob/master/rllib/examples/hierarchical_training.py>`__:
   Example of hierarchical training using the multi-agent API.
- `PPO with centralized value function <https://github.com/ray-project/ray/pull/3642/files>`__:
   Example of customizing PPO to include a centralized value function, including a runnable script that demonstrates cooperative CartPole.

Community Examples
------------------
- `CARLA <https://github.com/layssi/Carla_Ray_Rlib>`__:
   Example of training autonomous vehicles with RLlib and `CARLA <http://carla.org/>`__ simulator.
- `GFootball <https://github.com/google-research/football/blob/master/gfootball/examples/run_multiagent_rllib.py>`__:
   Example of setting up a multi-agent version of `GFootball <https://github.com/google-research>`__ with RLlib.
- `NeuroCuts <https://github.com/neurocuts/neurocuts>`__:
   Example of building packet classification trees using RLlib / multi-agent in a bandit-like setting.
- `Roboschool / SageMaker <https://github.com/awslabs/amazon-sagemaker-examples/tree/master/reinforcement_learning/rl_roboschool_ray>`__:
   Example of training robotic control policies in SageMaker with RLlib.
- `StarCraft2 <https://github.com/oxwhirl/smac>`__:
   Example of training in StarCraft2 maps with RLlib / multi-agent.
- `Traffic Flow <https://berkeleyflow.readthedocs.io/en/latest/flow_setup.html>`__:
   Example of optimizing mixed-autonomy traffic simulations with RLlib / multi-agent.
- `Sequential Social Dilemma Games <https://github.com/eugenevinitsky/sequential_social_dilemma_games>`__:
   Example of using the multi-agent API to model several `social dilemma games <https://arxiv.org/abs/1702.03037>`__.
