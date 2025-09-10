
.. include:: /_includes/rllib/we_are_hiring.rst

.. _rllib-getting-started:

Getting Started
===============

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-in-60min:

RLlib in 60 minutes
-------------------

.. figure:: images/rllib-index-header.svg

In this tutorial, you learn how to design, customize, and run an end-to-end RLlib learning experiment
from scratch. This includes picking and configuring an :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`,
running a couple of training iterations, saving the state of your
:py:class:`~ray.rllib.algorithms.algorithm.Algorithm` from time to time, running a separate
evaluation loop, and finally utilizing one of the checkpoints to deploy your trained model
to an environment outside of RLlib and compute actions.

You also learn how to customize your :ref:`RL environment <rllib-key-concepts-environments>`
and your :ref:`neural network model <rllib-key-concepts-rl-modules>`.

Installation
~~~~~~~~~~~~

First, install RLlib, `PyTorch <https://pytorch.org>`__, and `Farama Gymnasium <https://gymnasium.farama.org>`__ as shown below:

.. code-block:: bash

    pip install "ray[rllib]" torch "gymnasium[atari,accept-rom-license,mujoco]"


.. _rllib-python-api:

Python API
~~~~~~~~~~

RLlib's Python API provides all the flexibility required for applying the library to any
type of RL problem.

You manage RLlib experiments through an instance of the :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
class. An :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` typically holds a neural
network for computing actions, called ``policy``, the :ref:`RL environment <rllib-key-concepts-environments>`
that you want to optimize against, a loss function, an optimizer, and some code describing the
algorithm's execution logic, like determining when to collect samples, when to update your model, etc..

In :ref:`multi-agent training <rllib-multi-agent-environments-doc>`,
:py:class:`~ray.rllib.algorithms.algorithm.Algorithm` manages the querying and optimization of multiple policies at once.

Through the algorithm's interface, you can train the policy, compute actions, or store your
algorithm's state through checkpointing.


Configure and build the algorithm
+++++++++++++++++++++++++++++++++

You first create an :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` instance
and change some default settings through the config object's various methods.

For example, you can set the :ref:`RL environment <rllib-key-concepts-environments>`
you want to use by calling the config's :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.environment`
method:

.. testcode::

    from ray.rllib.algorithms.ppo import PPOConfig

    # Create a config instance for the PPO algorithm.
    config = (
        PPOConfig()
        .environment("Pendulum-v1")
    )


To scale your setup and define how many :py:class:`~ray.rllib.env.env_runner.EnvRunner` actors you want to leverage,
you can call the :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.env_runners` method.
``EnvRunners`` are used to collect samples for training updates from your :ref:`environment <rllib-key-concepts-environments>`.

.. testcode::

    config.env_runners(num_env_runners=2)

For training-related settings or any algorithm-specific settings, use the
:py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.training` method:

.. testcode::

    config.training(
        lr=0.0002,
        train_batch_size_per_learner=2000,
        num_epochs=10,
    )

Finally, you build the actual :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` instance
through calling your config's :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.build_algo`
method.

.. testcode::

    # Build the Algorithm (PPO).
    ppo = config.build_algo()


.. note::

    See here to learn about all the :ref:`methods you can use to configure your Algorithm <rllib-algo-configuration-docs>`.


Run the algorithm
+++++++++++++++++

After you built your :ref:`PPO <ppo>` from its configuration, you can ``train`` it for a number of
iterations through calling the :py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.train` method,
which returns a result dictionary that you can pretty-print for debugging purposes:

.. testcode::

    from pprint import pprint

    for _ in range(4):
        pprint(ppo.train())


Checkpoint the algorithm
++++++++++++++++++++++++

To save the current state of your :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`,
create a checkpoint through calling its :py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.save_to_path` method,
which returns the directory of the saved checkpoint.

Instead of not passing any arguments to this call and letting the :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` decide where to save
the checkpoint, you can also provide a checkpoint directory yourself:

.. testcode::

    checkpoint_path = ppo.save_to_path()

    # OR:
    # ppo.save_to_path([a checkpoint location of your choice])


Evaluate the algorithm
++++++++++++++++++++++

RLlib supports setting up a separate :py:class:`~ray.rllib.env.env_runner_group.EnvRunnerGroup`
for the sole purpose of evaluating your model from time to time on the :ref:`RL environment <rllib-key-concepts-environments>`.

Use your config's :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.evaluation` method
to set up the details. By default, RLlib doesn't perform evaluation during training and only reports the
results of collecting training samples with its "regular" :py:class:`~ray.rllib.env.env_runner_group.EnvRunnerGroup`.


.. testcode::
    :hide:

    ppo.stop()


.. testcode::

    config.evaluation(
        # Run one evaluation round every iteration.
        evaluation_interval=1,

        # Create 2 eval EnvRunners in the extra EnvRunnerGroup.
        evaluation_num_env_runners=2,

        # Run evaluation for exactly 10 episodes. Note that because you have
        # 2 EnvRunners, each one runs through 5 episodes.
        evaluation_duration_unit="episodes",
        evaluation_duration=10,
    )

    # Rebuild the PPO, but with the extra evaluation EnvRunnerGroup
    ppo_with_evaluation = config.build_algo()

    for _ in range(3):
        pprint(ppo_with_evaluation.train())

.. testcode::
    :hide:

    ppo_with_evaluation.stop()


.. _rllib-with-ray-tune:

RLlib with Ray Tune
+++++++++++++++++++

All online RLlib :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` classes are compatible with
the :ref:`Ray Tune API <tune-api-ref>`.

.. note::

    The offline RL algorithms, like :ref:`BC <bc>`, :ref:`CQL <cql>`, and :ref:`MARWIL <marwil>`
    require more work on :ref:`Tune <tune-main>` and :ref:`Ray Data <data>`
    to add Ray Tune support.

This integration allows for utilizing your configured :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` in
:ref:`Ray Tune <tune-main>` experiments.

For example, the following code performs a hyper-parameter sweep of your :ref:`PPO <ppo>`, creating three ``Trials``,
one for each of the configured learning rates:

.. testcode::

    from ray import train, tune
    from ray.rllib.algorithms.ppo import PPOConfig

    config = (
        PPOConfig()
        .environment("Pendulum-v1")
        # Specify a simple tune hyperparameter sweep.
        .training(
            lr=tune.grid_search([0.001, 0.0005, 0.0001]),
        )
    )

    # Create a Tuner instance to manage the trials.
    tuner = tune.Tuner(
        config.algo_class,
        param_space=config,
        # Specify a stopping criterion. Note that the criterion has to match one of the
        # pretty printed result metrics from the results returned previously by
        # ``.train()``. Also note that -1100 is not a good episode return for
        # Pendulum-v1, we are using it here to shorten the experiment time.
        run_config=train.RunConfig(
            stop={"env_runners/episode_return_mean": -1100.0},
        ),
    )
    # Run the Tuner and capture the results.
    results = tuner.fit()

Note that each :py:class:`~ray.tune.trial.Trial` creates a separate
:py:class:`~ray.rllib.algorithms.algorithm.Algorithm` instance as a :ref:`Ray actor <actor-guide>`,
assigns compute resources to each ``Trial``, and runs them in parallel, if possible,
on your Ray cluster:

.. code-block:: text

    Trial status: 3 RUNNING
    Current time: 2025-01-17 18:47:33. Total running time: 3min 0s
    Logical resource usage: 9.0/12 CPUs, 0/0 GPUs
    ╭───────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
    │ Trial name                   status       lr   iter  total time (s)  episode_return_mean  .._sampled_lifetime │
    ├───────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
    │ PPO_Pendulum-v1_b5c41_00000  RUNNING  0.001      29         86.2426             -998.449               108000 │
    │ PPO_Pendulum-v1_b5c41_00001  RUNNING  0.0005     25         74.4335             -997.079               100000 │
    │ PPO_Pendulum-v1_b5c41_00002  RUNNING  0.0001     20         60.0421             -960.293                80000 │
    ╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────╯

``Tuner.fit()`` returns a ``ResultGrid`` object that allows for a detailed analysis of the
training process and for retrieving the :ref:`checkpoints <rllib-checkpoints-docs>` of the trained
algorithms and their models:

.. testcode::
    # Get the best result of the final iteration, based on a particular metric.
    best_result = results.get_best_result(
        metric="env_runners/episode_return_mean",
        mode="max",
        scope="last",
    )

    # Get the best checkpoint corresponding to the best result
    # from the preceding experiment.
    best_checkpoint = best_result.checkpoint


Deploy a trained model for inference
++++++++++++++++++++++++++++++++++++

After training, you might want to deploy your models into a new environment, for example
to run inference in production. For this purpose, you can use the checkpoint directory created
in the preceding example. To read more about checkpoints, model deployments, and restoring algorithm state,
see this :ref:`page on checkpointing <rllib-checkpoints-docs>` here.

Here is how you would create a new model instance from the checkpoint and run inference through
a single episode of your RL environment. Note in particular the use of the
:py:meth:`~ray.rllib.utils.checkpoints.Checkpointable.from_checkpoint` method to create
the model and the
:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_inference`
method to compute actions:


.. testcode::

    from pathlib import Path
    import gymnasium as gym
    import numpy as np
    import torch
    from ray.rllib.core.rl_module import RLModule

    # Create only the neural network (RLModule) from our algorithm checkpoint.
    # See here (https://docs.ray.io/en/master/rllib/checkpoints.html)
    # to learn more about checkpointing and the specific "path" used.
    rl_module = RLModule.from_checkpoint(
        Path(best_checkpoint.path)
        / "learner_group"
        / "learner"
        / "rl_module"
        / "default_policy"
    )

    # Create the RL environment to test against (same as was used for training earlier).
    env = gym.make("Pendulum-v1", render_mode="human")

    episode_return = 0.0
    done = False

    # Reset the env to get the initial observation.
    obs, info = env.reset()

    while not done:
        # Uncomment this line to render the env.
        # env.render()

        # Compute the next action from a batch (B=1) of observations.
        obs_batch = torch.from_numpy(obs).unsqueeze(0)  # add batch B=1 dimension
        model_outputs = rl_module.forward_inference({"obs": obs_batch})

        # Extract the action distribution parameters from the output and dissolve batch dim.
        action_dist_params = model_outputs["action_dist_inputs"][0].numpy()

        # We have continuous actions -> take the mean (max likelihood).
        greedy_action = np.clip(
            action_dist_params[0:1],  # 0=mean, 1=log(stddev), [0:1]=use mean, but keep shape=(1,)
            a_min=env.action_space.low[0],
            a_max=env.action_space.high[0],
        )
        # For discrete actions, you should take the argmax over the logits:
        # greedy_action = np.argmax(action_dist_params)

        # Send the action to the environment for the next step.
        obs, reward, terminated, truncated, info = env.step(greedy_action)

        # Perform env-loop bookkeeping.
        episode_return += reward
        done = terminated or truncated

    print(f"Reached episode return of {episode_return}.")


Alternatively, if you still have an :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` instance up and running
in your script, you can get the :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` through the
:py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.get_module` method:

.. code-block:: python

    rl_module = ppo.get_module("default_policy")  # Equivalent to `rl_module = ppo.get_module()`


Customizing your RL environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the preceding examples, your :ref:`RL environment <rllib-key-concepts-environments>` was
a `Farama gymnasium <gymnasium.farama.org>`__ pre-registered one,
like ``Pendulum-v1`` or ``CartPole-v1``. However, if you would like to run your
experiments against a custom one, see this tab below for a less-than-50-lines example.

See here for an :ref:`in-depth guide on how to setup RL environments in RLlib <rllib-environments-doc>`
and how to customize them.

.. dropdown:: Quickstart: Custom RL environment
    :animate: fade-in-slide-down

    .. testcode::

        import gymnasium as gym
        from ray.rllib.algorithms.ppo import PPOConfig

        # Define your custom env class by subclassing gymnasium.Env:

        class ParrotEnv(gym.Env):
            """Environment in which the agent learns to repeat the seen observations.

            Observations are float numbers indicating the to-be-repeated values,
            e.g. -1.0, 5.1, or 3.2.
            The action space is the same as the observation space.
            Rewards are `r=-abs([observation] - [action])`, for all steps.
            """
            def __init__(self, config=None):
                # Since actions should repeat observations, their spaces must be the same.
                self.observation_space = config.get(
                    "obs_act_space",
                    gym.spaces.Box(-1.0, 1.0, (1,), np.float32),
                )
                self.action_space = self.observation_space
                self._cur_obs = None
                self._episode_len = 0

            def reset(self, *, seed=None, options=None):
                """Resets the environment, starting a new episode."""
                # Reset the episode len.
                self._episode_len = 0
                # Sample a random number from our observation space.
                self._cur_obs = self.observation_space.sample()
                # Return initial observation.
                return self._cur_obs, {}

            def step(self, action):
                """Takes a single step in the episode given `action`."""
                # Set `terminated` and `truncated` flags to True after 10 steps.
                self._episode_len += 1
                terminated = truncated = self._episode_len >= 10
                # Compute the reward: `r = -abs([obs] - [action])`
                reward = -sum(abs(self._cur_obs - action))
                # Set a new observation (random sample).
                self._cur_obs = self.observation_space.sample()
                return self._cur_obs, reward, terminated, truncated, {}

        # Point your config to your custom env class:
        config = (
            PPOConfig()
            .environment(
                ParrotEnv,
                # Add `env_config={"obs_act_space": [some Box space]}` to customize.
            )
        )

        # Build a PPO algorithm and train it.
        ppo_w_custom_env = config.build_algo()
        ppo_w_custom_env.train()

    .. testcode::
        :hide:

        ppo_w_custom_env.stop()


Customizing your models
~~~~~~~~~~~~~~~~~~~~~~~

In the preceding examples, because you didn't specify anything in your
:py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`, RLlib provided a default
neural network model. If you would like to either reconfigure the type and size of RLlib's default models,
for example define the number of hidden layers and their activation functions,
or even write your own custom models from scratch using PyTorch, see here
for a :ref:`detailed guide on the RLModule class <rlmodule-guide>`.

See this tab below for a 30-lines example.

.. dropdown:: Quickstart: Custom RLModule
    :animate: fade-in-slide-down

    .. testcode::

        import torch

        from ray.rllib.core.columns import Columns
        from ray.rllib.core.rl_module.torch import TorchRLModule

        # Define your custom env class by subclassing `TorchRLModule`:
        class CustomTorchRLModule(TorchRLModule):
            def setup(self):
                # You have access here to the following already set attributes:
                # self.observation_space
                # self.action_space
                # self.inference_only
                # self.model_config  # <- a dict with custom settings
                input_dim = self.observation_space.shape[0]
                hidden_dim = self.model_config["hidden_dim"]
                output_dim = self.action_space.n

                # Define and assign your torch subcomponents.
                self._policy_net = torch.nn.Sequential(
                    torch.nn.Linear(input_dim, hidden_dim),
                    torch.nn.ReLU(),
                    torch.nn.Linear(hidden_dim, output_dim),
                )

            def _forward(self, batch, **kwargs):
                # Push the observations from the batch through our `self._policy_net`.
                action_logits = self._policy_net(batch[Columns.OBS])
                # Return parameters for the default action distribution, which is
                # `TorchCategorical` (due to our action space being `gym.spaces.Discrete`).
                return {Columns.ACTION_DIST_INPUTS: action_logits}
