.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst


.. _new-api-stack-migration-guide:


New API Stack Migration Guide
=============================

This page explains - step by step - how to convert and translate your existing old API stack
RLlib classes and code to RLlib's new API stack.
:ref:`Read here on what the new API stack is and why you should migrate soon<rllib-new-api-stack-guide>`.


.. note::

    Even though the new API stack still rudimentary supports `TensorFlow <https://tensorflow.org>`__ and
    has been written in a framework-agnostic fashion, RLlib will soon move to `PyTorch <https://pytorch.org>`__
    as the only supported deep learning framework (dropping TensorFlow support entirely).


Change your AlgorithmConfig
---------------------------

The new API stack is disabled by default for all RLlib algorithms. To activate it, use the `api_stack()` method
in your `AlgorithmConfig` object like so:

.. testcode::

    from ray.rllib.algorithms.ppo import PPOConfig

    config = (
        PPOConfig()
        # Switch both the new API stack flags to True (both False by default).
        # This enables the use of
        # a) RLModule (replaces ModelV2) and Learner (replaces Policy)
        # b) the correct EnvRunner (single-agent vs multi-agent) and ConnectorV2 pipelines.
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
    )


Note that there are a few other differences between configuring an old stack algorithm
and its new stack counterpart. The following sections list all important differences.
Go through the list of changes and make sure you are either translating the respective
settings or drop them altogether, if they are no longer supported or needed in the new API stack:


AlgorithmConfig.framework()
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Even though the new API stack still rudimentary supports `TensorFlow <https://tensorflow.org>`__, RLlib will soon move to
`PyTorch <https://pytorch.org>`__ as the only supported deep learning framework.

This means that the following framework-related settings will be deprecated when using the new API stack:

.. testcode::

    config.framework(
        eager_tracing=True,
        eager_max_retraces=20,
        tf_session_args={},
        local_tf_session_args={},
    )


AlgorithmConfig.resources()
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `num_gpus` and `_fake_gpus` settings have been deprecated. In order to place your RLModule on one or more GPUs on the Learner side,
do the following:

.. testcode::

    # The following setting is equivalent to the old stack's `config.resources(num_gpus=2)`.
    config.learners(
        num_learners=2,
        num_gpus_per_learner=1,
    )

`See here for an example on how to train with fractional GPUs <https://github.com/ray-project/ray/blob/master/rllib/examples/gpus/fractional_gpus_per_learner.py>`__.

If you want to learn with more than one :py:class:`~ray.rllib.core.learner.learner.Learner`
in a multi-**CPU** fashion, you should do:

.. testcode::

    config.learners(
        num_learners=2,  # or larger
        num_cpus_per_learner=1,  # <- default
        num_gpus_per_learner=0,  # <- default
    )

The setting `num_cpus_for_local_worker` has been renamed to `num_cpus_for_main_process`.

.. testcode::

    config.resources(num_cpus_for_main_process=0)  # default is 1


AlgorithmConfig.training()
~~~~~~~~~~~~~~~~~~~~~~~~~~

Train Batch Size
................

Due to the new API stack's Learner worker architecture (training may be distributed over n
:py:class:`~ray.rllib.core.learner.learner.Learner` workers), the train batch size is now provided per
:py:class:`~ray.rllib.core.learner.learner.Learner`:


.. testcode::

    config.training(
        train_batch_size_per_learner=512,
    )

This way, you won't need to touch this setting, even when increasing the number of :py:class:`~ray.rllib.core.learner.learner.Learner`
(through `config.learners(num_learners=...)`).


Neural Network Config
.....................

The old stack's `config.training(model=...)` is no longer supported on the new stack.
Instead, use the new stack's :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.rl_module`
method to configure an RLlib default RLModule or specify (and configure) a custom one.

:ref:`See here for more details on how to configure your RLModule(s) <rlmodule-guide>`.


Learner Config
..............




Learning Rate- and Coefficient Schedules
........................................

If you are using schedules for your learning rate or other coefficients (for example the
`entropy_coeff` setting in PPO), provide scheduling information directly in the respective setting.
There is no specific, separate setting anymore for scheduling behavior.

When defining a schedule, provide a list of 2-tuples, where the first item is the global timestep
(*num_env_steps_sampled_lifetime* in the reported metrics) and the second item is the value that should
be reached at that timestep. Always start the first 2-tuple with timestep 0. Note that values between
two provided timesteps are linearly interpolated.

For example, to create a learning rate schedule that starts with a value of 1e-5, then increases over 1M timesteps to 1e-4 and stays constant after that, do:

.. testcode::

    config.training(
        lr=[
            [0, 1e-5],  # <- initial value at timestep 0
            [1000000, 1e-4],  # <- final value at 1M timesteps
        ],
    )


In the above example, the value after 500k timesteps will roughly be `5e-5` (linear interpolation).

Another example: To create a entropy coefficient schedule that starts with a value of 0.05, then increases over 1M timesteps to 0.1 and
then suddenly drops to 0 (after the 1Mth timestep), do:

.. testcode::

    config.training(
        entropy_coeff=[
            [0, 0.05],  # <- initial value at timestep 0
            [1000000, 0.1],  # <- value at 1M timesteps
            [1000001, 0.0],  # <- sudden drop to 0.0 right after 1M timesteps
        ]
    )


AlgorithmConfig.env_runners()
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. testcode::

    # RolloutWorkers have been re-written to EnvRunners:
    config.env_runners(
        num_env_runners=2,  # use this instead of `num_workers`
    )

    # The following `env_runners` settings are deprecated and should no longer be explicitly
    # set on the new stack:
    config.env_runners(
        create_env_on_local_worker=False,
        sample_collector=None,
        enable_connectors=True,
        remote_worker_envs=False
        remote_env_batch_wait_ms=0,
        preprocessor_pref="deepmind",
        enable_tf1_exec_eagerly=False,
        sampler_perf_stats_ema_coef=None,
    )


In case you were using the `observation_filter` setting, perform the following translations:

.. testcode::

    # For `observation_filter="NoFilter"`, do not set anything in particular, this is the default.

    # For `observation_filter="MeanStdFilter"`, do:
    from ray.rllib.connectors.env_to_module import MeanStdFilter

    config.env_runners(
        env_to_module_connector=lambda env: MeanStdFilter(multi_agent=False),  # <- or True
    )


AlgorithmConfig.exploration()
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Only the `explore` setting remains. It determines, whether the :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration` (in case `explore=True`)
or the :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_inference` (in case `explore=False`) method
is called on your :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`
inside the :py:class:`~ray.rllib.env.env_runner.EnvRunner`.


.. testcode::

    config.exploration(explore=True)  # <- or False


The `exploration_config` setting is deprecated and no longer used. Instead, determine the exact exploratory
behavior (for example, sample an action from a distribution) inside the overridden
:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration` method of your
:py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`.


Custom Callbacks
----------------

If you are using custom callbacks on the old API stack, you are subclassing the :py:class`~ray.rllib.algorithms.callbacks.DefaultCallbacks` class.
You can continue doing so on the new API stack and also pass your custom subclass to your config like so:

.. testcode::

    # config.callbacks(YourCallbacksClass)

However, if you are overriding those methods triggered on the EnvRunner side (`on_episode_start/stop/step/etc..`),
you might have to do a small amount of translation, because the
arguments passed into many of these methods may have changed.

Here is a 1:1 translation guide for those types of Callbacks methods:

.. testcode::

    from ray.rllib.algorithms.callbacks import DefaultCallbacks

    class YourCallbacksClass(DefaultCallbacks):

        def on_episode_start(
            self,
            *,
            episode,
            env_runner,
            metrics_logger,
            env,
            env_index,
            rl_module,

            # Old API stack args; you should no longer use/access these inside your method code.
            worker=None,
            base_env=None,
            policies=None,
            **kwargs,
        ):
            # The `SingleAgentEpisode` or `MultiAgentEpisode` that has just been started.
            # See here for more details: https://docs.ray.io/en/latest/rllib/single-agent-episode.html
            print(episode)

            # The `EnvRunner` class where the episode in question is being collected.
            # This used to be a `RolloutWorker`, now on the new stack, this is either a
            # `SingleAgentEnvRunner` or a `MultiAgentEnvRunner` holding the gymnasium Env,
            # the RLModule, and the 2 connector pipelines (env-to-module and module-to-env).
            print(env_runner)

            # The MetricsLogger object on the EnvRunner (documentation is wip).
            print(metrics_logger.peek("episode_return_mean", default=0.0))

            # The gymnasium env used for sample collection. Note that this may be a
            # gymnasium.vector.VectorEnv.
            print(env)

            # The env index (in case of a vector env) that handles the `episode`.
            print(env_index)

            # The RLModule used on this EnvRunner. Note that this may be a "plain" (single-agent)
            # `RLModule` or a `MultiRLModule` (in the multi-agent case).
            print(rl_module)

    # Change similarly:
    # on_episode_created()
    # on_episode_step()
    # on_episode_end()


The following callback methods are no longer available on the new API stack:

**`on_sub_environment_created()`**: The new API stack uses Farama's gymnasium vector Envs leaving no control for RLlib
to call a callback on each individual env-index's creation.

**`on_create_policy()`**: This method is only called on :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker`
and thus no longer available on the new API stack.

**`on_postprocess_trajectory()`**: Trajectory processing is handled entirely through :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2`
pipelines on the new API stack.


TODO: Continue here!!



ModelV2 to RLModule
-------------------

In case you are using a custom :py:class:`~ray.rllib.models.modelv2.ModelV2` class and would like to translate
the entire NN architecture and possibly action distribution logic to the new API stack, take a look at
the :ref:`RLModule documentation <rlmodule-guide>` first, then come back to this location here.

Here are also helpful example scripts on `how to write a custom CNN-containing RLModule <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/custom_cnn_rl_module.py>`__
and `how to write a custom LSTM-containing RLModule <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/custom_lstm_rl_module.py>`__.

Also, there are different options for translating an existing, custom :py:class:`~ray.rllib.models.modelv2.ModelV2` (old API stack)
to the new API stack's :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`:

1) You lift your ModelV2 code and drop it into a new, custom RLModule class (see the :ref:`RLModule documentation <rlmodule-guide>` for details).
1) You use an Algorithm checkpoint or a Policy checkpoint that you have from an old API stack training run and use this with the `new stack RLModule convenience wrapper <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/migrate_modelv2_to_new_api_stack_by_policy_checkpoint.py>`__.
1) You have an :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` object from an old API stack training run and use this with the `new stack RLModule convenience wrapper <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/migrate_modelv2_to_new_api_stack_by_config.py>`__.


Custom Loss Functions and/or Policies
-------------------------------------

In case you are using one or more custom loss functions and/or custom (PyTorch) optimizers to train your models, instead of doing these
customizations inside the old stack's Policy class, you need to move these logics into the new API stack's
:py:class:`~ray.rllib.core.learner.learner.Learner` class.

:ref:`See here for more details on how to write a custom Learner <learner-guide>`.

Note that the Policy class is no longer supported in the new API stack. This class used to hold a
neural network (now moved into :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`),
a (old stack) connector (now moved into :py:class:`~ray.rllib.connector.connector_v2.ConnectorV2`),
and one or more optimizers and losses (now moved into :py:class:`~ray.rllib.core.learner.learner.Learner`).

The RLModule API is much more flexible than the old stack's Policy API and provides a cleaner separation-of-concerns experience (things
related to action inference run on the EnvRunners, things related to updating run on the Learner workers).


Custom (old-stack) Connectors
-----------------------------

If you are using custom (old API stack) connectors,
