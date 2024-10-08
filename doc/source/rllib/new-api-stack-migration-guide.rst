.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst


.. _rllib-new-api-stack-migration-guide:


New API stack migration guide
=============================

This page explains, step by step, how to convert and translate your existing old API stack
RLlib classes and code to RLlib's new API stack.
:ref:`Why you should migrate to the new API stack <rllib-new-api-stack-guide>`.


.. note::

    Even though the new API stack still provides rudimentary support for `TensorFlow <https://tensorflow.org>`__,
    RLlib supports a single deep learning framework, the `PyTorch <https://pytorch.org>`__ 
    framework, dropping TensorFlow support entirely.
    Note, though, that the Ray team continues to  design RLlib to be framework-agnostic.


Change your AlgorithmConfig
---------------------------

RLlib turns off the new API stack by default for all RLlib algorithms. To activate it, use the `api_stack()` method
in your `AlgorithmConfig` object like so:

.. testcode::

    from ray.rllib.algorithms.ppo import PPOConfig

    config = (
        PPOConfig()
        # Switch both the new API stack flags to True (both False by default).
        # This action enables the use of
        # a) RLModule (replaces ModelV2) and Learner (replaces Policy).
        # b) the correct EnvRunner, which replaces RolloutWorker, and
        #    ConnectorV2 pipelines, which replaces the old stack Connectors.
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
    )


Note that there are a few other differences between configuring an old API stack algorithm
and its new stack counterpart.
Go through the following sections and make sure you're translating the respective
settings. Remove settings that the new stack doesn't support or need.


AlgorithmConfig.framework()
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Even though the new API stack still provides rudimentary support for `TensorFlow <https://tensorflow.org>`__,
RLlib supports a single deep learning framework, the `PyTorch <https://pytorch.org>`__ framework.

The new API stack deprecates the following framework-related settings:

.. testcode::

    # Make sure you always set the framework to "torch"...
    config.framework("torch")

    # ... and drop all tf-specific settings.
    config.framework(
        eager_tracing=True,
        eager_max_retraces=20,
        tf_session_args={},
        local_tf_session_args={},
    )


AlgorithmConfig.resources()
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `num_gpus` and `_fake_gpus` settings have been deprecated. To place your
RLModule on one or more GPUs on the Learner side, do the following:

.. testcode::

    # The following setting is equivalent to the old stack's `config.resources(num_gpus=2)`.
    config.learners(
        num_learners=2,
        num_gpus_per_learner=1,
    )

.. hint::

    The `num_learners` setting determines how many remote :py:class:`~ray.rllib.core.learner.learner.Learner`
    workers there are in your Algorithm's :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup`.
    If you set this to 0, your LearnerGroup only contains a **local** Learner that runs on the main
    process (and shares the compute resources with that process, usually 1 CPU).
    For asynchronous algorithms like IMPALA or APPO, this setting should therefore always be >0.

`See here for an example on how to train with fractional GPUs <https://github.com/ray-project/ray/blob/master/rllib/examples/gpus/fractional_gpus_per_learner.py>`__.
Also note that for fractional GPUs, you should always set `num_learners` to 0 or 1.

If GPUs aren't available, but you want to learn with more than one
:py:class:`~ray.rllib.core.learner.learner.Learner` in a multi-**CPU** fashion, you can do the following:

.. testcode::

    config.learners(
        num_learners=2,  # or >2
        num_cpus_per_learner=1,  # <- default
        num_gpus_per_learner=0,  # <- default
    )

The setting `num_cpus_for_local_worker` has been renamed to `num_cpus_for_main_process`.

.. testcode::

    config.resources(num_cpus_for_main_process=0)  # default is 1


AlgorithmConfig.training()
~~~~~~~~~~~~~~~~~~~~~~~~~~

Train batch size
................

Due to the new API stack's :py:class:`~ray.rllib.core.learner.learner.Learner` worker
architecture, training may be distributed over n
:py:class:`~ray.rllib.core.learner.learner.Learner` workers, so RLlib provides the train batch size
per individual :py:class:`~ray.rllib.core.learner.learner.Learner`.
You should no longer use the `train_batch_size` setting:


.. testcode::

    config.training(
        train_batch_size_per_learner=512,
    )

You don't need to change this setting, even when increasing the number of
:py:class:`~ray.rllib.core.learner.learner.Learner`, through `config.learners(num_learners=...)`.

Note that a good rule of thumb for scaling on the learner axis is to keep the
`train_batch_size_per_learner` value constant with a growing number of Learners and
to increase the learning rate as follows:

`lr = [original_lr] * ([num_learners] ** 0.5)`


Neural network configuration
............................

The old stack's `config.training(model=...)` is no longer supported on the new API stack.
Instead, use the new :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.rl_module`
method to configure RLlib's default :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`
or specify and configure a custom :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`.

See :ref:`RLModules API <rlmodule-guide>`, a general guide that also explains
the use of the `config.rl_module()` method.

If you have an old stack `ModelV2` and want to migrate the entire NN logic to the
new stack, see :ref:`ModelV2 to RLModule <rllib-modelv2-to-rlmodule>` for migration instructions.


Learning rate- and coefficient schedules
........................................

If you're using schedules for learning rate or other coefficients, for example, the
`entropy_coeff` setting in PPO, provide scheduling information directly in the respective setting.
Scheduling behavior doesn't require a specific, separate setting anymore.

When defining a schedule, provide a list of 2-tuples, where the first item is the global timestep
(*num_env_steps_sampled_lifetime* in the reported metrics) and the second item is the value that the learning rate should reach at that timestep.
Always start the first 2-tuple with timestep 0. Note that RLlib linearly interpolates values between
two provided timesteps.

For example, to create a learning rate schedule that starts with a value of 1e-5, then increases over 1M timesteps to 1e-4 and stays constant after that, do the following:

.. testcode::

    config.training(
        lr=[
            [0, 1e-5],  # <- initial value at timestep 0
            [1000000, 1e-4],  # <- final value at 1M timesteps
        ],
    )


In the preceding example, the value after 500k timesteps is roughly `5e-5` from linear interpolation.

Another example is to create an entropy coefficient schedule that starts with a value of 0.05, then increases over 1M timesteps to 0.1 and
then suddenly drops to 0, after the 1Mth timestep, do the following:

.. testcode::

    config.training(
        entropy_coeff=[
            [0, 0.05],  # <- initial value at timestep 0
            [1000000, 0.1],  # <- value at 1M timesteps
            [1000001, 0.0],  # <- sudden drop to 0.0 right after 1M timesteps
        ]
    )

In case you need to configure a more complex learning rate scheduling behavior or chain different schedulers
into a pipeline, you can use the experimental `_torch_lr_schedule_classes` config property.
See `this example script <https://github.com/ray-project/ray/blob/master/rllib/examples/learners/ppo_with_torch_lr_schedulers.py>`__  for more details.
Note that this example only covers learning rate schedules, but not any other coefficients.


AlgorithmConfig.learners()
~~~~~~~~~~~~~~~~~~~~~~~~~~

This method isn't used on the old API stack because the old stack doesn't use Learner workers.

It allows you to specify:

#. the number of `Learner` workers through `.learners(num_learners=...)`.
#. the resources per learner; use `.learners(num_gpus_per_learner=1)` for GPU training
   and `.learners(num_gpus_per_learner=0)` for CPU training.
#. the custom Learner class you want to use (`example on how to do this here <https://github.com/ray-project/ray/blob/master/rllib/examples/learners/custom_loss_fn_simple.py>`__)
#. a config dict you would like to set for your custom learner:
   `.learners(learner_config_dict={...})`. Note that every `Learner` has access to the
   entire `AlgorithmConfig` object through `self.config`, but setting the
   `learner_config_dict` is a convenient way to avoid having to create an entirely new
   `AlgorithmConfig` subclass only to support a few extra settings for your custom
   `Learner` class.


AlgorithmConfig.env_runners()
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. testcode::

    # RolloutWorkers have been replace by EnvRunners. EnvRunners are more efficient and offer
    # a more separation-of-concerns design and cleaner code.
    config.env_runners(
        num_env_runners=2,  # use this instead of `num_workers`
    )

    # The following `env_runners` settings are deprecated and should no longer be explicitly
    # set on the new stack:
    config.env_runners(
        create_env_on_local_worker=False,
        sample_collector=None,
        enable_connectors=True,
        remote_worker_envs=False,
        remote_env_batch_wait_ms=0,
        preprocessor_pref="deepmind",
        enable_tf1_exec_eagerly=False,
        sampler_perf_stats_ema_coef=None,
    )

.. hint::

    If you want to IDE-debug what's going on inside your `EnvRunners`, set `num_env_runners=0`
    and make sure you are running your experiment locally and not through Ray Tune.
    In order to do this with any of RLlib's `example <https://github.com/ray-project/ray/tree/master/rllib/examples>`__
    or `tuned_example <https://github.com/ray-project/ray/tree/master/rllib/tuned_examples>`__ scripts,
    simply set the command line args: `--no-tune --num-env-runners=0`.

In case you were using the `observation_filter` setting, perform the following translations:

.. testcode::

    # For `observation_filter="NoFilter"`, don't set anything in particular. This is the default.

    # For `observation_filter="MeanStdFilter"`, do the following:
    from ray.rllib.connectors.env_to_module import MeanStdFilter

    config.env_runners(
        env_to_module_connector=lambda env: MeanStdFilter(multi_agent=False),  # <- or True
    )


AlgorithmConfig.exploration()
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The new stack only supports the `explore` setting.
It determines whether the :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration`, in the case `explore=True`,
or the :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_inference`, in the case `explore=False`, is the method
your :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` calls
inside the :py:class:`~ray.rllib.env.env_runner.EnvRunner`.

.. testcode::

    config.exploration(explore=True)  # <- or False


The `exploration_config` setting is deprecated and no longer used. Instead, determine the exact exploratory
behavior, for example, sample an action from a distribution, inside the overridden
:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration` method of your
:py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`.


Custom callbacks
----------------

If you're using custom callbacks on the old API stack, you're subclassing the :py:class`~ray.rllib.algorithms.callbacks.DefaultCallbacks` class.
You can continue this approach with the new API stack and also pass your custom subclass to your config like the following:

.. testcode::

    # config.callbacks(YourCallbacksClass)

However, if you're overriding those methods that the EnvRunner side triggered, for example,`on_episode_start/stop/step/etc...`,
you might have to do a small amount of translation, because the
EnvRunner may have changed the arguments that RLlib passes to many of these methods.

The following is a one-to-one translation guide for these types of Callbacks methods:

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

            # Old API stack args; don't use or access these inside your method code.
            worker=None,
            base_env=None,
            policies=None,
            **kwargs,
        ):
            # The `SingleAgentEpisode` or `MultiAgentEpisode` that RLlib has just started.
            # See https://docs.ray.io/en/latest/rllib/single-agent-episode.html for more details: 
            print(episode)

            # The `EnvRunner` class that collects the episode in question.
            # This class used to be a `RolloutWorker`. On the new stack, this class is either a
            # `SingleAgentEnvRunner` or a `MultiAgentEnvRunner` holding the gymnasium Env,
            # the RLModule, and the 2 connector pipelines, env-to-module and module-to-env.
            print(env_runner)

            # The MetricsLogger object on the EnvRunner (documentation is a WIP).
            print(metrics_logger.peek("episode_return_mean", default=0.0))

            # The gymnasium env that sample collection uses. Note that this env may be a
            # gymnasium.vector.VectorEnv.
            print(env)

            # The env index, in case of a vector env, that handles the `episode`.
            print(env_index)

            # The RL Module that this EnvRunner uses. Note that this module may be a "plain", single-agent
            # `RLModule`, or a `MultiRLModule` in the multi-agent case.
            print(rl_module)

    # Change similarly:
    # on_episode_created()
    # on_episode_step()
    # on_episode_end()


The following callback methods are no longer available on the new API stack:

**`on_sub_environment_created()`**: The new API stack uses `Farama's gymnasium <https://farama.org>`__ vector Envs leaving no control for RLlib
to call a callback on each individual env-index's creation.

**`on_create_policy()`**: This method is no longer available on the new API stack because only :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` calls it.

**`on_postprocess_trajectory()`**: The new API stack no longer triggers and calls this method,
because :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` pipelines handle trajectory processing entirely.
The documention for :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` documentation is under development.


.. _rllib-modelv2-to-rlmodule:

ModelV2 to RLModule
-------------------

If you're using a custom :py:class:`~ray.rllib.models.modelv2.ModelV2` class and want to translate
the entire NN architecture and possibly action distribution logic to the new API stack, see
:ref:`RL Modules <rlmodule-guide>` in addition to this section.

See these example scripts on `how to write a custom CNN-containing RL Module <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/custom_cnn_rl_module.py>`__
and `how to write a custom LSTM-containing RL Module <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/custom_lstm_rl_module.py>`__.

There are various options for translating an existing, custom :py:class:`~ray.rllib.models.modelv2.ModelV2` from the old API stack,
to the new API stack's :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`:

#. Move your ModelV2 code to a new, custom `RLModule` class. See :ref:`RL Modules <rlmodule-guide>` for details).
#. Use an Algorithm checkpoint or a Policy checkpoint that you have from an old API stack
   training run and use this checkpoint with the `new stack RL Module convenience wrapper <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/migrate_modelv2_to_new_api_stack_by_policy_checkpoint.py>`__.
#. Use an existing :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`
   object from an old API stack training run, with the `new stack RL Module convenience wrapper <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/migrate_modelv2_to_new_api_stack_by_config.py>`__.


Custom loss functions and policies
-------------------------------------

If you're using one or more custom loss functions or custom (PyTorch) optimizers to train your models, instead of doing these
customizations inside the old stack's Policy class, you need to move the logic into the new API stack's
:py:class:`~ray.rllib.core.learner.learner.Learner` class.

See :ref:`Learner <learner-guide>` for details on how to write a custom Learner .

The following example scripts show how to write:
- `a simple custom loss function <https://github.com/ray-project/ray/blob/master/rllib/examples/learners/custom_loss_fn_simple.py>`__
- `a custom Learner with 2 optimizers and different learning rates for each <https://github.com/ray-project/ray/blob/master/rllib/examples/learners/separate_vf_lr_and_optimizer.py>`__.

Note that the new API stack doesn't support the Policy class. In the old stack, this class holds a
neural network, which is the :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` in the new API stack,
an old stack connector, which is the :py:class:`~ray.rllib.connector.connector_v2.ConnectorV2` in the new API stack,
and one or more optimizers and losses, which are the :py:class:`~ray.rllib.core.learner.learner.Learner` class in the new API stack.

The RL Module API is much more flexible than the old stack's Policy API and
provides a cleaner separation-of-concerns experience. Things related to action
inference run on the EnvRunners, and things related to updating run on the Learner workers
It also provides superior scalability, allowing training in a multi-GPU setup in any Ray cluster
and multi-node with multi-GPU training on the `Anyscale <https://anyscale.com>`__ platform.


Custom connectors (old-stack) 
-----------------------------

If you're using custom connectors from the old API stack, move your logic into the
new :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` API.
Translate your agent connectors into env-to-module ConnectorV2 pieces and your
action connectors into module-to-env ConnectorV2 pieces.

The :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` documentation is under development.

The following are some examples on how to write ConnectorV2 pieces for the
different pipelines:

#. `Observation frame-stacking <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/frame_stacking.py>`__.
#. `Add the most recent action and reward to the RL Module's input <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/prev_actions_prev_rewards.py>`__.
#. `Mean-std filtering on all observations <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/mean_std_filtering.py>`__.
#. `Flatten any complex observation space to a 1D space <https://github.com/ray-project/ray/blob/master/rllib/examples/connectors/flatten_observations_dict_space.py>`__.
