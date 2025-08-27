.. include:: /_includes/rllib/we_are_hiring.rst

.. _rllib-new-api-stack-migration-guide:

.. testcode::
    :hide:

    from ray.rllib.algorithms.ppo import PPOConfig
    config = PPOConfig()


New API stack migration guide
=============================

.. include:: /_includes/rllib/new_api_stack.rst

This page explains, step by step, how to convert and translate your existing old API stack
RLlib classes and code to RLlib's new API stack.


What's the new API stack?
--------------------------

The new API stack is the result of re-writing the core RLlib APIs from scratch and reducing
user-facing classes from more than a dozen critical ones down to only a handful
of classes, without any loss of features. When designing these new interfaces,
the Ray Team strictly applied the following principles:

* Classes must be usable outside of RLlib.
* Separation of concerns. Try to answer: "**What** should get done **when** and **by whom**?"
  and give each class as few non-overlapping and clearly defined tasks as possible.
* Offer fine-grained modularity, full interoperability, and frictionless pluggability of classes.
* Use widely accepted third-party standards and APIs wherever possible.

Applying the preceding principles, the Ray Team reduced the important **must-know** classes
for the average RLlib user from eight on the old stack, to only five on the new stack.
The **core** new API stack classes are:

* :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`, which replaces ``ModelV2`` and ``PolicyMap`` APIs
* :py:class:`~ray.rllib.core.learner.learner.Learner`, which replaces ``RolloutWorker`` and some of ``Policy``
* :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` and :py:class:`~ray.rllib.env.multi_agent_episode.MultiAgentEpisode`, which replace ``ViewRequirement``, ``SampleCollector``, ``Episode``, and ``EpisodeV2``
* :py:class:`~ray.rllib.connector.connector_v2.ConnectorV2`, which replaces ``Connector`` and some of ``RolloutWorker`` and ``Policy``

The :py:class:`~ray.rllib.algorithm.algorithm_config.AlgorithmConfig` and
:py:class:`~ray.rllib.algorithm.algorithm.Algorithm` APIs remain as-is.
These classes are already established APIs on the old stack.


.. note::

    Even though the new API stack still provides rudimentary support for `TensorFlow <https://tensorflow.org>`__,
    RLlib supports a single deep learning framework, the `PyTorch <https://pytorch.org>`__
    framework, dropping TensorFlow support entirely.
    Note, though, that the Ray team continues to design RLlib to be framework-agnostic
    and may add support for additional frameworks in the future.


Check your AlgorithmConfig
--------------------------

RLlib turns on the new API stack by default for all RLlib algorithms.

.. note::
    To **deactivate** the new API stack and switch back to the old one, use the
    `api_stack()` method in your `AlgorithmConfig` object like so:

    .. testcode::

        config.api_stack(
            enable_rl_module_and_learner=False,
            enable_env_runner_and_connector_v2=False,
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

The Ray team deprecated the ``num_gpus`` and ``_fake_gpus`` settings. To place your
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
    If you set this parameter to ``0``, your LearnerGroup only contains a **local** Learner that runs on the main
    process and shares its compute resources, typically 1 CPU.
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

the Ray team renamed the setting ``num_cpus_for_local_worker`` to ``num_cpus_for_main_process``.

.. testcode::

    config.resources(num_cpus_for_main_process=0)  # default is 1


AlgorithmConfig.training()
~~~~~~~~~~~~~~~~~~~~~~~~~~

Train batch size
................

Due to the new API stack's :py:class:`~ray.rllib.core.learner.learner.Learner` worker architecture,
training may happen in distributed fashion over ``n`` :py:class:`~ray.rllib.core.learner.learner.Learner` workers,
so RLlib provides the train batch size per individual :py:class:`~ray.rllib.core.learner.learner.Learner`.
Don't use the ``train_batch_size`` setting any longer:


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
#. the custom Learner class you want to use. See this `example <https://github.com/ray-project/ray/blob/master/rllib/examples/learners/ppo_with_custom_loss_fn.py>`__ for more details.
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


.. hint::
    The main switch for whether to explore or not during sample collection has moved
    to the :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.env_runners` method.
    See :ref:`here for more details <rllib-algo-config-exploration-docs>`.


.. _rllib-algo-config-exploration-docs:

AlgorithmConfig.exploration()
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The main switch for whether to explore or not during sample collection has moved from
the deprecated ``AlgorithmConfig.exploration()`` method
to :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.env_runners`:

It determines whether the method your :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` calls
inside the :py:class:`~ray.rllib.env.env_runner.EnvRunner` is either
:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration`, in the case `explore=True`,
or :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_inference`, in the case `explore=False`.

.. testcode::

    config.env_runners(explore=True)  # <- or False


The Ray team has deprecated the ``exploration_config`` setting. Instead, define the exact exploratory
behavior, for example, sample an action from a distribution, inside the overridden
:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration` method of your
:py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`.


Custom callbacks
----------------

If you're using custom callbacks on the old API stack, you're subclassing the ``DefaultCallbacks`` class,
which the Ray team renamed to :py:class:`~ray.rllib.callbacks.callbacks.RLlibCallback`.
You can continue this approach with the new API stack and pass your custom subclass to your config like the following:

.. testcode::

    # config.callbacks(YourCallbacksClass)

However, if you're overriding those methods that triggered on the :py:class:`~ray.rllib.env.env_runner.EnvRunner`
side, for example, ``on_episode_start/stop/step/etc...``, you may have to translate some call arguments.

The following is a one-to-one translation guide for these types of :py:class:`~ray.rllib.callbacks.callbacks.RLlibCallback`
methods:

.. testcode::

    from ray.rllib.callbacks.callbacks import RLlibCallback

    class YourCallbacksClass(RLlibCallback):

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

* ``on_sub_environment_created()``: The new API stack uses `Farama's gymnasium <https://farama.org>`__ vector Envs leaving no control for RLlib
  to call a callback on each individual env-index's creation.
* ``on_create_policy()``: This method is no longer available on the new API stack because only ``RolloutWorker`` calls it.
* ``on_postprocess_trajectory()``: The new API stack no longer triggers and calls this method
  because :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` pipelines handle trajectory processing entirely.
  The documentation for :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` is under development.

.. See :ref:`<rllib-callback-docs>` for a detailed description of RLlib callback APIs.
    TODO (sven): ref doesn't work for some weird reason. Getting: undefined label: '<rllib-callback-docs>'

.. _rllib-modelv2-to-rlmodule:

ModelV2 to RLModule
-------------------

If you're using a custom ``ModelV2`` class and want to translate
the entire NN architecture and possibly action distribution logic to the new API stack, see
:ref:`RL Modules <rlmodule-guide>` in addition to this section.

Also, see these example scripts on `how to write a custom CNN-containing RL Module <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/custom_cnn_rl_module.py>`__
and `how to write a custom LSTM-containing RL Module <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/custom_lstm_rl_module.py>`__.

There are various options for translating an existing, custom ``ModelV2`` from the old API stack,
to the new API stack's :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`:

#. Move your ModelV2 code to a new, custom `RLModule` class. See :ref:`RL Modules <rlmodule-guide>` for details).
#. Use an Algorithm checkpoint or a Policy checkpoint that you have from an old API stack
   training run and use this checkpoint with the `new stack RL Module convenience wrapper <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/migrate_modelv2_to_new_api_stack_by_policy_checkpoint.py>`__.
#. Use an existing :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`
   object from an old API stack training run, with the `new stack RL Module convenience wrapper <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/migrate_modelv2_to_new_api_stack_by_config.py>`__.

In more complex scenarios, you might've implemented custom policies, such that you could modify the behavior of constructing models
and distributions.


Translating Policy.compute_actions_from_input_dict
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This old API stack method, as well as ``compute_actions`` and ``compute_single_action``, directly translate to
:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_inference`
and :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration`.
:ref:`The RLModule guide explains how to implement this method <rlmodule-guide>`.


Translating Policy.action_distribution_fn
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To translate ``action_distribution_fn``, write the following custom RLModule code:

.. tab-set::

    .. tab-item:: Same action dist. class

        .. testcode::
            :skipif: True

            from ray.rllib.models.torch.torch_distributions import YOUR_DIST_CLASS


            class MyRLModule(TorchRLModule):
                def setup(self):
                    ...
                    # Set the following attribute at the end of your custom `setup()`.
                    self.action_dist_cls = YOUR_DIST_CLASS


    .. tab-item:: Different action dist. classes

        .. testcode::
            :skipif: True

            from ray.rllib.models.torch.torch_distributions import (
                YOUR_INFERENCE_DIST_CLASS,
                YOUR_EXPLORATION_DIST_CLASS,
                YOUR_TRAIN_DIST_CLASS,
            )

                def get_inference_action_dist_cls(self):
                    return YOUR_INFERENCE_DIST_CLASS

                def get_exploration_action_dist_cls(self):
                    return YOUR_EXPLORATION_DIST_CLASS

                def get_train_action_dist_cls(self):
                    return YOUR_TRAIN_DIST_CLASS


Translating Policy.action_sampler_fn
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To translate ``action_sampler_fn``, write the following custom RLModule code:

.. testcode::
    :skipif: True

    from ray.rllib.models.torch.torch_distributions import YOUR_DIST_CLASS


    class MyRLModule(TorchRLModule):

        def _forward_exploration(self, batch):
            computation_results = ...
            my_dist = YOUR_DIST_CLASS(computation_results)
            actions = my_dist.sample()
            return {Columns.ACTIONS: actions}

        # Maybe for inference, you would like to sample from the deterministic version
        # of your distribution:
        def _forward_inference(self, batch):
            computation_results = ...
            my_dist = YOUR_DIST_CLASS(computation_results)
            greedy_actions = my_dist.to_deterministic().sample()
            return {Columns.ACTIONS: greedy_actions}


Policy.compute_log_likelihoods
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Implement your custom RLModule's :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_train` method and
return the ``Columns.ACTION_LOGP`` key together with the corresponding action log probabilities to pass this information
to your loss functions, which your code calls after `forward_train()`. The loss logic can then access
`Columns.ACTION_LOGP`.


Custom loss functions and policies
-------------------------------------

If you're using one or more custom loss functions or custom (PyTorch) optimizers to train your models, instead of doing these
customizations inside the old stack's Policy class, you need to move the logic into the new API stack's
:py:class:`~ray.rllib.core.learner.learner.Learner` class.

See :ref:`Learner <learner-guide>` for details on how to write a custom Learner .

The following example scripts show how to write:
- `a simple custom loss function <https://github.com/ray-project/ray/blob/master/rllib/examples/learners/ppo_with_custom_loss_fn.py>`__
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


Custom connectors
-----------------

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
