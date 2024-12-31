.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-callback-docs:

RLlib's callback APIs
=====================

Overview
--------

RLlib's callbacks are the easiest way for you to inject code into your experiments.

In a nutshell, you define the code you want to execute at certain events and pass it to your
:py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`.

Here is an example of defining a simple lambda that prints out an episode's return after
the episode has terminated:

.. testcode::

    from ray.rllib.algorithms.ppo import PPOConfig

    ppo = config = (
        PPOConfig()
        .environment("CartPole-v1")
        .callbacks(
            on_episode_end=(
                lambda episode, **kw: print(f"Episode done. R={episode.get_return()}")
            )
        )
        .build()
    )
    ppo.train()

.. testcode::
    :hide:

    ppo.stop()


Callback lambdas vs stateful RLlibCallback
------------------------------------------
There are two ways to define custom code to be executed at the various callback events.

Callback lambdas
~~~~~~~~~~~~~~~~
If your injected code is rather simple and doesn't require state or storing temporary
information to be reused in succeeding calls triggered by the same event, you can use a lambda
and pass it to the :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.callbacks`
method as previously shown.

See here for a :ref:`complete list of supported callback events <rllib-callback-event-overview>`.
The names of the events always match the argument names for the
:py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.callbacks` method.


Stateful RLlibCallback
~~~~~~~~~~~~~~~~~~~~~~
If you need your code to be stateful and be able to temporarily store results for reuse
in succeeding calls triggered by the same or a different event, you
need to subclass the :py:class:`~ray.rllib.callbacks.callbacks.RLlibCallback` API and then implement
one or more methods, for example :py:meth:`~ray.rllib.callbacks.callbacks.RLlibCallback.on_algorithm_init`:

Here is the same example, printing out a terminated episode's return, but using
a subclass of :py:class:`~ray.rllib.callbacks.callbacks.RLlibCallback`.

.. testcode::

    from ray.rllib.algorithms.ppo import PPOConfig
    from ray.rllib.callbacks.callbacks import RLlibCallback

    class EpisodeReturn(RLlibCallback):
        def on_episode_end(self, *, episode, **kwargs):
            print(f"Episode done. R={episode.get_return()}")

    ppo = (
        PPOConfig()
        .environment("CartPole-v1")
        .callbacks(EpisodeReturn)
        .build()
    )
    ppo.train()

.. testcode::
    :hide:

    ppo.stop()


.. _rllib-callback-event-overview:

Overview of all callback events
-------------------------------

During a training iteration, the Algorithm normally walks through the following event tree. Note
that some of the events in the tree happen simultaneously, on different processes through Ray actors.
For example an EnvRunner actor may trigger its ``on_episode_start`` event while at the same time another
EnvRunner actor may trigger its ``on_sample_end`` event and the main Algorithm process triggers
``on_train_result``.

.. note::

    Currently, RLlib only invokes callbacks in :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
    and :py:class:`~ray.rllib.env.env_runner.EnvRunner` actors.
    The Ray team is considering expanding callbacks onto :py:class:`~ray.rllib.core.learner.learner.Learner`
    actors and possibly :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` instances as well.


Here is a high-level overview of all supported events in RLlib's callbacks system.

.. code-block:: text

    Algorithm
        .__init__()
            `on_algorithm_init` - After algorithm construction and setup.
        .train()
            `on_train_result` - After a training iteration.
        .evaluate()
            `on_evaluate_start` - Before evaluation starts using the eval ``EnvRunnerGroup``.
            `on_evaluate_end` - After evaluation is finished.
        .restore_from_path()
            `on_checkpoint_loaded` - After a checkpoint's new state has been loaded.

    EnvRunner
        .__init__()
            `on_environment_created` - After the RL environment has been created.
        .sample()
            `on_episode_created` - After a new episode object has been created.
            `on_episode_start` - After an episode object has started (after ``env.reset()``).
            `on_episode_step` - After an episode object has stepped (after ``env.step()``).
            `on_episode_end` - After an episode object has terminated (or truncated).
            `on_sample_end` - At the end of the ``EnvRunner.sample()`` call.


.. currentmodule:: ray.rllib.callbacks.callbacks

.. dropdown:: Click here to see all Algorithm-bound methods of ``RLlibCallback``
    .. autosummary::
        :nosignatures:
        :toctree: doc/

        ~RLlibCallback.on_algorithm_init
        ~RLlibCallback.on_evaluate_start
        ~RLlibCallback.on_evaluate_end
        ~RLlibCallback.on_env_runners_recreated
        ~RLlibCallback.on_checkpoint_loaded

.. dropdown:: Click here to see all EnvRunner-bound methods of ``RLlibCallback``
    .. autosummary::
        :nosignatures:
        :toctree: doc/

        ~RLlibCallback.on_environment_created
        ~RLlibCallback.on_episode_created
        ~RLlibCallback.on_episode_start
        ~RLlibCallback.on_episode_step
        ~RLlibCallback.on_episode_end
        ~RLlibCallback.on_sample_end


Chaining callbacks
------------------

You can define more than one :py:class:`~ray.rllib.callbacks.callbacks.RLlibCallback` class and send them in a list to the
:py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.callbacks` method.
You can also send lists of callables, instead of a single callable, to the different
arguments of that method.

For example, assume you already have a subclass of :py:class:`~ray.rllib.callbacks.callbacks.RLlibCallback`
written and would like to reuse it in different experiments. However, one of your experiments
requires some debug callback code you would like to inject only temporarily for a couple of runs.

Resolution order of chained callbacks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RLlib resolves all available callback methods and callables for a given event
as follows:

Subclasses of :py:class:`~ray.rllib.callbacks.callbacks.RLlibCallback` take precedence
over individual or lists of callables provided through the various arguments of
the :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.callbacks` method.

For example, assume the callback event is ``on_train_result``, which fires at the end of
a training iteration and inside the algorithm's process.

- RLlib loops through the list of all given :py:class:`~ray.rllib.callbacks.callbacks.RLlibCallback`
  subclasses and calls their ``on_train_result`` method. Thereby, it keeps the exact order the user
  provided in the list.
- RLlib then loops through the list of all defined ``on_train_result`` callables. The user configured these
  by calling the :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.callbacks` method
  and defining the ``on_train_result`` argument in this call.

.. code-block:: python

    class MyCallbacks(RLlibCallback):
        def on_train_result(self, *, algorithm, metrics_logger, result, **kwargs):
            print("RLlibCallback subclass")

    class MyDebugCallbacks(RLlibCallback):
        def on_train_result(self, *, algorithm, metrics_logger, result, **kwargs):
            print("debug subclass")

    # Define the callbacks order through the config.
    # Subclasses first, then individual `on_train_result` (or other events) callables:
    config.callbacks(
        callbacks_class=[MyDebugCallbacks, MyCallbacks],  # <- note: debug class first
        on_train_result=[
            lambda algorithm, **kw: print('lambda 1'),
            lambda algorithm, **kw: print('lambda 2'),
        ],
    )

    # When training the algorithm, after each training iteration, you should see
    # something like:
    # > debug subclass
    # > RLlibCallback subclass
    # > lambda 1
    # > lambda 2


Examples
--------

Here are two examples showing you how to setup custom callbacks on the
Algorithm process as well as on the EnvRunner processes.

Both examples utilize the :py:class:`~ray.rllib.utils.metrics.metrics_logger.MetricsLogger`
API to log the custom computations happening in the injected code to the main
metrics system.


.. todo: uncomment this once metrics-logger.rst page is online.
   Read :ref:`more about the MetricsLogger API here <>`__ or also take a look at this
   more complex example on
   `how to generate and log a PacMan heatmap (image) to WandB <https://github.com/ray-project/ray/blob/master/rllib/examples/metrics/custom_metrics_in_env_runners.py>`__.


Example 1: on_train_result
--------------------------

The following example demonstrates how to implement a simple custom function writing the replay buffer
contents to disk from time to time.

You normally don't want the contents of buffers to be written with your
:ref:`Algorithm checkpoints <rllib-checkpointing-docs>`, so doing this in a more
controlled fashion through a custom callback would be a good compromise.

.. testcode::

    import ormsgpack
    from ray.rllib.algorithms.dqn import DQNConfig

    def _write_buffer_if_necessary(algorithm, metrics_logger, result):
        # Write the buffer contents only every ith iteration.
        if algorithm.training_iteration % 2 == 0:
            # python dict
            buffer_contents = algorithm.local_replay_buffer.get_state()

            # binary
            msgpacked = ormsgpack.packb(
               buffer_contents,
               option=ormsgpack.OPT_SERIALIZE_NUMPY,
            )

            # Open some file and write the buffer contents into it using `ormsgpack`.
            with open("replay_buffer_contents.msgpack", "wb") as f:
               f.write(msgpacked)

    config = (
        DQNConfig()
        .environment("CartPole-v1")
        .callbacks(
           on_train_result=_write_buffer_if_necessary,
        )
    )
    dqn = config.build()

    # Train n times. Expect buffer to be written every ith iteration.
    for _ in range(4):
        print(dqn.train())

.. tip::
   See :ref:`here for the exact call signatures and expected argument types <rllib-callback-reference-algo-bound>`
   of all available callbacks.


Example 2: on_episode_end
-------------------------

The following example demonstrates how to implement custom RLlibCallback class computing
the average first-joint angle, ``theta1`` of the `Acrobot-v1 RL environment <https://github.com/Farama-Foundation/Gymnasium/blob/main/gymnasium/envs/classic_control/acrobot.py>`__.

It is decribed in the original environment's code as:

.. code-block:: text
    `theta1` is the angle of the first joint, where an angle of 0 indicates the first
    link is pointing directly downwards.




You normally don't want the contents of buffers to be written with your
:ref:`Algorithm checkpoints <rllib-checkpointing-docs>`, so doing this in a more
controlled fashion through a custom callback would be a good compromise.

.. testcode::



.. tip::
    You can base your custom logic on whether the calling EnvRunner is a regular "training"
    EnvRunner, used to collect training samples, or an "eval" EnvRunner, used to play
    through episodes for evaluation only.
    Access the ``env_runner.config.in_evaluation`` boolean flag, which is True on
    eval EnvRunner actors and False on training ones.


.. tip::
   See :ref:`here for the exact call signatures and expected argument types <rllib-callback-reference-algo-bound>`
   of all available callbacks.

