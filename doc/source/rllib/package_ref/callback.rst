.. include:: /_includes/rllib/we_are_hiring.rst

.. _rllib-callback-reference-docs:

Callback APIs
=============

.. include:: /_includes/rllib/new_api_stack.rst

Callback APIs enable you to inject code into an experiment, an Algorithm,
and the subcomponents of an Algorithm.

You can either subclass :py:class:`~ray.rllib.callbacks.callbacks.RLlibCallback` and implement
one or more of its methods, like :py:meth:`~ray.rllib.callbacks.callbacks.RLlibCallback.on_algorithm_init`,
or pass respective arguments to the :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.callbacks`
method of an Algorithm's config, like
``config.callbacks(on_algorithm_init=lambda algorithm, **kw: print('algo initialized!'))``.

.. tab-set::

    .. tab-item:: Subclass RLlibCallback

        .. testcode::

            from ray.rllib.algorithms.dqn import DQNConfig
            from ray.rllib.callbacks.callbacks import RLlibCallback

            class MyCallback(RLlibCallback):
                def on_algorithm_init(self, *, algorithm, metrics_logger, **kwargs):
                    print(f"Algorithm {algorithm} has been initialized!")

            config = (
                DQNConfig()
                .callbacks(MyCallback)
            )

        .. testcode::
            :hide:

            config.validate()

    .. tab-item:: Pass individual callables to ``config.callbacks()``

        .. testcode::

            from ray.rllib.algorithms.dqn import DQNConfig

            config = (
                DQNConfig()
                .callbacks(
                    on_algorithm_init=(
                        lambda algorithm, **kwargs: print(f"Algorithm {algorithm} has been initialized!")
                    )
                )
            )

        .. testcode::
            :hide:

            config.validate()


See :ref:`Callbacks <rllib-callback-docs>` for more details on how to write and configure callbacks.


Methods to implement for custom behavior
----------------------------------------

.. note::

    RLlib only invokes callbacks in :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
    and :py:class:`~ray.rllib.env.env_runner.EnvRunner` actors.
    The Ray team is considering expanding callbacks onto :py:class:`~ray.rllib.core.learner.learner.Learner`
    actors and possibly :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` instances as well.


.. currentmodule:: ray.rllib.callbacks.callbacks

RLlibCallback
-------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~RLlibCallback


.. _rllib-callback-reference-algorithm-bound:

Callbacks invoked in Algorithm
------------------------------

The main Algorithm process always executes the following callback methods:

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~RLlibCallback.on_algorithm_init
    ~RLlibCallback.on_sample_end
    ~RLlibCallback.on_train_result
    ~RLlibCallback.on_evaluate_start
    ~RLlibCallback.on_evaluate_end
    ~RLlibCallback.on_env_runners_recreated
    ~RLlibCallback.on_checkpoint_loaded


.. _rllib-callback-reference-env-runner-bound:

Callbacks invoked in EnvRunner
------------------------------

The EnvRunner actors always execute the following callback methods:

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~RLlibCallback.on_environment_created
    ~RLlibCallback.on_episode_created
    ~RLlibCallback.on_episode_start
    ~RLlibCallback.on_episode_step
    ~RLlibCallback.on_episode_end
