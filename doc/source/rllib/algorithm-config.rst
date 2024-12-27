.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-algo-configuration-docs:

AlgorithmConfig API
===================

You can configure your RLlib :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
in a type-safe fashion by working with the :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` API.

In essence, you first create an instance of :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`
and then call some of its methods to set various configuration options.

RLlib uses the following, `black <https://github.com/psf/black>`__ compliant notation
in all parts of the code. Note that you can chain together more than one method call, including
the constructor:

.. testcode::

    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

    config = (
        # Create an `AlgorithmConfig` instance.
        AlgorithmConfig()
        # Change the learning rate.
        .training(lr=0.0005)
        # Change the number of Learner actors.
        .learners(num_learners=2)
    )


.. hint::

    For value checking and type-safety reasons, you should never set attributes in your
    :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`
    directly, but always go through the proper methods:

    .. testcode::

        # WRONG!
        config.env = "CartPole-v1"  # <- don't set attributes directly

        # CORRECT!
        config.environment(env="CartPole-v1")  # call the proper method


Algorithm specific config classes
---------------------------------

Normally, you should pick the specific :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`
subclass that matches the :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
you would like to run your learning experiments with. For example, if you would like to
use ``IMPALA`` as your algorithm, you should import its specific config class:

.. testcode::

    from ray.rllib.algorithms.impala import IMPALAConfig

    config = (
        # Create an `IMPALAConfig` instance.
        IMPALAConfig()
        # Change the learning rate.
        .training(lr=0.0004)
    )

You can build the :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` directly from the
config object through calling the
:py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.build_algo` method:

.. testcode::

    impala = config.build_algo()


The config object stored inside any built :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` instance
is a copy of your original config. Hence, you can further alter your original config object and
build another instance of the algo without affecting the previously built one:


.. testcode::

    # Further alter the config without affecting the previously built IMPALA ...
    config.env_runners(num_env_runners=4)
    # ... and build another algo from it.
    another_impala = config.build_algo()


If you are working with `Ray Tune <https://docs.ray.io/en/latest/tune/index.html>`__,
pass your :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`
instance into the constructor of the :py:class:`~ray.tune.tuner.Tuner`:

.. code-block:: python

    from ray import tune

    tuner = tune.Tuner(
        "IMPALA",
        param_space=config,  # <- your RLlib AlgorithmConfig object
        ..
    )
    # Run the experiment with Ray Tune.
    results = tuner.fit()



Generic config settings
-----------------------

Most config settings are generic and apply to all of RLlib's
:py:class:`~ray.rllib.algorithms.algorithm.Algorithm` classes.


RL Environment
~~~~~~~~~~~~~~



Learning rate `lr`
~~~~~~~~~~~~~~~~~~

Set the learning rate for updating your models through the ``lr`` arg to the
:py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.training` method:

.. testcode::

    config.training(lr=0.0001)


Train batch size
~~~~~~~~~~~~~~~~

Set the train batch size, per Learner actor,
through the ``train_batch_size_per_learner`` arg to the :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.training`
method:

.. testcode::

    config.training(train_batch_size_per_learner=256)


Discount factor `gamma`
~~~~~~~~~~~~~~~~~~~~~~~

Set the `RL discount factor <https://www.envisioning.io/vocab/discount-factor?utm_source=chatgpt.com>`__
through the ``gamma`` arg to the :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.training`
method:

.. testcode::

    config.training(gamma=0.995)



num_learners
num_env_runners
num_envs_per_env_runner

explore

rollout_fragment_length


env
env_config


policies
policy_mapping_fn



Algorithm specific settings
---------------------------

Each RLlib algorithm has its own config class that inherits from `AlgorithmConfig`.
For instance, to create a `PPO` algorithm, you start with a `PPOConfig` object, to work
with a `DQN` algorithm, you start with a `DQNConfig` object, etc.

.. note::

    Each algorithm has its specific settings, but most configuration options are shared.
    We discuss the common options below, and refer to
    :ref:`the RLlib algorithms guide <rllib-algorithms-doc>` for algorithm-specific
    properties.
    Algorithms differ mostly in their `training` settings.

Below you find the basic signature of the `AlgorithmConfig` class, as well as some
advanced usage examples:

.. autoclass:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig
    :noindex:

As RLlib algorithms are fairly complex, they come with many configuration options.
To make things easier, the common properties of algorithms are naturally grouped into
the following categories:

- :ref:`training options <rllib-config-train>`,
- :ref:`environment options <rllib-config-env>`,
- :ref:`deep learning framework options <rllib-config-framework>`,
- :ref:`env runner options <rllib-config-env-runners>`,
- :ref:`evaluation options <rllib-config-evaluation>`,
- :ref:`options for training with offline data <rllib-config-offline_data>`,
- :ref:`options for training multiple agents <rllib-config-multi_agent>`,
- :ref:`reporting options <rllib-config-reporting>`,
- :ref:`options for saving and restoring checkpoints <rllib-config-checkpointing>`,
- :ref:`debugging options <rllib-config-debugging>`,
- :ref:`options for adding callbacks to algorithms <rllib-config-callbacks>`,
- :ref:`Resource options <rllib-config-resources>`
- :ref:`and options for experimental features <rllib-config-experimental>`

Let's discuss each category one by one, starting with training options.
