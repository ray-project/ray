.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rlmodule-guide:

RL Modules
==========

The :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` class in RLlib's new API stack allows you to write custom
models, including highly complex multi-network setups often found in multi-agent or model-based algorithms.

:py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` is the main neural network class and exposes
three public methods, each corresponding to a distinct phase in the reinforcement learning cycle:
- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_exploration` handles the computation of actions during data collection
if RLlib uses the data for a succeeding training step, balancing exploration and exploitation.
- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_inference` computes actions for evaluation and production, which often need to be greedy or less stochastic.
- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_train` manages the training phase, performing calculations required to
compute losses, such as Q-values in a DQN model, value function predictions in a PG-style setup,
or world-model predictions in model-based algorithms.


.. figure:: images/rl_modules/rl_module_overview.svg
    :width: 700
    :align: left

    **RLModule overview**: (*left*) A plain :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` contains the
    neural network RLlib uses for computations,
    for example, a policy network written in `PyTorch <pytorch.org>`__, and exposes the three forward methods:
    :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_exploration` for sample collection,
    :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_inference` for production/deployment, and
    :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_train` for computing loss function inputs when training.
    (*right*) A :py:class:`~ray.rllib.core.rl_module.multi_rl_module.MultiRLModule` may contain one
    or more sub-RLModules, each identified by a `ModuleID`, allowing you to implement
    arbitrarily complex multi-network or multi-agent architectures and algorithms.


Enabling the RLModule API in the AlgorithmConfig
------------------------------------------------

In the :ref:`new API stack <rllib-new-api-stack-guide>`, activated by default, RLlib exclusively uses RLModules.

If you're working with a legacy config or want to migrate ``ModelV2`` or ``Policy`` classes to the
new API stack, see the :ref:`new API stack migration guide <rllib-new-api-stack-migration-guide>` for more information.

If your config is set to the old API stack, use the
:py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.api_stack` method to switch:

.. testcode::

    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

    config = (
        AlgorithmConfig()
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
    )


.. _rllib-default-rl-modules-docs:

Default RLModules
-----------------

If you don't specify module-related settings in the
:py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`, RLlib uses the respective algorithm's default
RLModule, which is an appropriate choice for initial experimentation and benchmarking. All of the default RLModules support 1D-tensor and
image observations (``[width] x [height] x [channels]``).

.. note::
    For discrete or more complex input observation spaces like dictionaries, use the
    :py:class:`~ray.rllib.connectors.env_to_module.flatten_observations.FlattenObservations` connector
    piece as follows:

    .. testcode::

        from ray.rllib.algorithms.ppo import PPOConfig
        from ray.rllib.connectors.env_to_module import FlattenObservations

        config = (
            PPOConfig()
            # FrozenLake has a discrete observation space, ...
            .environment("FrozenLake-v1")
            # ... which `FlattenObservations` converts to one-hot.
            .env_runners(env_to_module_connector=lambda env: FlattenObservations())
        )

    .. TODO (sven): Link here to the connector V2 page and preprocessors once that page is done.

Furthermore, all default models offer configurable architecture choices with respect to the number
and size of the layers used (``Dense`` or ``Conv2D``), their activations and initializations, and automatic LSTM-wrapping behavior.

Use the :py:class:`~ray.rllib.core.rl_module.default_model_config.DefaultModelConfig` datadict class to configure
any default model in RLlib. Note that you should only use this class for configuring default models.
When writing your own custom RLModules, use plain python dicts to define the model configurations.
For how to write and configure your custom RLModules, see :ref:`Implementing custom RLModules <rllib-implementing-custom-rl-modules>`.


Configuring default MLP nets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To train a simple MLP policy like the "multi layer perceptron", which only contains dense layers,
with PPO and the default RLModule, configure your experiment as follows:

.. testcode::

    from ray.rllib.algorithms.ppo import PPOConfig
    from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig

    config = (
        PPOConfig()
        .environment("CartPole-v1")
        .rl_module(
            # Use a non-default 32,32-stack with ReLU activations.
            model_config=DefaultModelConfig(
                fcnet_hiddens=[32, 32],
                fcnet_activation="relu",
            )
        )
    )

The following is the compete list of all supported ``fcnet_..`` options:

.. literalinclude:: ../../../rllib/core/rl_module/default_model_config.py
        :language: python
        :start-after: __sphinx_doc_default_model_config_fcnet_begin__
        :end-before: __sphinx_doc_default_model_config_fcnet_end__


Configuring default CNN nets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For image-based environments like `Atari <https://ale.farama.org/environments/>`__, use the
``conv_..`` fields in :py:class:`~ray.rllib.core.rl_module.default_model_config.DefaultModelConfig` to configure
the CNN ("convolutional neural network") stack.

For example:

.. testcode::

    from ray.rllib.algorithms.ppo import PPOConfig
    from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig

    config = (
        PPOConfig()
        .environment("ale_py:ALE/Pong-v5")  # `pip install gymnasium[atari]`
        .rl_module(
            model_config=DefaultModelConfig(
                # Use a DreamerV3-style CNN stack.
                conv_filters=[
                    [16, 4, 2],  # 1st CNN layer: num_filters, kernel, stride(, padding)?
                    [32, 4, 2],  # 2nd CNN layer
                    [64, 4, 2],  # etc..
                    [128, 4, 2],
                ],
                conv_activation="silu",

                # After the last CNN, the default model flattens, then adds an optional MLP.
                head_fcnet_hiddens=[256],
            )
        )
    )

The following is the compete list of all supported ``conv_..`` options:

.. literalinclude:: ../../../rllib/core/rl_module/default_model_config.py
        :language: python
        :start-after: __sphinx_doc_default_model_config_conv_begin__
        :end-before: __sphinx_doc_default_model_config_conv_end__


Other default model settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For LSTM-based configurations and specific settings for continuous action output layers,
see :py:class:`~ray.rllib.core.rl_module.default_model_config.DefaultModelConfig`.


Constructing RLModule instances
-------------------------------

To maintain consistency and usability, RLlib offers a standardized approach for constructing
:py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` instances for both single-module and multi-module use cases. An example of a single-module use case is a single-agent experiment. Examples of multi-module use cases are
multi-agent learning or other multi-NN setups.


.. _rllib-constructing-rlmodule-w-class-constructor:

Construction through the class constructor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The most direct way to construct your :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` is through its constructor:

.. testcode::

    import gymnasium as gym
    from ray.rllib.algorithms.bc.torch.default_bc_torch_rl_module import DefaultBCTorchRLModule

    # Create an env object to know the spaces.
    env = gym.make("CartPole-v1")

    # Construct the actual RLModule object.
    rl_module = DefaultBCTorchRLModule(
        observation_space=env.observation_space,
        action_space=env.action_space,
        # A custom dict that's accessible inside your class as `self.model_config`.
        model_config={"fcnet_hiddens": [64]},
    )


.. note::
    If you have a checkpoint of an `py:class:`~ray.rllib.algorithms.algorithm.Algorithm` or an individual
    :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`,
    see :ref:`creating instances with from_checkpoint <rllib-checkpointing-from-checkpoint>` for how to recreate your
    :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` from disk.


Construction through RLModuleSpecs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Because RLlib is a distributed RL library and needs to create more than one copy of
your :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`, you can use
:py:class:`~ray.rllib.core.rl_module.rl_module.RLModuleSpec` objects to define how RLlib should construct
each copy during the algorithm's setup process. The algorithm passes the spec to all
subcomponents that need to have a copy of your RLModule.

Creating an :py:class:`~ray.rllib.core.rl_module.rl_module.RLModuleSpec` is straightforward
and analogous to the :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` constructor:

.. tab-set::

    .. tab-item:: RLModuleSpec (single model)

        .. testcode::

            import gymnasium as gym
            from ray.rllib.algorithms.bc.torch.default_bc_torch_rl_module import DefaultBCTorchRLModule
            from ray.rllib.core.rl_module.rl_module import RLModuleSpec

            # Create an env object to know the spaces.
            env = gym.make("CartPole-v1")

            # First construct the spec.
            spec = RLModuleSpec(
                module_class=DefaultBCTorchRLModule,
                observation_space=env.observation_space,
                action_space=env.action_space,
                # A custom dict that's accessible inside your class as `self.model_config`.
                model_config={"fcnet_hiddens": [64]},
            )

            # Then, build the RLModule through the spec's `build()` method.
            rl_module = spec.build()

    .. tab-item:: MultiRLModuleSpec (multi model)

        .. testcode::

            import gymnasium as gym
            from ray.rllib.algorithms.bc.torch.default_bc_torch_rl_module import DefaultBCTorchRLModule
            from ray.rllib.core.rl_module.rl_module import RLModuleSpec
            from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec

            # First construct the MultiRLModuleSpec.
            spec = MultiRLModuleSpec(
                rl_module_specs={
                    "module_1": RLModuleSpec(
                        module_class=DefaultBCTorchRLModule,

                        # Define the spaces for only this sub-module.
                        observation_space=gym.spaces.Box(low=-1, high=1, shape=(10,)),
                        action_space=gym.spaces.Discrete(2),

                        # A custom dict that's accessible inside your class as
                        # `self.model_config`.
                        model_config={"fcnet_hiddens": [32]},
                    ),
                    "module_2": RLModuleSpec(
                        module_class=DefaultBCTorchRLModule,

                        # Define the spaces for only this sub-module.
                        observation_space=gym.spaces.Box(low=-1, high=1, shape=(5,)),
                        action_space=gym.spaces.Discrete(2),

                        # A custom dict that's accessible inside your class as
                        # `self.model_config`.
                        model_config={"fcnet_hiddens": [16]},
                    ),
                },
            )

            # Construct the actual MultiRLModule instance with .build():
            multi_rl_module = spec.build()


You can pass the :py:class:`~ray.rllib.core.rl_module.rl_module.RLModuleSpec` instances to your
:py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` to
tell RLlib to use the particular module class and constructor arguments:

.. tab-set::

    .. tab-item:: Single-Module (like single-agent)

        .. code-block:: python

            from ray.rllib.algorithms.ppo import PPOConfig
            from ray.rllib.core.rl_module.rl_module import RLModuleSpec

            config = (
                PPOConfig()
                .environment("CartPole-v1")
                .rl_module(
                    rl_module_spec=RLModuleSpec(
                        module_class=MyRLModuleClass,
                        model_config={"some_key": "some_setting"},
                    ),
                )
            )
            ppo = config.build()
            print(ppo.get_module())

        .. note::
            Often when creating an `RLModuleSpec`, you don't have to define attributes like `observation_space` or `action_space`
            because RLlib automatically infers these attributes based on the used
            environment or other configuration parameters.

    .. tab-item:: Multi-Agent (shared policy net)

        .. code-block:: python

            from ray.rllib.algorithms.ppo import PPOConfig
            from ray.rllib.core.rl_module.rl_module import RLModuleSpec
            from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
            from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole

            config = (
                PPOConfig()
                .environment(MultiAgentCartPole, env_config={"num_agents": 2})
                .rl_module(
                    rl_module_spec=MultiRLModuleSpec(
                        # All agents (0 and 1) use the same (single) RLModule.
                        rl_module_specs=RLModuleSpec(
                            module_class=MyRLModuleClass,
                            model_config={"some_key": "some_setting"},
                        )
                    ),
                )
            )
            ppo = config.build()
            print(ppo.get_module())

    .. tab-item:: Multi-Agent (two or more policy nets)

        .. code-block:: python

            from ray.rllib.algorithms.ppo import PPOConfig
            from ray.rllib.core.rl_module.rl_module import RLModuleSpec
            from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
            from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole

            config = (
                PPOConfig()
                .environment(MultiAgentCartPole, env_config={"num_agents": 2})
                .multi_agent(
                    policies={"p0", "p1"},
                    # Agent IDs of `MultiAgentCartPole` are 0 and 1, mapping to
                    # "p0" and "p1", respectively.
                    policy_mapping_fn=lambda agent_id, episode, **kw: f"p{agent_id}"
                )
                .rl_module(
                    rl_module_spec=MultiRLModuleSpec(
                        # Agents (0 and 1) use different (single) RLModules.
                        rl_module_specs={
                            "p0": RLModuleSpec(
                                module_class=MyRLModuleClass,
                                # Small network.
                                model_config={"fcnet_hiddens": [32, 32]},
                            ),
                            "p1": RLModuleSpec(
                                module_class=MyRLModuleClass,
                                # Large network.
                                model_config={"fcnet_hiddens": [128, 128]},
                            ),
                        },
                    ),
                )
            )
            ppo = config.build()
            print(ppo.get_module())


.. _rllib-implementing-custom-rl-modules:

Implementing custom RLModules
-----------------------------

To implement your own neural network architecture and computation logic, subclass
:py:class:`~ray.rllib.core.rl_module.torch_rl_module.TorchRLModule` for any single-agent learning experiment
or for independent multi-agent learning.

For more advanced multi-agent use cases like ones with shared communication between agents,
or any multi-model use cases, subclass the :py:class:`~ray.rllib.core.rl_module.multi_rl_module.MultiRLModule` class, instead.


.. note::
    An alternative to subclassing :py:class:`~ray.rllib.core.rl_module.torch_rl_module.TorchRLModule` is to
    directly subclass your Algorithm's default RLModule. For example, to use PPO, you can subclass
    :py:class:`~ray.rllib.algorithms.ppo.torch.default_ppo_torch_rl_module.DefaultPPOTorchRLModule`.
    You should carefully study the existing default model in this case to understand how to override
    the :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.setup`, the
    `_forward_()` methods, and possibly some algo-specific API methods.
    See :ref:`Algorithm-specific RLModule APIs <rllib-algo-specific-rl-module-apis-docs>` for how to determine which APIs your algorithm requires you to implement.


The setup() method
~~~~~~~~~~~~~~~~~~

You should first implement the :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.setup` method,
in which you add needed NN subcomponents and assign these to class attributes of your choice.

Note that you should call `super().setup()` in your implementation.

You also have access to the following attributes anywhere in the class, including in :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.setup`:

#. ``self.observation_space``
#. ``self.action_space``
#. ``self.inference_only``
#. ``self.model_config`` (a dict with any custom config settings)


.. testcode::

    import torch
    from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule

    class MyTorchPolicy(TorchRLModule):

        def setup(self):
            # You have access here to the following already set attributes:
            # self.observation_space
            # self.action_space
            # self.inference_only
            # self.model_config  # <- a dict with custom settings

            # Use the observation space (if a Box) to infer the input dimension.
            input_dim = self.observation_space.shape[0]

            # Use the model_config dict to extract the hidden dimension.
            hidden_dim = self.model_config["fcnet_hiddens"][0]

            # Use the action space to infer the number of output nodes.
            output_dim = self.action_space.n

            # Build all the layers and subcomponents here you need for the
            # RLModule's forward passes.
            self._pi_head = torch.nn.Sequential(
                torch.nn.Linear(input_dim, hidden_dim),
                torch.nn.ReLU(),
                torch.nn.Linear(hidden_dim, output_dim),
            )


Forward methods
~~~~~~~~~~~~~~~~~~~

Implementing the forward computation logic, you can either define a generic forward behavior by overriding the
private :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward` method, which RLlib then uses everywhere in the model's lifecycle,
or, if you require more granularity, define the following three private methods:

- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration`: Forward pass for computing exploration actions for collecting training data.
- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_inference`: Forward pass for action inference, like greedy.
- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_train`: Forward pass for computing loss function inputs for a training update.

For custom :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward`,
:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_inference`, and
:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration` methods, you must return a
dictionary that contains the key ``actions`` and/or the key ``action_dist_inputs``.

If you return the ``actions`` key from your forward method:

- RLlib uses the provided actions as-is.
- In case you also return the ``action_dist_inputs`` key, RLlib creates a :py:class:`~ray.rllib.models.distributions.Distribution`
  instance from the parameters under that key. In the case of :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_exploration`, RLlib also creates
  compute action probabilities and log probabilities for the given actions automatically.
  See :ref:`Custom action distributions <rllib-rl-module-w-custom-action-dists>` for more information on custom action distribution classes.

If you don't return the ``actions`` key from your forward method:

- You must return the ``action_dist_inputs`` key from your :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration`
  and :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_inference` methods.
- RLlib creates a :py:class:`~ray.rllib.models.distributions.Distribution` instance from the parameters under that key and
  sample actions from that distribution. See :ref:`here for more information on custom action distribution classes <rllib-rl-module-w-custom-action-dists>`.
- For :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration`, RLlib also computes action probability and
  log probability values from the sampled actions automatically.

.. note::

    In case of :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_inference`,
    RLlib always makes the generated distributions from returned key ``action_dist_inputs`` deterministic first through
    the :py:meth:`~ray.rllib.models.distributions.Distribution.to_deterministic` utility before a possible action sample step.
    For example, RLlib reduces the sampling from a Categorical distribution to selecting the argmax actions from the distribution's logits or probs.
    If you return the "actions" key, RLlib skips that sampling step.


.. tab-set::

    .. tab-item:: Returning "actions" key

        .. code-block:: python

            from ray.rllib.core import Columns, TorchRLModule

            class MyTorchPolicy(TorchRLModule):
                ...

                def _forward_inference(self, batch):
                    ...
                    return {
                        Columns.ACTIONS: ...  # RLlib uses these actions as-is
                    }

                def _forward_exploration(self, batch):
                    ...
                    return {
                        Columns.ACTIONS: ...  # RLlib uses these actions as-is (no sampling step!)
                        Columns.ACTION_DIST_INPUTS: ...  # If provided, RLlib uses these dist inputs to compute probs and logp.
                    }

    .. tab-item:: Not returning "actions" key

        .. code-block:: python

            from ray.rllib.core import Columns, TorchRLModule

            class MyTorchPolicy(TorchRLModule):
                ...

                def _forward_inference(self, batch):
                    ...
                    return {
                        # RLlib:
                        # - Generates distribution from ACTION_DIST_INPUTS parameters.
                        # - Converts distribution to a deterministic equivalent.
                        # - Samples from the deterministic distribution.
                        Columns.ACTION_DIST_INPUTS: ...
                    }

                def _forward_exploration(self, batch):
                    ...
                    return {
                        # RLlib:
                        # - Generates distribution from ACTION_DIST_INPUTS parameters.
                        # - Samples from the stochastic distribution.
                        # - Computes action probs and logs automatically using the sampled
                        #   actions and the distribution.
                        Columns.ACTION_DIST_INPUTS: ...
                    }


Never override the constructor (`__init__`), however, note that the
:py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` class's constructor requires the following arguments
and also receives these properly when you call a spec's `build()` method:

- :py:attr:`~ray.rllib.core.rl_module.rl_module.RLModule.observation_space`: The observation space after having passed all connectors; this observation space is the actual input space for the model after all preprocessing steps.
- :py:attr:`~ray.rllib.core.rl_module.rl_module.RLModule.action_space`: The action space of the environment.
- :py:attr:`~ray.rllib.core.rl_module.rl_module.RLModule.inference_only`: Whether RLlib should build the RLModule in inference-only mode, dropping subcomponents that it only needs for learning.
- :py:attr:`~ray.rllib.core.rl_module.rl_module.RLModule.model_config`: The model config, which is either a custom dictionary for custom RLModules or a :py:class:`~ray.rllib.core.rl_module.default_model_config.DefaultModelConfig` dataclass object, which is only for RLlib's default models. Define model hyper-parameters such as number of layers, type of activation, etc. in this object.

See :ref:`Construction through the class constructor <rllib-constructing-rlmodule-w-class-constructor>` for more details on how to create an RLModule through the constructor.


.. _rllib-algo-specific-rl-module-apis-docs:

Algorithm-specific RLModule APIs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The algorithm that you choose to use with your RLModule affects to some
extent the structure of the final custom module.
Each Algorithm class has a fixed set of APIs that all RLModules trained
by that algorithm, need to implement.

To find out, what APIs your Algorithms require, do the following:

.. testcode::

    # Import the config of the algorithm of your choice.
    from ray.rllib.algorithms.sac import SACConfig

    # Print out the abstract APIs, you need to subclass from and whose
    # abstract methods you need to implement, besides the `setup()` and `_forward_..()`
    # methods.
    print(
        SACConfig()
        .get_default_learner_class()
        .rl_module_required_apis()
    )


.. note::
    You don't need the preceding VPG example module to implement any APIs because
    you haven't considered training it with any particular algorithm.
    You can find examples of algorithm-ready :py:class:`~ray.rllib.algorithms.ppo.PPO` custom RLModules
    in the `tiny_atari_cnn_rlm example <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/classes/tiny_atari_cnn_rlm.py>`__
    and in the `lstm_containing_rlm example <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/classes/lstm_containing_rlm.py>`__.


End-to-end example
~~~~~~~~~~~~~~~~~~~~~~~

Putting together the elements of your custom :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` that you implemented,
a working end-to-end example is as follows:

.. literalinclude:: ../../../rllib/examples/rl_modules/classes/vpg_rlm.py
        :language: python


.. _rllib-rl-module-w-custom-action-dists:

Custom action distributions
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The preceding examples rely on :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` using the correct action distribution with the computed
``ACTION_DIST_INPUTS`` returned by the forward methods. RLlib picks a default distribution class based on
the action space, which is :py:class:`~ray.rllib.models.torch.torch_distributions.TorchCategorical` for ``Discrete`` action spaces
and :py:class:`~ray.rllib.models.torch.torch_distributions.TorchDiagGaussian` for ``Box`` action spaces.

To use a different distribution class and return parameters for its constructor from your forward methods,
override the following methods in the :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` implementation:

- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.get_inference_action_dist_cls`
- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.get_exploration_action_dist_cls`
- and :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.get_train_action_dist_cls`

.. note::
    If you only return ``ACTION_DIST_INPUTS`` from your forward methods, RLlib automatically
    uses the :py:meth:`~ray.rllib.models.distributions.Distribution.to_deterministic` method of the
    distribution returned by your :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.get_inference_action_dist_cls`.

See `torch_distributions.py <https://github.com/ray-project/ray/blob/master/rllib/models/torch/torch_distributions.py>`__ for common distribution implementations.


Implementing custom MultiRLModules
----------------------------------

For multi-module setups, RLlib provides the :py:class:`~ray.rllib.core.rl_module.multi_rl_module.MultiRLModule` class,
whose default implementation is a dictionary of individual :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` objects.
one for each submodule and identified by a ``ModuleID``.

The base-class :py:class:`~ray.rllib.core.rl_module.multi_rl_module.MultiRLModule` implementation works for most of the
use cases that need to define independent neural networks. However, for any complex, multi-network or multi-agent use case, where agents share one or more neural networks,
you should inherit from this class and override the default implementation.

The following code snippets create a custom multi-agent RL module with two simple "policy head" modules, which
share the same encoder, the third network in the MultiRLModule. The encoder receives the raw observations from the env
and outputs embedding vectors that then serve as input for the two policy heads to compute the agents' actions.


.. tab-set::

    .. tab-item:: MultiRLModule (w/ two policy nets and one encoder)

        .. literalinclude:: ../../../rllib/examples/rl_modules/classes/vpg_using_shared_encoder_rlm.py
            :language: python
            :start-after: __sphinx_doc_mrlm_begin__
            :end-before: __sphinx_doc_mrlm_end__

        .. literalinclude:: ../../../rllib/examples/rl_modules/classes/vpg_using_shared_encoder_rlm.py
            :language: python
            :start-after: __sphinx_doc_mrlm_2_begin__
            :end-before: __sphinx_doc_mrlm_2_end__

    .. tab-item:: Policy RLModule

        Within the MultiRLModule, you need to have two policy sub-RLModules. They may be of the same
        class, which you can implement as follows:

        .. literalinclude:: ../../../rllib/examples/rl_modules/classes/vpg_using_shared_encoder_rlm.py
            :language: python
            :start-after: __sphinx_doc_policy_begin__
            :end-before: __sphinx_doc_policy_end__
        .. literalinclude:: ../../../rllib/examples/rl_modules/classes/vpg_using_shared_encoder_rlm.py
            :language: python
            :start-after: __sphinx_doc_policy_2_begin__
            :end-before: __sphinx_doc_policy_2_end__

    .. tab-item:: Shared encoder RLModule

        Finally, the shared encoder RLModule should look similar to this:

        .. literalinclude:: ../../../rllib/examples/rl_modules/classes/vpg_using_shared_encoder_rlm.py
            :language: python
            :start-after: __sphinx_doc_encoder_begin__
            :end-before: __sphinx_doc_encoder_end__


To plug in the custom MultiRLModule (from the first tab) into your algorithm's config, create a
:py:class:`~ray.rllib.core.rl_module.multi_rl_module.MultiRLModuleSpec` with the new class and its constructor
settings. Also, create one :py:class:`~ray.rllib.core.rl_module.rl_module.RLModuleSpec` for each agent
and the shared encoder RLModule, because RLlib requires their observation- and action spaces and their
model hyper-parameters:

.. literalinclude:: ../../../rllib/examples/rl_modules/classes/vpg_using_shared_encoder_rlm.py
    :language: python
    :start-after: __sphinx_doc_how_to_run_begin__
    :end-before: __sphinx_doc_how_to_run_end__


.. note::
    In order to properly learn with the preceding setup, you should write and use a specific multi-agent
    :py:class:`~ray.rllib.core.learner.learner.Learner`, capable of handling the shared encoder.
    This Learner should only have a single optimizer updating all three submodules, which are the encoder and the two policy nets,
    to stabilize learning.
    When using the standard "one-optimizer-per-module" Learners, however, the two optimizers for policy 1 and 2
    would take turns updating the same shared encoder, which would lead to learning instabilities.


.. _rllib-checkpointing-rl-modules-docs:

Checkpointing RLModules
-----------------------

You can checkpoint :py:class:`~ray.rllib.core.rl_module.rl_module.RLModules` instances with their
:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.save_to_path` method.
If you already have an instantiated RLModule and would like to load new model weights into it from an existing
checkpoint, use the :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.restore_from_path` method.

The following examples show how you can use these methods outside of, or in conjunction with, an RLlib Algorithm.

Creating an RLModule checkpoint
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. testcode::

    import tempfile

    import gymnasium as gym

    from ray.rllib.algorithms.ppo.torch.default_ppo_torch_rl_module import DefaultPPOTorchRLModule
    from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig

    env = gym.make("CartPole-v1")

    # Create an RLModule to later checkpoint.
    rl_module = DefaultPPOTorchRLModule(
        observation_space=env.observation_space,
        action_space=env.action_space,
        model_config=DefaultModelConfig(fcnet_hiddens=[32]),
    )

    # Finally, write the RLModule checkpoint.
    module_ckpt_path = tempfile.mkdtemp()
    rl_module.save_to_path(module_ckpt_path)


Creating an RLModule from an (RLModule) checkpoint
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you have an RLModule checkpoint saved and would like to create a new RLModule directly from it,
use the :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.from_checkpoint` method:


.. testcode::

    from ray.rllib.core.rl_module.rl_module import RLModule

    # Create a new RLModule from the checkpoint.
    new_module = RLModule.from_checkpoint(module_ckpt_path)


Loading an RLModule checkpoint into a running Algorithm
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. testcode::

    from ray.rllib.algorithms.ppo import PPOConfig

    # Create a new Algorithm (with the changed module config: 32 units instead of the
    # default 256; otherwise loading the state of `module` will fail due to a shape
    # mismatch).
    config = (
        PPOConfig()
        .environment("CartPole-v1")
        .rl_module(model_config=DefaultModelConfig(fcnet_hiddens=[32]))
    )
    ppo = config.build()


Now you can load the saved RLModule state (from the preceding `module.save_to_path()`) directly
into the running Algorithm's RLModule(s). Note that all RLModules within the algorithm get updated, the ones
in the Learner workers and the ones in the EnvRunners.

.. testcode::

    ppo.restore_from_path(
        module_ckpt_path,  # <- NOT an Algorithm checkpoint, but single-agent RLModule one.

        # Therefore, we have to provide the exact path (of RLlib components) down
        # to the individual RLModule within the algorithm, which is:
        component="learner_group/learner/rl_module/default_policy",
    )

.. testcode::
    :hide:

    import shutil

    ppo.stop()
    shutil.rmtree(module_ckpt_path)
