.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rlmodule-guide:

RL Modules
==========

:py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` is the main neural network class in RLlib's new API stack and exposes
three public methods, each corresponding to a distinct phase in the reinforcement learning cycle:
:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_exploration` handles the computation of actions during data collection
(if the data is used for a succeeding training step), balancing exploration and exploitation.
:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_inference` is used to compute actions during evaluation (for example in production),
often requiring greedy or less stochastic action selection.
Finally, :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_train` manages the training phase, performing calculations required to
compute losses, such as Q-values in a DQN model, value function predictions in a PG-style setup,
or world-model predictions in model-based algorithms.


.. figure:: images/rl_modules/rl_module_overview.svg
    :width: 600

    **RLModule overview**: A plain :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` contains the
    neural network used for computations
    (for example, a policy network) and exposes the three forward methods:
    `forward_exploration` (sample collection), `forward_inference` (production), and
    `forward_train` (aiding loss computations for training).
    A :py:class:`~ray.rllib.core.rl_module.multi_rl_module.MultiRLModule` may contain one
    or more sub-RLModules, each identified by a `ModuleID`. This allows you to implement
    arbitrarily complex multi-network and/or multi-agent architectures and algorithms.


Enabling the RLModule API in the AlgorithmConfig
------------------------------------------------

RLModules are used exclusively in the :ref:`new API stack <rllib-new-api-stack-guide>`, which is activated by default in RLlib.

In case you are working with a legacy config and would like to migrate it to the new API stack, see
:ref:`our new API stack migration guide <rllib-new-api-stack-migration-guide>` for more information.

If you have a config that's accidentally still set to the old API stack,
use the :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.api_stack`
method to switch:

.. testcode::

    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

    config = (
        AlgorithmConfig()
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
    )


Default RLModules
-----------------

If you don't specify any module-related settings in your
:py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`, RLlib uses the
respective algorithm's default RLModule, which is usually a great choice for initial
experimentation and benchmarking. All of these default RLModules support 1D-tensor- and
image observations (``[width] x [height] x [channels]``).



In order to


Constructing RL Modules
-----------------------

The :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` API allows you to define custom models in RLlib,
including highly complex multi-network setups, often found in multi-agent- or model-based algorithms.

To maintain consistency and usability, RLlib offers a standardized approach for constructing
:py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` instances for both single-module use cases
(for example for single-agent) and multi-module use cases (for example for multi-agent learning or other multi-NN setups).

The most direct and easiest way to construct your :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`
is through its constructor.

.. testcode::

    import gymnasium as gym
    from ray.rllib.algorithms.bc.torch.default_bc_torch_rl_module import DefaultBCTorchRLModule

    # Create an env object to know the spaces.
    env = gym.make("CartPole-v1")

    # Construct the actual RLModule object.
    rl_module = DefaultBCTorchRLModule(
        observation_space=env.observation_space,
        action_space=env.action_space,
        # A custom dict that will be accessible inside your class as `self.model_config`.
        model_config={"fcnet_hiddens": [64]},
    )

However, since RLlib is a distributed RL library and needs to create more than one copy of
your RLModule, you can use :py:class:`~ray.rllib.core.rl_module.rl_module.RLModuleSpec` objects
to define, how RLlib should construct each such copy of your
:py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`.

Constructing an :py:class:`~ray.rllib.core.rl_module.rl_module.RLModuleSpec` is straightforward
and analogous to creating an :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` through its constructor:

.. tab-set::

    .. tab-item:: Single-Module (ex. single-agent)

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
                # A custom dict that will be accessible inside your class as `self.model_config`.
                model_config={"fcnet_hiddens": [64]},
            )

            # Then, build the RLModule through the spec's `build()` method.
            rl_module = spec.build()

    .. tab-item:: Multi-Module (ex. multi-agent)

        .. testcode::

            import gymnasium as gym
            from ray.rllib.algorithms.bc.torch.default_bc_torch_rl_module import DefaultBCTorchRLModule
            from ray.rllib.core.rl_module.rl_module import RLModuleSpec
            from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec

            # First construct the MultiRLModuleSpec.
            spec = MultiRLModuleSpec(
                module_specs={
                    "module_1": RLModuleSpec(
                        module_class=DefaultBCTorchRLModule,

                        # Define the spaces for only this sub-module.
                        observation_space=gym.spaces.Box(low=-1, high=1, shape=(10,)),
                        action_space=gym.spaces.Discrete(2),

                        # A custom dict that will be accessible inside your class as
                        # `self.model_config`.
                        model_config={"fcnet_hiddens": [32]},
                    ),
                    "module_2": RLModuleSpec(
                        module_class=DiscreteBCTorchModule,

                        # Define the spaces for only this sub-module.
                        observation_space=gym.spaces.Box(low=-1, high=1, shape=(5,)),
                        action_space=gym.spaces.Discrete(2),

                        # A custom dict that will be accessible inside your class as
                        # `self.model_config`.
                        model_config={"fcnet_hiddens": [16]},
                    ),
                },
            )

            # Construct the actual MultiRLModule instance with .build():
            multi_rl_module = spec.build()


You can pass `RLModuleSpecs` instances to your :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` in order to
tell RLlib to use a particular module class and constructor arguments with the respective algorithm:

.. tab-set::

    .. tab-item:: Single-Module (ex. single-agent)

        .. testcode::

            from ray.rllib.core.rl_module.rl_module import RLModuleSpec
            from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
            from ray.rllib.core.testing.bc_algorithm import BCConfigTest


            config = (
                BCConfigTest()
                .api_stack(
                    enable_rl_module_and_learner=True,
                    enable_env_runner_and_connector_v2=True,
                )
                .environment("CartPole-v1")
                .rl_module(
                    rl_module_spec=RLModuleSpec(
                        module_class=DiscreteBCTorchModule,
                        # The `self.model_config` attribute is available everywhere inside your
                        # custom `DiscreteBCTorchModule` class.
                        model_config={"fcnet_hiddens": [32, 32]},
                    ),
                )
            )

            algo = config.build()
            print(algo.get_module())

        .. note::
            For passing `RLModuleSpecs`, some fields (for example, `observation_space` and `action_space`)
            don't have to be filled and are auto-provided based on the RL environment or other algorithm configuration parameters.

    .. tab-item:: Multi-Agent (shared policy net)

        .. testcode::

            from ray.rllib.core.rl_module.rl_module import RLModuleSpec
            from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
            from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
            from ray.rllib.core.testing.bc_algorithm import BCConfigTest
            from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole

            config = (
                BCConfigTest()
                .api_stack(
                    enable_rl_module_and_learner=True,
                    enable_env_runner_and_connector_v2=True,
                )
                .environment(MultiAgentCartPole, env_config={"num_agents": 2})
                .rl_module(
                    rl_module_spec=MultiRLModuleSpec(
                        # All agents (0 and 1) use the same (single) RLModule.
                        rl_module_specs=RLModuleSpec(
                            module_class=DiscreteBCTorchModule,
                            model_config={"fcnet_hiddens": [32, 32]},
                        )
                    ),
                )
            )
            algo = config.build()
            print(algo.get_module())

    .. tab-item:: Multi-Agent (two or more policy nets)

        .. testcode::

            from ray.rllib.core.rl_module.rl_module import RLModuleSpec
            from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
            from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
            from ray.rllib.core.testing.bc_algorithm import BCConfigTest
            from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole

            config = (
                BCConfigTest()
                .api_stack(
                    enable_rl_module_and_learner=True,
                    enable_env_runner_and_connector_v2=True,
                )
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
                                module_class=DiscreteBCTorchModule,
                                # Small network.
                                model_config={"fcnet_hiddens": [32, 32]},
                            ),
                            "p1": RLModuleSpec(
                                module_class=DiscreteBCTorchModule,
                                # Large network.
                                model_config={"fcnet_hiddens": [128, 128]},
                            ),
                        },
                    ),
                )
            )
            algo = config.build()
            print(algo.get_module())


.. _rllib-writing-custom-rl-modules:

Writing Custom RL Modules
-------------------------

For single-agent learning or for independent multi-agent learning with any algorithm (e.g., PPO, DQN),
use :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`. For more advanced multi-agent use cases
(for example with shared communication between agents) or any other multi-network use cases, extend the
:py:class:`~ray.rllib.core.rl_module.multi_rl_module.MultiRLModule` class.

RLlib implements the following abstract framework specific base classes:

- :class:`TorchRLModule <ray.rllib.core.rl_module.torch_rl_module.TorchRLModule>`: For PyTorch-based RL Modules.
- :class:`TfRLModule <ray.rllib.core.rl_module.tf.tf_rl_module.TfRLModule>`: For TensorFlow-based RL Modules.

When writing an actual neural network class (as opposed to a heuristic model without any neural network elements), you should first implement
the setup() method, where you can add any NN subcomponent you might need and assign these to attributes of your choice:

.. testcode::

    import torch
    from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule

    class MyTorchPolicy(TorchRLModule):

        def setup(self) -> None:
            # You have access here to the following already set attributes:
            # self.observation_space
            # self.action_space
            # self.inference_only
            # self.model_config  # <- a dict with custom settings
            # self.catalog
            input_dim = self.observation_space.shape[0]
            hidden_dim = self.model_config["fcnet_hiddens"][0]
            output_dim = self.action_space.n

            # Build all the layers and subcomponents here you need for the
            # RLModule's forward passes.
            self._pi_head = torch.nn.Sequential(
                torch.nn.Linear(input_dim, hidden_dim),
                torch.nn.ReLU(),
                torch.nn.Linear(hidden_dim, output_dim),
            )

After that, the minimum requirement, when subclassing :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`, is to implement the following three methods:

- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_inference`: Forward pass for action inference (greedy).
- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration`: Forward pass for computing exploration actions (for collecting training data).
- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_train`: Forward pass for training (right before loss computation).

For your custom :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_inference` and
:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration`
methods, you must return a dictionary that either contains the key "actions" and/or the key "action_dist_inputs".

If you return the "actions" key:

- RLlib will use the actions provided thereunder as-is.
- If you also returned the "action_dist_inputs" key: RLlib will also create a :py:class:`~ray.rllib.models.distributions.Distribution` object from the distribution parameters under that key and - in the case of :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_exploration` - compute action probs and logp values from the given actions automatically.

If you don't return the "actions" key:

- You must return the "action_dist_inputs" key instead from your :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration` and :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_inference` methods.
- RLlib will create a :py:class:`~ray.rllib.models.distributions.Distribution` object from the distribution parameters under that key and sample actions from the thus generated distribution.
- In the case of :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration`, RLlib will also compute action probs and logp values from the sampled actions automatically.

.. note::

    In the case of :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_inference`,
    the generated distributions (from returned key "action_dist_inputs") will always be made deterministic first via
    the :py:meth:`~ray.rllib.models.distributions.Distribution.to_deterministic` utility before a possible action sample step.
    For example, sampling from a Categorical distribution will be reduced to selecting the argmax actions from the distribution's logits/probs.

Commonly used distribution implementations can be found under ``ray.rllib.models.torch.torch_distributions`` for torch.
You can choose to compute determinstic actions, by creating the determinstic counterpart of your distribution class through
the :py:meth:`~ray.rllib.models.distributions.Distribution.to_deterministic` method.


.. tab-set::

    .. tab-item:: Returning "actions" key

        .. code-block:: python

            from ray.rllib.core import Columns, TorchRLModule

            class MyTorchPolicy(TorchRLModule):
                ...

                def _forward_inference(self, batch):
                    ...
                    return {
                        Columns.ACTIONS: ...  # actions will be used as-is
                    }

                def _forward_exploration(self, batch):
                    ...
                    return {
                        Columns.ACTIONS: ...  # actions will be used as-is (no sampling step!)
                        Columns.ACTION_DIST_INPUTS: ...  # optional: If provided, will be used to compute action probs and logp.
                    }

    .. tab-item:: Not returning "actions" key

        .. code-block:: python

            from ray.rllib.core import Columns, TorchRLModule

            class MyTorchPolicy(TorchRLModule):
                ...

                def _forward_inference(self, batch):
                    ...
                    return {
                        # RLlib will:
                        # - Generate distribution from these parameters.
                        # - Convert distribution to a deterministic equivalent.
                        # - "sample" from the deterministic distribution.
                        Columns.ACTION_DIST_INPUTS: ...
                    }

                def _forward_exploration(self, batch):
                    ...
                    return {
                        # RLlib will:
                        # - Generate distribution from these parameters.
                        # - "sample" from the (stochastic) distribution.
                        # - Compute action probs/logs automatically using the sampled
                        #   actions and the generated distribution object.
                        Columns.ACTION_DIST_INPUTS: ...
                    }


You should never override the constructor (`__init__`) itself, however, it might be important for your understanding of this API to know that the
:py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` class's constructor requires the following arguments (and receives these properly when
a spec's `build()` method is called):

- :py:attr:`~ray.rllib.core.rl_module.rl_module.RLModule.observation_space`: The observation space (after having passed all connectors); This is the actual input space for the model after all preprocessing steps.
- :py:attr:`~ray.rllib.core.rl_module.rl_module.RLModule.action_space`: The action space of the environment.
- :py:attr:`~ray.rllib.core.rl_module.rl_module.RLModule.inference_only`: Whether the RLModule should be built in inference-only mode, dropping subcomponents that are only needed for learning.
- :py:attr:`~ray.rllib.core.rl_module.rl_module.RLModule.model_config`: The model config, which is either a custom dictionary (for custom RLModules) or a :py:class:`~ray.rllib.core.rl_module.default_model_config.DefaultModelConfig` dataclass object (only for RLlib's default models). Model hyper-parameters such as number of layers, type of activation, etc. are defined here.
- `catalog_class`: The type of the :py:class:`~ray.rllib.core.models.catalog.Catalog` object to build the RLModule.


Putting all of the above together, we get the following working example of a custom RLModule implementation:

.. testcode::

    from typing import Any, Dict
    from ray.rllib.core import Columns
    from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule

    class MyTorchPolicy(TorchRLModule):
        def setup(self) -> None:
            # You have access here to the following already set attributes:
            # self.observation_space
            # self.action_space
            # self.inference_only
            # self.model_config  # <- a dict with custom settings
            # self.catalog
            input_dim = self.observation_space.shape[0]
            hidden_dim = self.model_config["fcnet_hiddens"][0]
            output_dim = self.action_space.n

            # Build all the layers and subcomponents here you need for the
            # RLModule's forward passes.
            self._pi_head = nn.Sequential(
                nn.Linear(input_dim, hidden_dim),
                nn.ReLU(),
                nn.Linear(hidden_dim, output_dim),
            )

        def _forward_train(self, batch) -> dict:
            # Push the observations from the batch through our pi-head.
            action_logits = self._pi_head(batch[Columns.OBS])

            # Return parameters for the (default) action distribution, which is
            # `TorchCategorical` (due to our action space being `gym.spaces.Discrete`).
            return {
                Columns.ACTION_DIST_INPUTS: action_logits,
            }

        def _forward_inference(self, batch) -> dict:
            with torch.no_grad():
                return self._forward_train(batch)

        def _forward_exploration(self, batch) -> dict:
            with torch.no_grad():
                return self._forward_train(batch)


Writing Custom Multi-RL Modules
-------------------------------

For multi-module setups, RLlib provides the :py:class:`~ray.rllib.core.rl_module.multi_rl_module.MultiRLModule` class,
whose default implementation is a dictionary of individual :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` objects,
one for each submodule.
The base-class implementation works for most of use cases that need to define independent neural networks.
However, for any complex, multi-network or multi-agent use cases, where agents share one or more neural networks,
you should inherit from this class and override the default implementation.

The following code snippets create a custom multi-agent RL module with two simple "policy head" modules, which
share an encoder (the third network in the MultiRLModule). The encoder receives the observations from the env
and outputs feature vectors that then serve as input for the two policy heads to compute the agents' actions.


.. tab-set::

    .. tab-item:: Policy head module

        .. testcode::

            from ray.rllib.core import Columns
            from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
            from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule

            import torch
            import torch.nn as nn


            class PGPolicyUsingSharedEncoder(TorchRLModule):
                """A PG-style RLModule using the inputs (features) of a shared encoder.

                Note that the shared encoder is a separate RLModule handled separately by the
                top level MultiRLModule that holds this RLModule.
                """
                def setup(self):
                    feature_dim = self.model_config["feature_dim"]
                    hidden_dim = self.model_config["hidden_dim"]

                    self._pi_head = nn.Sequential(
                        nn.Linear(feature_dim, hidden_dim),
                        nn.ReLU(),
                        nn.Linear(hidden_dim, self.action_space.n),
                    )

                def _forward_inference(self, batch):
                    with torch.no_grad():
                        return self._common_forward(batch)

                def _forward_exploration(self, batch):
                    with torch.no_grad():
                        return self._common_forward(batch)

                def _forward_train(self, batch):
                    return self._common_forward(batch)

                def _common_forward(self, batch):
                    features = batch["encoder_features"]
                    logits = self._pi_head(features)

                    return {Columns.ACTION_DIST_INPUTS: logits}

    .. tab-item:: MultiRLModule with shared encoder

        .. testcode::

            class MultiAgentPGWithSharedEncoder(MultiRLModule):
                def setup(self):
                    # Call the super constructor to create all submodules (the policies)
                    # first.
                    super().setup()

                    # Then construct the shared encoder
                    obs_dim = self.observation_space.shape[0]
                    hidden_dim = self.model_config["encoder_hidden_dim"]
                    feature_dim = self.model_config["feature_dim"]
                    self._shared_encoder = nn.Sequential(
                        nn.Linear(obs_dim, hidden_dim),
                        nn.ReLU(),
                        nn.Linear(hidden_dim, feature_dim),
                    )

                def _forward_inference(self, batch):
                    # Pass observations through the shared encoder.
                    features = self._shared_encoder(batch[Columns.OBS])
                    # Pass computed features through all submodules (policies).
                    ret = {
                        mid: {
                            Columns.ACTION_DIST_INPUTS: self[mid].forward_inference(
                                batch[mid]
                            )
                        } mid for mid, logits in self.items()
                    }
                    return ret

                def _forward_exploration(self, batch):
                    return self._forward_inference(batch)

                def _forward_train(self, batch):
                    return self._forward_inference(batch)


To plugin this custom multi-agent RL module into your algorithm's config, pass the new class
and its settings to the :py:class:`~ray.rllib.core.rl_module.multi_rl_module.MultiRLModuleSpec`.
Also, pass the :py:class:`~ray.rllib.core.rl_module.rl_module.RLModuleSpec` for each agent
because RLlib requires the observation, action spaces, and model hyper-parameters for each agent:


.. testcode::

    from ray.rllib.algorithms.ppo import PPOConfig
    from ray.rllib.core import MultiRLModuleSpec, RLModuleSpec
    from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole

    FEATURE_DIM = 128
    ENCODER_HIDDEN_DIM = 256

    config = (
        PPOConfig()
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        .environment(MultiAgentCartPole, env_config={"num_agents": 2})
        .multi_agent(
            policies={"p0", "p1"},
            # Agent IDs of `MultiAgentCartPole` are 0 and 1, mapping to
            # "p0" and "p1", respectively.
            policy_mapping_fn=lambda agent_id, episode, **kw: f"p{agent_id}"
        )
        .rl_module(
            rl_module_spec=MultiRLModuleSpec(
                # Speficy our custom MultiRLModule class.
                multi_rl_module_class=MultiAgentPGWithSharedEncoder,
                model_config={
                    "feature_dim": FEATURE_DIM,
                    "encoder_hidden_dim": ENCODER_HIDDEN_DIM,
                }.
                # Specify the individual policy RLModules (with different hidden dims).
                rl_module_specs={
                    "p0": RLModuleSpec(
                        module_class=PGPolicyUsingSharedEncoder,
                        # Large policy net.
                        model_config={"feature_dim": FEATURE_DIM, "hidden_dim": 1024},
                    ),
                    "p1": RLModuleSpec(
                        module_class=PGPolicyUsingSharedEncoder,
                        # Small policy net.
                        model_config={"feature_dim": FEATURE_DIM, "hidden_dim": 64},
                    ),
                },
            ),
        )
    )
    algo = config.build()
    print(algo.get_module())



Extending Existing RLlib RL Modules
-----------------------------------

RLlib provides a number of default RL Modules for the different algorithms as well as some example custom RLModule implementations
catering to specific requirements.

See `this CNN example here <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/classes/tiny_atari_cnn_rlm.py>`__
for training with Atari envs and
`this LSTM example here <https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/classes/lstm_containing_rlm.py>`__
for training in non-Markovian envs.

To customize any existing RLModule you can subclass it and change the :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.setup` to add your
custom components to it as well as override the forward methods for implementing a custom flow through the model.

Here are two good recipes for extending existing RL Modules:

.. tab-set::

    .. tab-item:: Subclass Torch/Tf base class and add APIs




    .. tab-item:: Subclass existing RLlib default RLModule

        The default way to extend existing RLModules is to inherit from the framework specific subclasses
        (for example :py:class:`~ray.rllib.core.rl_module.torch.torch_rl_module.TorchRLModule`) and override
        the :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.setup` method, but at a minimum the
        three `_forward_()` methods and any other method you need to customize. Then pass the new customized class
        into the :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.rl_module` method
        (`config.rl_module(rl_module_spec=RLModuleSpec(module_class=[your class]))`) to train your custom RLModule.

        .. code-block:: python

            import torch
            nn = torch.nn

            class MyRLModule(TorchRLModule):

                def setup(self):
                    # You have access here to the following already set attributes:
                    self.observation_space
                    self.action_space
                    self.inference_only
                    self.model_config  # <- a dict with custom settings
                    self.catalog
                    ...

                    # Build all the layers and subcomponents here you need for the
                    # RLModule's forward passes.
                    # For example:
                    self._encoder_fcnet = nn.Sequential(...)
                    ...

            # Pass in the custom RL Module class to the spec
            algo_config = algo_config.rl_module(
                rl_module_spec=RLModuleSpec(module_class=MyRLModule)
            )

        A concrete example: If you want to replace the default encoder that RLlib builds for torch, PPO and a given observation space,
        you can override the `setup` method on the :py:class:`~ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module.PPOTorchRLModule`
        class to create your custom encoder instead of the default one. We do this in the following example.

        .. literalinclude:: ../../../rllib/examples/rl_modules/classes/mobilenet_rlm.py
                :language: python
                :start-after: __sphinx_doc_begin__
                :end-before: __sphinx_doc_end__


Checkpointing RL Modules
------------------------

RL Modules can be checkpointed with their :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.save_to_path` method.
If you have a checkpoint saved and would like to create an RL Module directly from it, use the
:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.from_checkpoint` method.
If you already have an instantiated RLModule and would like to load a new state (weights) into it from an existing
checkpoint, use the :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.restore_from_path` method.

The following example shows how these methods can be used outside of, or in conjunction with, an RLlib Algorithm.

.. testcode::
    import gymnasium as gym
    import shutil
    import tempfile
    from ray.rllib.algorithms.ppo import PPOConfig
    from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
    from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
    from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleSpec

    config = (
        PPOConfig()
        # Enable the new API stack (RLModule and Learner APIs).
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        ).environment("CartPole-v1")
    )
    env = gym.make("CartPole-v1")
    # Create an RL Module that we would like to checkpoint
    module_spec = RLModuleSpec(
        module_class=PPOTorchRLModule,
        observation_space=env.observation_space,
        action_space=env.action_space,
        # If we want to use this externally created module in the algorithm,
        # we need to provide the same config as the algorithm. Any changes to
        # the defaults can be given via the right side of the `|` operator.
        model_config_dict=config.model_config | {"fcnet_hiddens": [32]},
        catalog_class=PPOCatalog,
    )
    module = module_spec.build()

    # Create the checkpoint.
    module_ckpt_path = tempfile.mkdtemp()
    module.save_to_path(module_ckpt_path)

    # Create a new RLModule from the checkpoint.
    loaded_module = RLModule.from_checkpoint(module_ckpt_path)

    # Create a new Algorithm (with the changed module config: 32 units instead of the
    # default 256; otherwise loading the state of `module` will fail due to a shape
    # mismatch).
    config.rl_module(model_config_dict=config.model_config | {"fcnet_hiddens": [32]})
    algo = config.build()
    # Now load the saved RLModule state (from the above `module.save_to_path()`) into the
    # Algorithm's RLModule(s). Note that all RLModules within the algo get updated, the ones
    # in the Learner workers and the ones in the EnvRunners.
    algo.restore_from_path(
        module_ckpt_path,  # <- NOT an Algorithm checkpoint, but single-agent RLModule one.
        # We have to provide the exact component-path to the (single) RLModule
        # within the algorithm, which is:
        component="learner_group/learner/rl_module/default_policy",
    )

.. testcode::
    :hide:

    algo.stop()
    shutil.rmtree(module_ckpt_path)


Migrating from Custom Policies and ModelV2 to RL Modules
--------------------------------------------------------

This document is for those who have implemented custom policies and models in RLlib and want to migrate to the
new :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` API.
