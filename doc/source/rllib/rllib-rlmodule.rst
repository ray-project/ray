.. include:: /_includes/rllib/announcement.rst

.. include:: /_includes/rllib/we_are_hiring.rst

.. |tensorflow| image:: images/tensorflow.png
    :class: inline-figure
    :width: 16

.. |pytorch| image:: images/pytorch.png
    :class: inline-figure
    :width: 16


RL Modules (Alpha)
==================

.. note::

    This is an experimental module that serves as a general replacement for ModelV2, and is subject to change. It will eventually match the functionality of the previous stack. If you only use high-level RLlib APIs such as :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` you should not experience siginficant changes, except for a few new parameters to the configuration object. If you've used custom models or policies before, you'll need to migrate them to the new modules. Check the Migration guide for more information.

    The table below shows the list of migrated algorithms and their current supported features, which will be updated as we progress.

    .. list-table::
       :header-rows: 1
       :widths: 20 20 20 20 20 20

       * - Algorithm
         - Independent MARL
         - Fully-connected
         - Image inputs (CNN)
         - RNN support (LSTM)
         - Complex observations (ComplexNet)
       * - **PPO**
         - |pytorch| |tensorflow|
         - |pytorch| |tensorflow|
         - |pytorch|
         -
         - |pytorch|
       * - **Impala**
         - |pytorch| |tensorflow|
         - |pytorch| |tensorflow|
         - |pytorch|
         -
         - |pytorch|
       * - **APPO**
         - |tensorflow|
         - |tensorflow|
         - 
         - 
         - 



RLModule is a neural network container that implements three public methods: :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_train`, :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_exploration`, and :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_inference`. Each method corresponds to a distinct reinforcement learning phase.

:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_exploration` handles acting and data collection, balancing exploration and exploitation. On the other hand, the :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_inference` serves the learned model during evaluation, often being less stochastic.

:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_train` manages the training phase, handling calculations exclusive to computing losses, such as learning Q values in a DQN model.

Enabling RL Modules in the Configuration
-----------------------------------------

Enable RLModules by setting the ``_enable_rl_module_api`` flag to ``True`` in the configuration object. 

.. code-block:: python

    import torch
    from pprint import pprint

    from ray.rllib.algorithms.ppo import PPOConfig

    config = (
        PPOConfig()
        .framework("torch")
        .environment("CartPole-v1")
        .rl_module(_enable_rl_module_api=True)
    )

    algorithm = config.build()

    # run for 2 training steps
    for _ in range(2):
        result = algorithm.train()
        pprint(result)

Constructing RL Modules
-----------------------
RLModule API provides a unified way to define custom reinforcement learning models in RLlib. This API enables you to design and implement your own models to suit specific needs.

To maintain consistency and usability, RLlib offers a standardized approach for defining module objects for both single-agent and multi-agent reinforcement learning environments. This is achieved through the :py:class:`~ray.rllib.core.rl_module.rl_module.SingleAgentRLModuleSpec` and :py:class:`~ray.rllib.core.rl_module.marl_module.MultiAgentRLModuleSpec` classes. The built-in RLModules in RLlib follow this consistent design pattern, making it easier for you to understand and utilize these modules.

.. tabbed:: Single Agent

    .. code-block:: python

        import gymnasium as gym
        from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
        from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule

        env = gym.make("CartPole-v1")

        spec = SingleAgentRLModuleSpec(
            module_class=DiscreteBCTorchModule,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config_dict={"fcnet_hiddens": [64]},
        )

        module = spec.build()


.. tabbed:: Multi Agent

    .. code-block:: python

        import gymnasium as gym
        from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
        from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
        from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule

        spec = MultiAgentRLModuleSpec(
            module_specs = {
                "module_1": SingleAgentRLModuleSpec(
                    module_class=DiscreteBCTorchModule,
                    observation_space=gym.spaces.Box(low=-1, high=1, shape=(10,)),
                    action_space=gym.spaces.Discrete(2),
                    model_config_dict={"fcnet_hiddens": [32]}
                ),
                "module_2": SingleAgentRLModuleSpec(
                    module_class=DiscreteBCTorchModule,
                    observation_space=gym.spaces.Box(low=-1, high=1, shape=(5,)),
                    action_space=gym.spaces.Discrete(2),
                    model_config_dict={"fcnet_hiddens": [16]}
                )
            },
        )

        marl_module = spec.build()


You can pass RLModule specs to the algorithm configuration to be used by the algorithm.

.. tabbed:: Single Agent

    .. code-block:: python

        import gymnasium as gym
        from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
        from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
        from ray.rllib.core.testing.bc_algorithm import BCConfigTest


        config = (
            BCConfigTest()
            .environment("CartPole-v1")
            .rl_module(
                _enable_rl_module_api=True,
                rl_module_spec=SingleAgentRLModuleSpec(module_class=DiscreteBCTorchModule),
            )
            .training(model={"fcnet_hiddens": [32, 32]})
        )

        algo = config.build()


    .. note::
        For passing RLModule specs, all fields do not have to be filled as they are filled based on the described environment or other algorithm configuration parameters (i.e. ,``observation_space``, ``action_space``, ``model_config_dict`` are not required fields when passing a custom RLModule spec to the algorithm config.)


.. tabbed:: Multi Agent

    .. code-block:: python

        import gymnasium as gym
        from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
        from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
        from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
        from ray.rllib.core.testing.bc_algorithm import BCConfigTest
        from ray.rllib.examples.env.multi_agent import MultiAgentCartPole


        config = (
            BCConfigTest()
            .environment(MultiAgentCartPole, env_config={"num_agents": 2})
            .rl_module(
                _enable_rl_module_api=True,
                rl_module_spec=MultiAgentRLModuleSpec(
                    module_specs=SingleAgentRLModuleSpec(module_class=DiscreteBCTorchModule)
                )
            )
            .training(model={"fcnet_hiddens": [32, 32]})
        )



Writing Custom Single Agent RL Modules 
--------------------------------------

For single-agent algorithms (e.g., PPO, DQN) or independent multi-agent algorithms (e.g., PPO-MultiAgent), use :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`. For more advanced multi-agent use cases with a shared communication between agents, extend the :py:class:`~ray.rllib.core.rl_module.marl_module.MultiAgentRLModule` class. 

RLlib treats single-agent modules as a special case of :py:class:`~ray.rllib.core.rl_module.marl_module.MultiAgentRLModule` with only one module. Create the multi-agent representation of all RLModules by calling :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.as_multi_agent`. For example:

.. code-block:: python

    import gymnasium as gym
    from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
    from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule

    env = gym.make("CartPole-v1")
    spec = SingleAgentRLModuleSpec(
        module_class=DiscreteBCTorchModule,
        observation_space=env.observation_space,
        action_space=env.action_space,
        model_config_dict={"fcnet_hiddens": [64]},
    )

    module = spec.build()
    marl_module = module.as_multi_agent()

RLlib implements the following abstract framework specific base classes:

- :class:`TorchRLModule <ray.rllib.core.rl_module.torch_rl_module.TorchRLModule>`: For PyTorch-based RLModules.
- :class:`TfRLModule <ray.rllib.core.rl_module.tf.tf_rl_module.TfRLModule>`: For TensorFlow-based RLModules.

The minimum requirement is for sub-classes of :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` is to implement the following methods:

- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_train`: Forward pass for training.

- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_inference`: Forward pass for inference.

- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration`: Forward pass for exploration.

Also the class's constrcutor requires a dataclass config object called `~ray.rllib.core.rl_module.rl_module.RLModuleConfig` which contains the following fields:

- :py:attr:`~ray.rllib.core.rl_module.rl_module.RLModuleConfig.observation_space`: The observation space of the environment (either processed or raw).
- :py:attr:`~ray.rllib.core.rl_module.rl_module.RLModuleConfig.action_space`: The action space of the environment.
- :py:attr:`~ray.rllib.core.rl_module.rl_module.RLModuleConfig.model_config_dict`: The model config dictionary of the algorithm. Model hyper-parameters such as number of layers, type of activation, etc. are defined here.
- :py:attr:`~ray.rllib.core.rl_module.rl_module.RLModuleConfig.catalog_class`: The `Catalog` object of the algorithm.

When writing RLModules, you need to use these fields to construct your model. 

.. tabbed:: Single Agent (torch)

    .. code-block:: python

        from typing import Mapping, Any
        from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
        from ray.rllib.core.rl_module.rl_module import RLModuleConfig
        from ray.rllib.utils.nested_dict import NestedDict

        import torch
        import torch.nn as nn

        class DiscreteBCTorchModule(TorchRLModule):

            def __init__(self, config: RLModuleConfig) -> None:
                super().__init__(config)

                input_dim = self.config.observation_space.shape[0]
                hidden_dim = self.config.model_config_dict["fcnet_hiddens"][0]
                output_dim = self.config.action_space.n

                self.policy = nn.Sequential(
                    nn.Linear(input_dim, hidden_dim),
                    nn.ReLU(),
                    nn.Linear(hidden_dim, output_dim),
                )

                self.input_dim = input_dim

            def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
                with torch.no_grad():
                    return self._forward_train(batch)

            def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
                with torch.no_grad():
                    return self._forward_train(batch)

            def _forward_train(self, batch: NestedDict) -> Mapping[str, Any]:
                action_logits = self.policy(batch["obs"])
                return {"action_dist": torch.distributions.Categorical(logits=action_logits)}
    


.. tabbed:: Single Agent (tensorflow)

    .. code-block:: python
        
        from typing import Mapping, Any
        from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
        from ray.rllib.core.rl_module.rl_module import RLModuleConfig
        from ray.rllib.utils.nested_dict import NestedDict

        import tensorflow as tf

        class DiscreteBCTfModule(TfRLModule):

            def __init__(self, config: RLModuleConfig) -> None:
                super().__init__(config)

                input_dim = self.config.observation_space.shape[0]
                hidden_dim = self.config.model_config_dict["fcnet_hiddens"][0]
                output_dim = self.config.action_space.n

                self.policy = tf.keras.Sequential(
                    [
                        tf.keras.layers.Dense(hidden_dim, activation="relu"),
                        tf.keras.layers.Dense(output_dim),
                    ]
                )

                self.input_dim = input_dim

            def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
                return self._forward_train(batch)

            def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
                return self._forward_train(batch)

            def _forward_train(self, batch: NestedDict) -> Mapping[str, Any]:
                action_logits = self.policy(batch["obs"])
                return {"action_dist": tf.distributions.Categorical(logits=action_logits)}


In :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` you can enforce the checking for the existence of certain input or output keys in the data that is communicated into and out of RLModules. This serves multiple purposes: 

- For the I/O requirement of each method to be self-documenting.
- For failures to happen quickly. If users extend the modules and implement something that does not match the assumptions of the I/O specs, the check reports missing keys and their expected format. For example, we always RLModule should have an ``obs`` key on the input batch and an output ``action_dist`` key as the output. 

.. tabbed:: Single Level Keys

    .. code-block:: python

        from ray.rllib.models.specs.typing import SpecType
        
        class DiscreteBCTorchModule(TorchRLModule):
            ...
            def input_specs_exploration(self) -> SpecType:
                # Enforce that input nested dict to exploration method has a key "obs"
                return ["obs"]
            
            def output_specs_exploration(self) -> SpecType:
                # Enforce that output nested dict from exploration method has a key 
                # "action_dist"
                return ["action_dist"]

.. tabbed:: Nested Keys

    .. code-block:: python

        from ray.rllib.models.specs.typing import SpecType
        
        class DiscreteBCTorchModule(TorchRLModule):
            ...
            def input_specs_exploration(self) -> SpecType:
                # Enforce that input nested dict to exploration method has a key "obs"
                # and within that key, it has a key "global" and "local". There should 
                # also be a key "action_mask"
                return [("obs", "global"), ("obs", "local"), "action_mask"]



.. tabbed:: TensorShape Spec

    .. code-block:: python

        from ray.rllib.models.specs.typing import SpecType
        from ray.rllib.models.specs.torch_spec import TorchTensorSpec
        
        class DiscreteBCTorchModule(TorchRLModule):
            ...
            def input_specs_exploration(self) -> SpecType:
                # Enforce that input nested dict to exploration method has a key "obs"
                # and its value is a torch.Tensor with shape (b, h) where b is the
                # batch size (determined at run-time) and h is the hidden size 
                # (fixed at 10).
                return {"obs": TorchTensorSpec("b, h", h=10)}



.. tabbed:: Type Spec

    .. code-block:: python

        from ray.rllib.models.specs.typing import SpecType
        import torch
        
        class DiscreteBCTorchModule(TorchRLModule):
            ...
            def output_specs_exploration(self) -> SpecType:
                # Enforce that output nested dict from exploration method has a key
                # "action_dist" and its value is a torch.distribution.Categorical
                return {"action_dist": torch.distribution.Categorical}

:py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` has two methods for each forward method, totaling 6 methods that can be override to describe the specs of the input and output of each method:

- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.input_specs_inference`
- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.output_specs_inference`
- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.input_specs_exploration`
- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.output_specs_exploration`
- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.input_specs_train`
- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.output_specs_train`

To learn more, see the `SpecType` documentation.



Writing Custom Multi-Agent RL Modules (Advanced)
------------------------------------------------

For multi-agent modules, RLlib implements :py:class:`~ray.rllib.core.rl_module.marl_module.MultiAgentRLModule`. It is a dictionary of :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` objects, one for each policy, plus possibly some shared modules. The base-class implementation works for most of use-cases where we need to defined independent neural for sub-groups of agents. For more complex, multi-agent use-cases, where the agents share some part of their neural network, one should inherit from this class and override the default implementation.


The :py:class:`~ray.rllib.core.rl_module.marl_module.MultiAgentRLModule` offers an API for constructing custom models tailored to specific needs. The key method for this customization is :py:meth:`~ray.rllib.core.rl_module.marl_module.MultiAgentRLModule`.build.

The following example shows how we can create a custom multi-agent RL module where the underlying modules share an encoder that gets applied to the global part of the observations space, while the local part is passed through a separate encoder specific to each policy. 

.. tabbed:: Multi agent with shared encoder (Torch)

    .. code-block:: python

        from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
        from ray.rllib.core.rl_module.marl_module import (
            MultiAgentRLModuleConfig, MultiAgentRLModule
        )
        from ray.rllib.utils.nested_dict import NestedDict

        import torch
        import torch.nn as nn


        class BCTorchRLModuleWithSharedGlobalEncoder(TorchRLModule):
            """A RLModule with a shared encoder between agents for global observation."""

            def __init__(
                self, encoder: nn.Module, local_dim: int, hidden_dim: int, action_dim: int
            ) -> None:
                super().__init__(config=None)

                self.encoder = encoder
                self.policy_head = nn.Sequential(
                    nn.Linear(hidden_dim + local_dim, hidden_dim),
                    nn.ReLU(),
                    nn.Linear(hidden_dim, action_dim),
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
                obs = batch["obs"]
                global_enc = self.encoder(obs["global"])
                policy_in = torch.cat([global_enc, obs["local"]], dim=-1)
                action_logits = self.policy_head(policy_in)

                return {"action_dist": torch.distributions.Categorical(logits=action_logits)}



        class BCTorchMultiAgentModuleWithSharedEncoder(MultiAgentRLModule):
            def __init__(self, config: MultiAgentRLModuleConfig) -> None:
                super().__init__(config)

            def build(self):

                module_specs = self.config.modules
                module_spec = next(iter(module_specs.values()))
                global_dim = module_spec.observation_space["global"].shape[0]
                hidden_dim = module_spec.model_config_dict["fcnet_hiddens"][0]
                shared_encoder = nn.Sequential(
                    nn.Linear(global_dim, hidden_dim),
                    nn.ReLU(),
                    nn.Linear(hidden_dim, hidden_dim),
                )

                rl_modules = {}
                for module_id, module_spec in module_specs.items():
                    rl_modules[module_id] = BCTorchRLModuleWithSharedGlobalEncoder(
                        encoder=shared_encoder,
                        local_dim=module_spec.observation_space["local"].shape[0],
                        hidden_dim=hidden_dim,
                        action_dim=module_spec.action_space.n,
                    )

                self._rl_modules = rl_modules

To construct this custom multi-agent RL module, pass the class to the :py:class:`~ray.rllib.core.rl_module.marl_module.MultiAgentRLModuleSpec` constructor. Also, pass the :py:class:`~ray.rllib.core.rl_module.rl_module.SingleAgentRLModuleSpec` for each agent because RLlib requires the observation, action spaces, and model hyper-parameters for each agent.

.. code-block:: python

    import gymnasium as gym
    from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
    from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec

    spec = MultiAgentRLModuleSpec(
        marl_module_class=BCTorchMultiAgentModuleWithSharedEncoder,
        module_specs={
            "local_2d": SingleAgentRLModuleSpec(
                observation_space=gym.spaces.Dict({
                    "global": gym.spaces.Box(low=-1, high=1, shape=(2,)),
                    "local": gym.spaces.Box(low=-1, high=1, shape=(2,)),
                }),
                action_space=gym.spaces.Discrete(2),
                model_config_dict={"fcnet_hiddens": [64]},
            ),
            "local_5d": SingleAgentRLModuleSpec(
                observation_space=gym.spaces.Dict({
                    "global": gym.spaces.Box(low=-1, high=1, shape=(2,)),
                    "local": gym.spaces.Box(low=-1, high=1, shape=(5,)),
                }),
                action_space=gym.spaces.Discrete(5),
                model_config_dict={"fcnet_hiddens": [64]},
            ),
        },
    )

    module = spec.build()

Extending Existing RLlib RL Modules
-----------------------------------

RLlib provides a number of RLModules for different frameworks (e.g. PyTorch, TensorFlow, etc.). These modules can be extended by inheriting from them and overriding the methods you need to customize. For example, you can extend :py:class:`~ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module.PPOTorchRLModule` and augment it with your own customization. You can then pass the new customized class into the algorithm configuration. 

There are two ways possible to extend existing RLModules:

.. tabbed:: Inheriting existing RLModules

    One way to extend existing RLModules is to inherit from them and override the methods you need to customize. For example, you can extend :py:class:`~ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module.PPOTorchRLModule` and augment it with your own customization. You can then pass the new customized class into the algorithm configuration to use the PPO algorithm to optimize your custom RLModule.

    .. code-block:: python

        class MyPPORLModule(PPORLModule):
    
            def __init__(self, config: RLModuleConfig):
                super().__init__(config)
                ...

        # Pass in the custom RLModule class to the spec
        algo_config = algo_config.rl_module(
            rl_module_spec=SingleAgentRLModuleSpec(module_class=MyPPORLModule)
        )

    
.. tabbed:: Extending RLModule Catalog

    Another way you can customize your module by extending their catalog. The catalog is a component that defines the default architecture and behavior of a model based on factors such as ``observation_space``, ``action_space``, and so on. To modify sub-components of an existing RLModule, extend the corresponding catalog class.

    For instance, if you want to adapt the existing ``PPORLModule`` for a custom graph observation space not supported by RLlib out-of-the-box, you can extend the catalog class used to create the ``PPORLModule`` and override the method responsible for returning the encoder component. This will ensure your custom encoder replaces the default one initially provided by RLlib. For more information on the catalog class, refer to the `RLModule Catalog` user-guide.


    .. code-block:: python

        class MyAwesomeCatalog(PPOCatalog):

            def get_actor_critic_encoder_config():
                # create your awesome graph encoder here and return it
                pass
        

        # Pass in the custom catalog class to the spec
        algo_config = algo_config.rl_module(
            rl_module_spec=SingleAgentRLModuleSpec(catalog_class=MyAwesomeCatalog)
        )


Migrating from Custom Policies and Models to RLModules
------------------------------------------------------

This document is for those who have implemented custom policies and models in RLlib and want to migrate to the new RLModule API. If you have implemented custom policies that extended the `~ray.rllib.policy.eager_tf_policy_v2.EagerTFPolicyV2` or `~ray.rllib.policy.torch_policy_v2.TorchPolicyV2` classes, you likely did so that you could either modify the behavior of constructing models and distributions (via overriding `~ray.rllib.policy.torch_policy_v2.TorchPolicyV2.make_model`, `~ray.rllib.policy.torch_policy_v2.TorchPolicyV2.make_model_and_action_dist`), control the action sampling logic (via overriding `~ray.rllib.policy.eager_tf_policy_v2.EagerTFPolicyV2.action_distribution_fn` or `~ray.rllib.policy.eager_tf_policy_v2.EagerTFPolicyV2.action_sampler_fn`), or control the logic for infernce (via overriding `~ray.rllib.policy.policy.Policy.compute_actions_from_input_dict`, `~ray.rllib.policy.policy.Policy.compute_actions`, or `~ray.rllib.policy.policy.Policy.compute_log_likelihoods`). These APIs were built with `ray.rllib.models.modelv2.ModelV2` models in mind to enable you to customize the behavior of those functions. However `~ray.rllib.core.rl_module.rl_module.RLModule` is a more general abstraction that will reduce the amount of functions that you need to override.  

In the new `~ray.rllib.core.rl_module.rl_module.RLModule` API the construction of the models and the action distribution class that should be used are best defined in the constructor. That RLModule is constructed automatically if users follow the instructions outlined in the sections `Enabling RL Modules in the Configuration`_ and `Constructing RL Modules`_. `~ray.rllib.policy.policy.Policy.compute_actions` and `~ray.rllib.policy.policy.Policy.compute_actions_from_input_dict` can still be used for sampling actions for inference or exploration by using the ``explore=True|False`` parameter. If called with ``explore=True`` these functions will invoke `~ray.rllib.core.rl_module.rl_module.RLModule.forward_exploration` and if ``explore=False`` then they will call `~ray.rllib.core.rl_module.rl_module.RLModule.forward_inference`. 


What your customization could have looked like before:

.. tabbed:: ModelV2

    .. code-block:: python
        
        from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
        from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2


        class MyCustomModel(TorchModelV2):
            """Code for your previous custom model"""
            ...


        class CustomPolicy(TorchPolicyV2):

            @DeveloperAPI
            @OverrideToImplementCustomLogic
            def make_model(self) -> ModelV2:
                """Create model.

                Note: only one of make_model or make_model_and_action_dist
                can be overridden.

                Returns:
                ModelV2 model.
                """
                return MyCustomModel(...)


.. tabbed:: ModelV2 + Distribution


    .. code-block:: python
    
        from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
        from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2


        class MyCustomModel(TorchModelV2):
            """Code for your previous custom model"""
            ...


        class CustomPolicy(TorchPolicyV2):

            @DeveloperAPI
            @OverrideToImplementCustomLogic
            def make_model_and_action_dist(self):
                """Create model and action distribution function.

                Returns:
                    ModelV2 model.
                    ActionDistribution class.
                """
                my_model = MyCustomModel(...) # construct some ModelV2 instance here
                dist_class = ... # Action distribution cls 

                return my_model, dist_class


.. tabbed:: Sampler functions

    .. code-block:: python

        from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
        from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2

        class CustomPolicy(TorchPolicyV2):

            @DeveloperAPI
            @OverrideToImplementCustomLogic
            def action_sampler_fn(
                self,
                model: ModelV2,
                *,
                obs_batch: TensorType,
                state_batches: TensorType,
                **kwargs,
            ) -> Tuple[TensorType, TensorType, TensorType, List[TensorType]]:
                """Custom function for sampling new actions given policy.

                Args:
                    model: Underlying model.
                    obs_batch: Observation tensor batch.
                    state_batches: Action sampling state batch.

                Returns:
                    Sampled action
                    Log-likelihood
                    Action distribution inputs
                    Updated state
                """
                return None, None, None, None


            @DeveloperAPI
            @OverrideToImplementCustomLogic
            def action_distribution_fn(
                self,
                model: ModelV2,
                *,
                obs_batch: TensorType,
                state_batches: TensorType,
                **kwargs,
            ) -> Tuple[TensorType, type, List[TensorType]]:
                """Action distribution function for this Policy.

                Args:
                    model: Underlying model.
                    obs_batch: Observation tensor batch.
                    state_batches: Action sampling state batch.

                Returns:
                    Distribution input.
                    ActionDistribution class.
                    State outs.
                """
                return None, None, None


All of the ``Policy.compute_***`` functions expect that `~ray.rllib.core.rl_module.rl_module.RLModule.forward_exploration` and `~ray.rllib.core.rl_module.rl_module.RLModule.forward_inference` return a dictionary that contains the key "action_dist" mapping to a ``ray.rllib.models.distributions.Distribution`` instance. Commonly used distribution implementations can be found under ``ray.rllib.models.tf.tf_distributions`` for tensorflow and ``ray.rllib.models.torch.torch_distributions`` for torch. You can choose to return determinstic actions, by creating a determinstic distribution instance. See `Writing Custom Single Agent RL Modules`_ for more details on how to implement your own custom RLModule.

.. tabbed:: The Equivalent RLModule

    .. code-block:: python

        """
        No need to override any policy functions. Simply instead implement any custom logic in your custom RLModule
        """
        from ray.rllib.models.torch.torch_distributions import YOUR_DIST_CLASS


        class MyRLModule(TorchRLModule):

            def __init__(self, config: RLConfig):
                # construct any custom networks here using config
                # specify an action distribution class here
                ...

            def _forward_inference(self, batch):
                ...

            def _forward_exploration(self, batch):
                ...


Notable TODOs
-------------

- [] Add support for RNNs.
- [] Checkpointing.
- [] End to end example for custom RLModules extending PPORLModule (e.g. LLM)