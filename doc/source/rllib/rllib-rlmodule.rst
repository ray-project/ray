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



RL Module is a neural network container that implements three public methods: :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_train`, :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_exploration`, and :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_inference`. Each method corresponds to a distinct reinforcement learning phase.

:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_exploration` handles acting and data collection, balancing exploration and exploitation. On the other hand, the :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_inference` serves the learned model during evaluation, often being less stochastic.

:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_train` manages the training phase, handling calculations exclusive to computing losses, such as learning Q values in a DQN model.

Enabling RL Modules in the Configuration
----------------------------------------

Enable RL Modules by setting the ``_enable_rl_module_api`` flag to ``True`` in the configuration object.

.. literalinclude:: doc_code/rlmodule_guide.py
    :language: python
    :start-after: __enabling-rlmodules-in-configs-begin__
    :end-before: __enabling-rlmodules-in-configs-end__

Constructing RL Modules
-----------------------
The RLModule API provides a unified way to define custom reinforcement learning models in RLlib. This API enables you to design and implement your own models to suit specific needs.

To maintain consistency and usability, RLlib offers a standardized approach for defining module objects for both single-agent and multi-agent reinforcement learning environments. This is achieved through the :py:class:`~ray.rllib.core.rl_module.rl_module.SingleAgentRLModuleSpec` and :py:class:`~ray.rllib.core.rl_module.marl_module.MultiAgentRLModuleSpec` classes. The built-in RLModules in RLlib follow this consistent design pattern, making it easier for you to understand and utilize these modules.

.. tab-set::

    .. tab-item:: Single Agent

        .. literalinclude:: doc_code/rlmodule_guide.py
            :language: python
            :start-after: __constructing-rlmodules-sa-begin__
            :end-before: __constructing-rlmodules-sa-end__


    .. tab-item:: Multi Agent

        .. literalinclude:: doc_code/rlmodule_guide.py
            :language: python
            :start-after: __constructing-rlmodules-ma-begin__
            :end-before: __constructing-rlmodules-ma-end__


You can pass RL Module specs to the algorithm configuration to be used by the algorithm.

.. tab-set::

    .. tab-item:: Single Agent

        .. literalinclude:: doc_code/rlmodule_guide.py
            :language: python
            :start-after: __pass-specs-to-configs-sa-begin__
            :end-before: __pass-specs-to-configs-sa-end__


        .. note::
            For passing RL Module specs, all fields do not have to be filled as they are filled based on the described environment or other algorithm configuration parameters (i.e. ,``observation_space``, ``action_space``, ``model_config_dict`` are not required fields when passing a custom RL Module spec to the algorithm config.)


    .. tab-item:: Multi Agent

        .. literalinclude:: doc_code/rlmodule_guide.py
            :language: python
            :start-after: __pass-specs-to-configs-ma-begin__
            :end-before: __pass-specs-to-configs-ma-end__


Writing Custom Single Agent RL Modules
--------------------------------------

For single-agent algorithms (e.g., PPO, DQN) or independent multi-agent algorithms (e.g., PPO-MultiAgent), use :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`. For more advanced multi-agent use cases with a shared communication between agents, extend the :py:class:`~ray.rllib.core.rl_module.marl_module.MultiAgentRLModule` class. 

RLlib treats single-agent modules as a special case of :py:class:`~ray.rllib.core.rl_module.marl_module.MultiAgentRLModule` with only one module. Create the multi-agent representation of all RLModules by calling :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.as_multi_agent`. For example:

.. literalinclude:: doc_code/rlmodule_guide.py
    :language: python
    :start-after: __convert-sa-to-ma-begin__
    :end-before: __convert-sa-to-ma-end__

RLlib implements the following abstract framework specific base classes:

- :class:`TorchRLModule <ray.rllib.core.rl_module.torch_rl_module.TorchRLModule>`: For PyTorch-based RL Modules.
- :class:`TfRLModule <ray.rllib.core.rl_module.tf.tf_rl_module.TfRLModule>`: For TensorFlow-based RL Modules.

The minimum requirement is for sub-classes of :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` is to implement the following methods:

- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_train`: Forward pass for training.

- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_inference`: Forward pass for inference.

- :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule._forward_exploration`: Forward pass for exploration.

Also the class's constrcutor requires a dataclass config object called `~ray.rllib.core.rl_module.rl_module.RLModuleConfig` which contains the following fields:

- :py:attr:`~ray.rllib.core.rl_module.rl_module.RLModuleConfig.observation_space`: The observation space of the environment (either processed or raw).
- :py:attr:`~ray.rllib.core.rl_module.rl_module.RLModuleConfig.action_space`: The action space of the environment.
- :py:attr:`~ray.rllib.core.rl_module.rl_module.RLModuleConfig.model_config_dict`: The model config dictionary of the algorithm. Model hyper-parameters such as number of layers, type of activation, etc. are defined here.
- :py:attr:`~ray.rllib.core.rl_module.rl_module.RLModuleConfig.catalog_class`: The :py:class:`~ray.rllib.core.models.catalog.Catalog` object of the algorithm.

When writing RL Modules, you need to use these fields to construct your model.

.. tab-set::

    .. tab-item:: Single Agent (torch)

        .. literalinclude:: doc_code/rlmodule_guide.py
            :language: python
            :start-after: __write-custom-sa-rlmodule-torch-begin__
            :end-before: __write-custom-sa-rlmodule-torch-end__


    .. tab-item:: Single Agent (tensorflow)

        .. literalinclude:: doc_code/rlmodule_guide.py
            :language: python
            :start-after: __write-custom-sa-rlmodule-tf-begin__
            :end-before: __write-custom-sa-rlmodule-tf-end__


In :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` you can enforce the checking for the existence of certain input or output keys in the data that is communicated into and out of RL Modules. This serves multiple purposes:

- For the I/O requirement of each method to be self-documenting.
- For failures to happen quickly. If users extend the modules and implement something that does not match the assumptions of the I/O specs, the check reports missing keys and their expected format. For example, RLModule should always have an ``obs`` key in the input batch and an ``action_dist`` key in the output.

.. tab-set::

    .. tab-item:: Single Level Keys

        .. literalinclude:: doc_code/rlmodule_guide.py
            :language: python
            :start-after: __extend-spec-checking-single-level-begin__
            :end-before: __extend-spec-checking-single-level-end__

    .. tab-item:: Nested Keys

        .. literalinclude:: doc_code/rlmodule_guide.py
            :language: python
            :start-after: __extend-spec-checking-nested-begin__
            :end-before: __extend-spec-checking-nested-end__


    .. tab-item:: TensorShape Spec

        .. literalinclude:: doc_code/rlmodule_guide.py
            :language: python
            :start-after: __extend-spec-checking-torch-specs-begin__
            :end-before: __extend-spec-checking-torch-specs-end__


    .. tab-item:: Type Spec

        .. literalinclude:: doc_code/rlmodule_guide.py
            :language: python
            :start-after: __extend-spec-checking-type-specs-begin__
            :end-before: __extend-spec-checking-type-specs-end__

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

For multi-agent modules, RLlib implements :py:class:`~ray.rllib.core.rl_module.marl_module.MultiAgentRLModule`, which is a dictionary of :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` objects, one for each policy, and possibly some shared modules. The base-class implementation works for most of use cases that need to define independent neural networks for sub-groups of agents. For more complex, multi-agent use cases, where the agents share some part of their neural network, you should inherit from this class and override the default implementation.


The :py:class:`~ray.rllib.core.rl_module.marl_module.MultiAgentRLModule` offers an API for constructing custom models tailored to specific needs. The key method for this customization is :py:meth:`~ray.rllib.core.rl_module.marl_module.MultiAgentRLModule`.build.

The following example creates a custom multi-agent RL module with underlying modules. The modules share an encoder, which gets applied to the global part of the observations space. The local part passes through a separate encoder, specific to each policy. 

.. tab-set::

    .. tab-item:: Multi agent with shared encoder (Torch)

        .. literalinclude:: doc_code/rlmodule_guide.py
            :language: python
            :start-after: __write-custom-marlmodule-shared-enc-begin__
            :end-before: __write-custom-marlmodule-shared-enc-end__


To construct this custom multi-agent RL module, pass the class to the :py:class:`~ray.rllib.core.rl_module.marl_module.MultiAgentRLModuleSpec` constructor. Also, pass the :py:class:`~ray.rllib.core.rl_module.rl_module.SingleAgentRLModuleSpec` for each agent because RLlib requires the observation, action spaces, and model hyper-parameters for each agent.

.. literalinclude:: doc_code/rlmodule_guide.py
    :language: python
    :start-after: __pass-custom-marlmodule-shared-enc-begin__
    :end-before: __pass-custom-marlmodule-shared-enc-end__


Extending Existing RLlib RL Modules
-----------------------------------

RLlib provides a number of RL Modules for different frameworks (e.g., PyTorch, TensorFlow, etc.). Extend these modules by inheriting from them and overriding the methods you need to customize. For example, extend :py:class:`~ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module.PPOTorchRLModule` and augment it with your own customization. Then pass the new customized class into the algorithm configuration.

There are two possible ways to extend existing RL Modules:

.. tab-set::

    .. tab-item:: Inheriting existing RL Modules

        One way to extend existing RL Modules is to inherit from them and override the methods you need to customize. For example, extend :py:class:`~ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module.PPOTorchRLModule` and augment it with your own customization. Then pass the new customized class into the algorithm configuration to use the PPO algorithm to optimize your custom RL Module.

        .. code-block:: python

            class MyPPORLModule(PPORLModule):

                def __init__(self, config: RLModuleConfig):
                    super().__init__(config)
                    ...

            # Pass in the custom RL Module class to the spec
            algo_config = algo_config.rl_module(
                rl_module_spec=SingleAgentRLModuleSpec(module_class=MyPPORLModule)
            )


    .. tab-item:: Extending RL Module Catalog

        Another way to customize your module is by extending its :py:class:`~ray.rllib.core.models.catalog.Catalog`. The :py:class:`~ray.rllib.core.models.catalog.Catalog` is a component that defines the default architecture and behavior of a model based on factors such as ``observation_space``, ``action_space``, etc. To modify sub-components of an existing RL Module, extend the corresponding Catalog class.

        For instance, to adapt the existing ``PPORLModule`` for a custom graph observation space not supported by RLlib out-of-the-box, extend the :py:class:`~ray.rllib.core.models.catalog.Catalog` class used to create the ``PPORLModule`` and override the method responsible for returning the encoder component to ensure that your custom encoder replaces the default one initially provided by RLlib. For more information on the :py:class:`~ray.rllib.core.models.catalog.Catalog` class, refer to the `Catalog user guide <rllib-catalogs.html>`__.


        .. code-block:: python

            class MyAwesomeCatalog(PPOCatalog):

                def get_actor_critic_encoder_config():
                    # create your awesome graph encoder here and return it
                    pass


            # Pass in the custom catalog class to the spec
            algo_config = algo_config.rl_module(
                rl_module_spec=SingleAgentRLModuleSpec(catalog_class=MyAwesomeCatalog)
            )


Migrating from Custom Policies and Models to RL Modules
-------------------------------------------------------

This document is for those who have implemented custom policies and models in RLlib and want to migrate to the new `~ray.rllib.core.rl_module.rl_module.RLModule` API. If you have implemented custom policies that extended the `~ray.rllib.policy.eager_tf_policy_v2.EagerTFPolicyV2` or `~ray.rllib.policy.torch_policy_v2.TorchPolicyV2` classes, you likely did so that you could either modify the behavior of constructing models and distributions (via overriding `~ray.rllib.policy.torch_policy_v2.TorchPolicyV2.make_model`, `~ray.rllib.policy.torch_policy_v2.TorchPolicyV2.make_model_and_action_dist`), control the action sampling logic (via overriding `~ray.rllib.policy.eager_tf_policy_v2.EagerTFPolicyV2.action_distribution_fn` or `~ray.rllib.policy.eager_tf_policy_v2.EagerTFPolicyV2.action_sampler_fn`), or control the logic for infernce (via overriding `~ray.rllib.policy.policy.Policy.compute_actions_from_input_dict`, `~ray.rllib.policy.policy.Policy.compute_actions`, or `~ray.rllib.policy.policy.Policy.compute_log_likelihoods`). These APIs were built with `ray.rllib.models.modelv2.ModelV2` models in mind to enable you to customize the behavior of those functions. However `~ray.rllib.core.rl_module.rl_module.RLModule` is a more general abstraction that will reduce the amount of functions that you need to override.

In the new `~ray.rllib.core.rl_module.rl_module.RLModule` API the construction of the models and the action distribution class that should be used are best defined in the constructor. That RL Module is constructed automatically if users follow the instructions outlined in the sections `Enabling RL Modules in the Configuration`_ and `Constructing RL Modules`_. `~ray.rllib.policy.policy.Policy.compute_actions` and `~ray.rllib.policy.policy.Policy.compute_actions_from_input_dict` can still be used for sampling actions for inference or exploration by using the ``explore=True|False`` parameter. If called with ``explore=True`` these functions will invoke `~ray.rllib.core.rl_module.rl_module.RLModule.forward_exploration` and if ``explore=False`` then they will call `~ray.rllib.core.rl_module.rl_module.RLModule.forward_inference`.


What your customization could have looked like before:

.. tab-set::

    .. tab-item:: ModelV2

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


    .. tab-item:: ModelV2 + Distribution


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


    .. tab-item:: Sampler functions

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


All of the ``Policy.compute_***`` functions expect that `~ray.rllib.core.rl_module.rl_module.RLModule.forward_exploration` and `~ray.rllib.core.rl_module.rl_module.RLModule.forward_inference` return a dictionary that contains the key "action_dist" mapping to a ``ray.rllib.models.distributions.Distribution`` instance. Commonly used distribution implementations can be found under ``ray.rllib.models.tf.tf_distributions`` for tensorflow and ``ray.rllib.models.torch.torch_distributions`` for torch. You can choose to return determinstic actions, by creating a determinstic distribution instance. See `Writing Custom Single Agent RL Modules`_ for more details on how to implement your own custom RL Module.

.. tab-set::

    .. tab-item:: The Equivalent RL Module

        .. code-block:: python

            """
            No need to override any policy functions. Simply instead implement any custom logic in your custom RL Module
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
- [] End to end example for custom RL Modules extending PPORLModule (e.g. LLM)